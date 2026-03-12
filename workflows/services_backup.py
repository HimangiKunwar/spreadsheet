from django.utils import timezone
from django.conf import settings
from datasets.models import Dataset
from datasets.services.data_cleaner import DataCleaner
from .models import CleanupWorkflow, WorkflowRun
from .formatting_helpers import FormattingOperations, dataframe_to_excel_with_formatting
from .validation_helpers import DataValidationOperations
import pandas as pd
import numpy as np
import os
import uuid
import logging
import re
from datetime import datetime, timedelta
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

def make_json_serializable(obj):
    """Convert numpy/pandas types to JSON-serializable Python types"""
    if isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif isinstance(obj, (np.integer, int)):
        return int(obj)
    elif isinstance(obj, (np.floating, float)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    else:
        return obj

class WorkflowExecutionService:
    """Execute workflow operations on dataset"""
    
    def __init__(self):
        self._formatting_ops = []  # Store formatting operations
        self._requires_excel_output = False  # Flag for Excel output
    
    @classmethod
    def run_workflow(cls, workflow: CleanupWorkflow, dataset, user) -> WorkflowRun:
        """Execute all operations in workflow sequence"""
        
        # Create service instance
        service = cls()
        
        logger.info(f"Starting workflow execution: {workflow.name} on dataset {dataset.id}")
        
        # Create run record
        run = WorkflowRun.objects.create(
            workflow=workflow,
            dataset=dataset,
            status='running'
        )
        
        results = {
            'operations_completed': 0,
            'operations_failed': 0,
            'details': [],
            'rows_before': 0,
            'rows_after': 0,
            'new_dataset_id': None
        }
        
        try:
            # Get the dataset data directly from the data field
            if not dataset.data:
                raise ValueError("Dataset has no data")
            
            # Convert JSON data to DataFrame
            df = pd.DataFrame(dataset.data)
            
            results['rows_before'] = len(df)
            logger.info(f"Loaded dataset with {len(df)} rows, {len(df.columns)} columns")
            
            # Apply each operation in sequence
            for i, op in enumerate(workflow.operations):
                operation_name = op.get('operation')
                column = op.get('column', '')
                params = op.get('params', {})
                
                logger.info(f"Executing operation {i+1}: {operation_name} on column '{column}'")
                
                try:
                    df, op_result = service._apply_operation(df, operation_name, column, params)
                    
                    results['operations_completed'] += 1
                    results['details'].append({
                        'step': i + 1,
                        'operation': operation_name,
                        'column': column,
                        'status': 'success',
                        'message': op_result
                    })
                    
                    logger.info(f"Operation {operation_name} completed: {op_result}")
                    
                except Exception as e:
                    logger.error(f"Operation {operation_name} failed: {str(e)}")
                    results['operations_failed'] += 1
                    results['details'].append({
                        'step': i + 1,
                        'operation': operation_name,
                        'column': column,
                        'status': 'failed',
                        'error': str(e)
                    })
            
            results['rows_after'] = len(df)
            
            # Save as NEW dataset if any operations completed
            if results['operations_completed'] > 0:
                new_dataset = service._save_cleaned_dataset(df, dataset, workflow, user)
                results['new_dataset_id'] = str(new_dataset.id)
                results['new_dataset_name'] = new_dataset.name
                logger.info(f"Created new dataset: {new_dataset.name}")
            
            # Update run record
            run.status = 'completed' if results['operations_failed'] == 0 else 'completed_with_errors'
            run.completed_at = timezone.now()
            run.results = make_json_serializable(results)
            run.save()
            
            # Update workflow stats
            workflow.last_run = timezone.now()
            workflow.run_count += 1
            workflow.save()
            
            logger.info(f"Workflow completed: {results['operations_completed']} operations, {results['rows_before']} → {results['rows_after']} rows")
            
        except Exception as e:
            logger.error(f"Workflow execution failed: {str(e)}")
            run.status = 'failed'
            run.completed_at = timezone.now()
            run.error_message = str(e)
            run.results = make_json_serializable(results)
            run.save()
        
        return run
    
    def _apply_operation(self, df, operation_name, column, params):
        """Apply a single cleanup operation to the DataFrame"""
        
        rows_before = len(df)
        
        if operation_name == 'remove_duplicates':
            df = df.drop_duplicates()
            removed = rows_before - len(df)
            return df, f"Removed {removed} duplicate rows"
        
        elif operation_name == 'trim_whitespace':
            if column and column in df.columns:
                df[column] = df[column].astype(str).str.strip()
                return df, f"Trimmed whitespace in '{column}'"
            return df, "Column not found for trim operation"
        
        elif operation_name == 'remove_empty_rows':
            df = df.dropna(how='all')
            removed = rows_before - len(df)
            return df, f"Removed {removed} empty rows"
        
        elif operation_name == 'uppercase':
            if column and column in df.columns:
                df[column] = df[column].astype(str).str.upper()
                return df, f"Converted '{column}' to uppercase"
            return df, "Column not found for uppercase operation"
        
        elif operation_name == 'lowercase':
            if column and column in df.columns:
                df[column] = df[column].astype(str).str.lower()
                return df, f"Converted '{column}' to lowercase"
            return df, "Column not found for lowercase operation"
        
        elif operation_name == 'extract_year':
            if column and column in df.columns:
                output_col = params.get('output_column', f'{column}_year')
                
                def extract_year(val):
                    if pd.isna(val) or val == '':
                        return ''
                    try:
                        if isinstance(val, (datetime, pd.Timestamp)):
                            return val.year
                        dt = date_parser.parse(str(val), dayfirst=True)
                        return dt.year
                    except:
                        return ''
                
                df[output_col] = df[column].apply(extract_year)
                return df, f"Extracted year from '{column}' into '{output_col}'"
            return df, "Column not found for extract year operation"
        
        elif operation_name == 'fill_empty_with_mean':
            if column and column in df.columns:
                mean_val = df[column].mean()
                filled = df[column].isna().sum()
                df[column] = df[column].fillna(mean_val)
                return df, f"Filled {filled} empty cells with mean ({mean_val:.2f})"
            return df, "Column not found for fill mean operation"
        
        elif operation_name == 'fill_empty_with_value':
            if column and column in df.columns:
                fill_value = params.get('value', '')
                filled = df[column].isna().sum()
                df[column] = df[column].fillna(fill_value)
                return df, f"Filled {filled} empty cells with '{fill_value}'"
            return df, "Column not found for fill value operation"
        
        # Text Transformation Operations
        elif operation_name == 'convert_uppercase':
            if column and column in df.columns:
                df[column] = df[column].astype(str).str.upper()
                return df, f"Converted '{column}' to uppercase"
            return df, "Column not found for uppercase operation"
        
        elif operation_name == 'convert_lowercase':
            if column and column in df.columns:
                df[column] = df[column].astype(str).str.lower()
                return df, f"Converted '{column}' to lowercase"
            return df, "Column not found for lowercase operation"
        
        elif operation_name == 'convert_titlecase':
            if column and column in df.columns:
                df[column] = df[column].astype(str).str.title()
                return df, f"Converted '{column}' to title case"
            return df, "Column not found for title case operation"
        
        elif operation_name == 'convert_sentencecase':
            if column and column in df.columns:
                df[column] = df[column].astype(str).apply(
                    lambda x: x.capitalize() if isinstance(x, str) else x
                )
                return df, f"Converted '{column}' to sentence case"
            return df, "Column not found for sentence case operation"
        
        elif operation_name == 'find_and_replace':
            if column and column in df.columns:
                find_val = params.get('find_value', '')
                replace_val = params.get('replace_value', '')
                if find_val:
                    df[column] = df[column].astype(str).str.replace(
                        find_val, replace_val, regex=False
                    )
                    return df, f"Replaced '{find_val}' with '{replace_val}' in '{column}'"
                return df, "Find value not specified"
            return df, "Column not found for find and replace operation"
        
        elif operation_name == 'add_prefix':
            if column and column in df.columns:
                prefix = params.get('prefix_value', '')
                df[column] = prefix + df[column].astype(str)
                return df, f"Added prefix '{prefix}' to '{column}'"
            return df, "Column not found for add prefix operation"
        
        elif operation_name == 'add_suffix':
            if column and column in df.columns:
                suffix = params.get('suffix_value', '')
                df[column] = df[column].astype(str) + suffix
                return df, f"Added suffix '{suffix}' to '{column}'"
            return df, "Column not found for add suffix operation"
        
        elif operation_name == 'extract_numbers':
            if column and column in df.columns:
                df[column] = df[column].astype(str).str.extract(r'(\d+)', expand=False).fillna('')
                return df, f"Extracted numbers from '{column}'"
            return df, "Column not found for extract numbers operation"
        
        elif operation_name == 'extract_text':
            if column and column in df.columns:
                df[column] = df[column].astype(str).str.extract(r'([a-zA-Z]+)', expand=False).fillna('')
                return df, f"Extracted text from '{column}'"
            return df, "Column not found for extract text operation"
        
        elif operation_name == 'split_text':
            if column and column in df.columns:
                delimiter = params.get('delimiter', ',')
                output_col = params.get('output_column', f'{column}_split')
                split_df = df[column].astype(str).str.split(delimiter, n=1, expand=True)
                df[column] = split_df[0]
                if split_df.shape[1] > 1:
                    df[output_col] = split_df[1]
                return df, f"Split '{column}' by '{delimiter}' into '{output_col}'"
            return df, "Column not found for split text operation"
        
        elif operation_name == 'merge_columns':
            columns_to_merge = params.get('columns_to_merge', [])
            separator = params.get('separator', ' ')
            output_col = params.get('output_column', 'merged')
            if columns_to_merge:
                valid_cols = [c for c in columns_to_merge if c in df.columns]
                if valid_cols:
                    df[output_col] = df[valid_cols].astype(str).agg(separator.join, axis=1)
                    return df, f"Merged {len(valid_cols)} columns into '{output_col}'"
                return df, "No valid columns found for merge operation"
            return df, "No columns specified for merge operation"
        
        elif operation_name == 'reverse_text':
            if column and column in df.columns:
                df[column] = df[column].astype(str).apply(lambda x: x[::-1])
                return df, f"Reversed text in '{column}'"
            return df, "Column not found for reverse text operation"
        
        # ============================================
        # NUMBER OPERATIONS MACROS (12 operations)
        # ============================================
        
        # 1. Round Numbers
        elif operation_name == 'round_numbers':
            if column and column in df.columns:
                decimal_places = int(params.get('decimal_places', 2))
                df[column] = pd.to_numeric(df[column], errors='coerce').round(decimal_places)
                return df, f"Rounded '{column}' to {decimal_places} decimal places"
            return df, "Column not found for round numbers operation"
        
        # 2. Format as Currency
        elif operation_name == 'format_currency':
            if column and column in df.columns:
                symbol = params.get('currency_symbol', '₹')
                decimals = int(params.get('decimal_places', 2))
                numeric_col = pd.to_numeric(df[column], errors='coerce')
                df[column] = numeric_col.apply(
                    lambda x: f"{symbol}{x:,.{decimals}f}" if pd.notna(x) else ''
                )
                return df, f"Formatted '{column}' as currency with {symbol}"
            return df, "Column not found for format currency operation"
        
        # 3. Format as Percentage
        elif operation_name == 'format_percentage':
            if column and column in df.columns:
                decimals = int(params.get('decimal_places', 2))
                numeric_col = pd.to_numeric(df[column], errors='coerce')
                df[column] = numeric_col.apply(
                    lambda x: f"{(x * 100):.{decimals}f}%" if pd.notna(x) and abs(x) <= 1 
                    else f"{x:.{decimals}f}%" if pd.notna(x) else ''
                )
                return df, f"Formatted '{column}' as percentage"
            return df, "Column not found for format percentage operation"
        
        # 4. Add/Subtract Value
        elif operation_name == 'add_subtract_value':
            if column and column in df.columns:
                math_op = params.get('operation', 'add')
                value = float(params.get('value', 0))
                numeric_col = pd.to_numeric(df[column], errors='coerce')
                if math_op == 'add':
                    df[column] = numeric_col + value
                    return df, f"Added {value} to '{column}'"
                else:  # subtract
                    df[column] = numeric_col - value
                    return df, f"Subtracted {value} from '{column}'"
            return df, "Column not found for add/subtract operation"
        
        # 5. Multiply/Divide
        elif operation_name == 'multiply_divide':
            if column and column in df.columns:
                math_op = params.get('operation', 'multiply')
                value = float(params.get('value', 1))
                numeric_col = pd.to_numeric(df[column], errors='coerce')
                if math_op == 'multiply':
                    df[column] = numeric_col * value
                    return df, f"Multiplied '{column}' by {value}"
                elif math_op == 'divide' and value != 0:
                    df[column] = numeric_col / value
                    return df, f"Divided '{column}' by {value}"
                else:
                    return df, "Cannot divide by zero"
            return df, "Column not found for multiply/divide operation"
        
        # 6. Calculate Sum
        elif operation_name == 'calculate_sum':
            if column and column in df.columns:
                output_type = params.get('output_type', 'new_row')
                total = pd.to_numeric(df[column], errors='coerce').sum()
                if output_type == 'new_row':
                    new_row = {col: '' for col in df.columns}
                    new_row[column] = total
                    if len(df.columns) > 0:
                        new_row[df.columns[0]] = 'TOTAL'
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    return df, f"Added sum ({total}) as new row"
                else:  # new_column
                    df[f'{column}_sum'] = total
                    return df, f"Added sum ({total}) as new column"
            return df, "Column not found for calculate sum operation"
        
        # 7. Calculate Average
        elif operation_name == 'calculate_average':
            if column and column in df.columns:
                output_type = params.get('output_type', 'new_row')
                avg = pd.to_numeric(df[column], errors='coerce').mean()
                avg = round(avg, 2) if pd.notna(avg) else 0
                if output_type == 'new_row':
                    new_row = {col: '' for col in df.columns}
                    new_row[column] = avg
                    if len(df.columns) > 0:
                        new_row[df.columns[0]] = 'AVERAGE'
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    return df, f"Added average ({avg}) as new row"
                else:  # new_column
                    df[f'{column}_avg'] = avg
                    return df, f"Added average ({avg}) as new column"
            return df, "Column not found for calculate average operation"
        
        # 8. Find Min/Max
        elif operation_name == 'find_min_max':
            if column and column in df.columns:
                math_op = params.get('operation', 'max')
                output_type = params.get('output_type', 'new_row')
                numeric_col = pd.to_numeric(df[column], errors='coerce')
                result = numeric_col.max() if math_op == 'max' else numeric_col.min()
                label = 'MAX' if math_op == 'max' else 'MIN'
                if output_type == 'new_row':
                    new_row = {col: '' for col in df.columns}
                    new_row[column] = result
                    if len(df.columns) > 0:
                        new_row[df.columns[0]] = label
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    return df, f"Added {label.lower()} ({result}) as new row"
                else:  # new_column
                    df[f'{column}_{math_op}'] = result
                    return df, f"Added {label.lower()} ({result}) as new column"
            return df, "Column not found for find min/max operation"
        
        # 9. Remove Decimals
        elif operation_name == 'remove_decimals':
            if column and column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').apply(
                    lambda x: int(x) if pd.notna(x) else x
                )
                return df, f"Removed decimals from '{column}'"
            return df, "Column not found for remove decimals operation"
        
        # 10. Negative to Positive
        elif operation_name == 'negative_to_positive':
            if column and column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').abs()
                return df, f"Converted negative values to positive in '{column}'"
            return df, "Column not found for negative to positive operation"
        
        # 11. Number to Words
        elif operation_name == 'number_to_words':
            if column and column in df.columns:
                try:
                    from num2words import num2words
                    lang = params.get('language', 'en')
                    def convert_to_words(x):
                        try:
                            if pd.isna(x):
                                return ''
                            n = float(x)
                            if n == int(n):
                                return num2words(int(n), lang=lang).title()
                            else:
                                return num2words(n, lang=lang).title()
                        except:
                            return str(x)
                    df[column] = df[column].apply(convert_to_words)
                    return df, f"Converted numbers to words in '{column}'"
                except ImportError:
                    return df, "num2words package not installed"
            return df, "Column not found for number to words operation"
        
        # 12. Generate Sequence
        elif operation_name == 'generate_sequence':
            start = int(params.get('start_value', 1))
            step = int(params.get('step', 1))
            col_name = params.get('column_name', 'sequence')
            df[col_name] = range(start, start + len(df) * step, step)
            return df, f"Generated sequence in '{col_name}' starting from {start} with step {step}"
        
        # ============================================
        # DATE/TIME OPERATIONS MACROS (12 operations)
        # ============================================
        
        # 1. Standardize Date Format
        elif operation_name == 'standardize_date_format':
            if column and column in df.columns:
                output_format = params.get('output_format', 'DD/MM/YYYY')
                format_map = {
                    'DD/MM/YYYY': '%d/%m/%Y',
                    'MM/DD/YYYY': '%m/%d/%Y',
                    'YYYY-MM-DD': '%Y-%m-%d',
                    'DD-MM-YYYY': '%d-%m-%Y',
                    'MM-DD-YYYY': '%m-%d-%Y',
                    'YYYY/MM/DD': '%Y/%m/%d',
                    'DD MMM YYYY': '%d %b %Y',
                    'MMM DD, YYYY': '%b %d, %Y',
                }
                py_format = format_map.get(output_format, '%d/%m/%Y')
                
                def standardize_date(val):
                    if pd.isna(val) or val == '':
                        return ''
                    try:
                        if isinstance(val, (datetime, pd.Timestamp)):
                            dt = val
                        else:
                            dt = date_parser.parse(str(val), dayfirst=True)
                        return dt.strftime(py_format)
                    except:
                        return str(val)
                
                df[column] = df[column].apply(standardize_date)
                return df, f"Standardized date format in '{column}' to {output_format}"
            return df, "Column not found for standardize date format operation"
        
        # 2. Extract Year
        elif operation_name == 'extract_year':
            if column and column in df.columns:
                output_col = params.get('output_column', f'{column}_year')
                
                def extract_year(val):
                    if pd.isna(val) or val == '':
                        return ''
                    try:
                        if isinstance(val, (datetime, pd.Timestamp)):
                            return val.year
                        dt = date_parser.parse(str(val), dayfirst=True)
                        return dt.year
                    except:
                        return ''
                
                df[output_col] = df[column].apply(extract_year)
                return df, f"Extracted year from '{column}' into '{output_col}'"
            return df, "Column not found for extract year operation"
        
        # 3. Extract Month
        elif operation_name == 'extract_month':
            if column and column in df.columns:
                output_type = params.get('output_type', 'name')
                output_col = params.get('output_column', f'{column}_month')
                
                def extract_month(val):
                    if pd.isna(val) or val == '':
                        return ''
                    try:
                        if isinstance(val, (datetime, pd.Timestamp)):
                            dt = val
                        else:
                            dt = date_parser.parse(str(val), dayfirst=True)
                        
                        if output_type == 'name':
                            return dt.strftime('%B')
                        elif output_type == 'short_name':
                            return dt.strftime('%b')
                        else:
                            return dt.month
                    except:
                        return ''
                
                df[output_col] = df[column].apply(extract_month)
                return df, f"Extracted month from '{column}' into '{output_col}'"
            return df, "Column not found for extract month operation"
        
        # 4. Extract Day
        elif operation_name == 'extract_day':
            if column and column in df.columns:
                output_col = params.get('output_column', f'{column}_day')
                
                def extract_day(val):
                    if pd.isna(val) or val == '':
                        return ''
                    try:
                        if isinstance(val, (datetime, pd.Timestamp)):
                            return val.day
                        dt = date_parser.parse(str(val), dayfirst=True)
                        return dt.day
                    except:
                        return ''
                
                df[output_col] = df[column].apply(extract_day)
                return df, f"Extracted day from '{column}' into '{output_col}'"
            return df, "Column not found for extract day operation"
        
        # 5. Calculate Age
        elif operation_name == 'calculate_age':
            if column and column in df.columns:
                output_col = params.get('output_column', 'age')
                today = datetime.now()
                
                def calculate_age(val):
                    if pd.isna(val) or val == '':
                        return ''
                    try:
                        if isinstance(val, (datetime, pd.Timestamp)):
                            dob = val
                        else:
                            dob = date_parser.parse(str(val), dayfirst=True)
                        
                        age = relativedelta(today, dob)
                        return age.years
                    except:
                        return ''
                
                df[output_col] = df[column].apply(calculate_age)
                return df, f"Calculated age from '{column}' into '{output_col}'"
            return df, "Column not found for calculate age operation"
        
        # 6. Add/Subtract Days
        elif operation_name == 'add_subtract_days':
            if column and column in df.columns:
                operation = params.get('operation', 'add')
                days = int(params.get('days', 0))
                output_format = params.get('output_format', 'DD/MM/YYYY')
                
                format_map = {
                    'DD/MM/YYYY': '%d/%m/%Y',
                    'MM/DD/YYYY': '%m/%d/%Y',
                    'YYYY-MM-DD': '%Y-%m-%d',
                }
                py_format = format_map.get(output_format, '%d/%m/%Y')
                
                def add_days(val):
                    if pd.isna(val) or val == '':
                        return ''
                    try:
                        if isinstance(val, (datetime, pd.Timestamp)):
                            dt = val
                        else:
                            dt = date_parser.parse(str(val), dayfirst=True)
                        
                        if operation == 'add':
                            new_dt = dt + timedelta(days=days)
                        else:
                            new_dt = dt - timedelta(days=days)
                        
                        return new_dt.strftime(py_format)
                    except:
                        return str(val)
                
                df[column] = df[column].apply(add_days)
                return df, f"{'Added' if operation == 'add' else 'Subtracted'} {days} days to '{column}'"
            return df, "Column not found for add/subtract days operation"
        
        # 7. Find Day of Week
        elif operation_name == 'find_day_of_week':
            if column and column in df.columns:
                output_col = params.get('output_column', f'{column}_weekday')
                output_type = params.get('output_type', 'name')
                
                def get_weekday(val):
                    if pd.isna(val) or val == '':
                        return ''
                    try:
                        if isinstance(val, (datetime, pd.Timestamp)):
                            dt = val
                        else:
                            dt = date_parser.parse(str(val), dayfirst=True)
                        
                        if output_type == 'name':
                            return dt.strftime('%A')
                        elif output_type == 'short_name':
                            return dt.strftime('%a')
                        else:
                            return dt.weekday() + 1
                    except:
                        return ''
                
                df[output_col] = df[column].apply(get_weekday)
                return df, f"Found day of week from '{column}' into '{output_col}'"
            return df, "Column not found for find day of week operation"
        
        # 8. Calculate Duration
        elif operation_name == 'calculate_duration':
            start_col = params.get('start_column') or column
            end_col = params.get('end_column', '')
            output_col = params.get('output_column', 'duration_days')
            unit = params.get('unit', 'days')
            
            if start_col in df.columns and end_col and end_col in df.columns:
                def calc_duration(row):
                    try:
                        start_val = row[start_col]
                        end_val = row[end_col]
                        
                        if pd.isna(start_val) or pd.isna(end_val) or start_val == '' or end_val == '':
                            return ''
                        
                        if isinstance(start_val, (datetime, pd.Timestamp)):
                            start_dt = start_val
                        else:
                            start_dt = date_parser.parse(str(start_val), dayfirst=True)
                        
                        if isinstance(end_val, (datetime, pd.Timestamp)):
                            end_dt = end_val
                        else:
                            end_dt = date_parser.parse(str(end_val), dayfirst=True)
                        
                        delta = end_dt - start_dt
                        
                        if unit == 'days':
                            return delta.days
                        elif unit == 'weeks':
                            return round(delta.days / 7, 1)
                        elif unit == 'months':
                            return round(delta.days / 30, 1)
                        elif unit == 'years':
                            return round(delta.days / 365, 2)
                        else:
                            return delta.days
                    except:
                        return ''
                
                df[output_col] = df.apply(calc_duration, axis=1)
                return df, f"Calculated duration between '{start_col}' and '{end_col}' in {unit}"
            return df, "Required columns not found for calculate duration operation"
        
        # 9. Insert Current Date
        elif operation_name == 'insert_current_date':
            col_name = params.get('column_name', 'current_date')
            date_format = params.get('date_format', 'DD/MM/YYYY')
            
            format_map = {
                'DD/MM/YYYY': '%d/%m/%Y',
                'MM/DD/YYYY': '%m/%d/%Y',
                'YYYY-MM-DD': '%Y-%m-%d',
                'DD MMM YYYY': '%d %b %Y',
            }
            py_format = format_map.get(date_format, '%d/%m/%Y')
            
            df[col_name] = datetime.now().strftime(py_format)
            return df, f"Inserted current date in '{col_name}' column"
        
        # 10. Insert Current Time
        elif operation_name == 'insert_current_time':
            col_name = params.get('column_name', 'current_time')
            time_format = params.get('time_format', 'HH:MM:SS')
            
            format_map = {
                'HH:MM:SS': '%H:%M:%S',
                'HH:MM': '%H:%M',
                'hh:mm:ss AM/PM': '%I:%M:%S %p',
                'hh:mm AM/PM': '%I:%M %p',
            }
            py_format = format_map.get(time_format, '%H:%M:%S')
            
            df[col_name] = datetime.now().strftime(py_format)
            return df, f"Inserted current time in '{col_name}' column"
        
        # 11. Convert Text to Date
        elif operation_name == 'convert_text_to_date':
            if column and column in df.columns:
                output_format = params.get('output_format', 'DD/MM/YYYY')
                
                format_map = {
                    'DD/MM/YYYY': '%d/%m/%Y',
                    'MM/DD/YYYY': '%m/%d/%Y',
                    'YYYY-MM-DD': '%Y-%m-%d',
                }
                py_format = format_map.get(output_format, '%d/%m/%Y')
                
                def text_to_date(val):
                    if pd.isna(val) or val == '':
                        return ''
                    try:
                        dt = date_parser.parse(str(val), dayfirst=True)
                        return dt.strftime(py_format)
                    except:
                        return str(val)
                
                df[column] = df[column].apply(text_to_date)
                return df, f"Converted text to date format in '{column}'"
            return df, "Column not found for convert text to date operation"
        
        # 12. Quarter Calculation
        elif operation_name == 'quarter_calculation':
            if column and column in df.columns:
                output_col = params.get('output_column', f'{column}_quarter')
                output_format = params.get('output_format', 'Q1')
                
                def get_quarter(val):
                    if pd.isna(val) or val == '':
                        return ''
                    try:
                        if isinstance(val, (datetime, pd.Timestamp)):
                            dt = val
                        else:
                            dt = date_parser.parse(str(val), dayfirst=True)
                        
                        quarter = (dt.month - 1) // 3 + 1
                        
                        if output_format == 'Q1':
                            return f'Q{quarter}'
                        elif output_format == 'number':
                            return quarter
                        elif output_format == 'Q1 YYYY':
                            return f'Q{quarter} {dt.year}'
                        else:
                            return f'Q{quarter}'
                    except:
                        return ''
                
                df[output_col] = df[column].apply(get_quarter)
                return df, f"Calculated quarter from '{column}' into '{output_col}'"
            return df, "Column not found for quarter calculation operation"
        
        # ============ ROW/COLUMN MACROS ============

        elif operation_name == 'insert_rows':
            position = params.get('position', 'at_end')  # after_every_n, at_index, at_end
            interval = int(params.get('interval', 5))
            count = int(params.get('count', 1))
            
            if position == 'after_every_n':
                # Insert blank row after every N rows
                new_rows = []
                for i, row in df.iterrows():
                    new_rows.append(row.to_dict())
                    if (i + 1) % interval == 0:
                        for _ in range(count):
                            new_rows.append({col: '' for col in df.columns})
                df = pd.DataFrame(new_rows)
                return df, f"Inserted {count} row(s) after every {interval} rows"
            
            elif position == 'at_index':
                index = int(params.get('index', 0))
                blank_rows = pd.DataFrame([{col: '' for col in df.columns} for _ in range(count)])
                df = pd.concat([df.iloc[:index], blank_rows, df.iloc[index:]]).reset_index(drop=True)
                return df, f"Inserted {count} row(s) at index {index}"
            
            else:  # at_end
                blank_rows = pd.DataFrame([{col: '' for col in df.columns} for _ in range(count)])
                df = pd.concat([df, blank_rows]).reset_index(drop=True)
                return df, f"Inserted {count} row(s) at end"

        elif operation_name == 'insert_columns':
            column_name = params.get('column_name', 'New_Column')
            position = params.get('position', 'at_end')  # before, after, at_index, at_end
            reference_column = params.get('reference_column', '')
            default_value = params.get('default_value', '')
            
            if column_name in df.columns:
                return df, f"Column '{column_name}' already exists"
            
            if position == 'at_end':
                df[column_name] = default_value
            elif position == 'before' and reference_column in df.columns:
                idx = df.columns.get_loc(reference_column)
                df.insert(idx, column_name, default_value)
            elif position == 'after' and reference_column in df.columns:
                idx = df.columns.get_loc(reference_column) + 1
                df.insert(idx, column_name, default_value)
            elif position == 'at_index':
                idx = int(params.get('index', 0))
                df.insert(min(idx, len(df.columns)), column_name, default_value)
            else:
                df[column_name] = default_value
            
            return df, f"Inserted column '{column_name}'"

        elif operation_name == 'delete_rows':
            condition = params.get('condition', 'blank_rows')  # blank_rows, duplicate_rows, by_value, by_index
            original_count = len(df)
            
            if condition == 'blank_rows':
                df = df.dropna(how='all').reset_index(drop=True)
                # Also remove rows where all values are empty strings
                df = df[~(df.astype(str).apply(lambda x: x.str.strip() == '').all(axis=1))].reset_index(drop=True)
            
            elif condition == 'duplicate_rows':
                subset_cols = params.get('columns', None)  # None means all columns
                keep = params.get('keep', 'first')  # first, last, False (remove all)
                df = df.drop_duplicates(subset=subset_cols, keep=keep).reset_index(drop=True)
            
            elif condition == 'by_value':
                col = params.get('column') or column
                value = params.get('value', '')
                operator = params.get('operator', 'equals')
                
                if col in df.columns:
                    if operator == 'equals':
                        df = df[df[col].astype(str) != str(value)].reset_index(drop=True)
                    elif operator == 'contains':
                        df = df[~df[col].astype(str).str.contains(str(value), case=False, na=False)].reset_index(drop=True)
                    elif operator == 'empty':
                        df = df[df[col].notna() & (df[col].astype(str).str.strip() != '')].reset_index(drop=True)
            
            elif condition == 'by_index':
                indices = params.get('indices', [])
                if isinstance(indices, str):
                    indices = [int(i.strip()) for i in indices.split(',') if i.strip().isdigit()]
                df = df.drop(index=[i for i in indices if i in df.index]).reset_index(drop=True)
            
            deleted_count = original_count - len(df)
            return df, f"Deleted {deleted_count} rows ({condition})"

        elif operation_name == 'delete_columns':
            condition = params.get('condition', 'by_name')  # empty_columns, by_name
            
            if condition == 'empty_columns':
                # Delete columns that are entirely empty/null
                empty_cols = [col for col in df.columns if df[col].isna().all() or (df[col].astype(str).str.strip() == '').all()]
                df = df.drop(columns=empty_cols)
                return df, f"Deleted {len(empty_cols)} empty columns: {empty_cols}"
            
            elif condition == 'by_name':
                columns_to_delete = params.get('columns', [])
                if isinstance(columns_to_delete, str):
                    columns_to_delete = [c.strip() for c in columns_to_delete.split(',')]
                
                # Also check column argument
                if column and column not in columns_to_delete:
                    columns_to_delete.append(column)
                
                existing = [c for c in columns_to_delete if c in df.columns]
                df = df.drop(columns=existing)
                return df, f"Deleted columns: {existing}"
            
            return df, "No columns deleted"

        elif operation_name == 'hide_rows':
            condition = params.get('condition', 'by_value')
            hidden_col = '_row_hidden'
            
            if hidden_col not in df.columns:
                df[hidden_col] = False
            
            if condition == 'by_value':
                col = params.get('column') or column
                value = params.get('value', '')
                if col in df.columns:
                    mask = df[col].astype(str).str.lower() == str(value).lower()
                    df.loc[mask, hidden_col] = True
                    hidden_count = mask.sum()
                    return df, f"Hidden {hidden_count} rows where {col}='{value}'"
            
            elif condition == 'by_index':
                indices = params.get('indices', [])
                if isinstance(indices, str):
                    indices = [int(i.strip()) for i in indices.split(',') if i.strip().isdigit()]
                df.loc[df.index.isin(indices), hidden_col] = True
                return df, f"Hidden {len(indices)} rows by index"
            
            return df, "No rows hidden"

        elif operation_name == 'hide_columns':
            columns_to_hide = params.get('columns', [])
            if isinstance(columns_to_hide, str):
                columns_to_hide = [c.strip() for c in columns_to_hide.split(',')]
            if column and column not in columns_to_hide:
                columns_to_hide.append(column)
            
            # Store hidden columns in a metadata column
            meta_col = '_hidden_columns'
            existing_hidden = []
            if meta_col in df.columns and len(df) > 0:
                try:
                    existing_hidden = eval(df[meta_col].iloc[0]) if pd.notna(df[meta_col].iloc[0]) else []
                except:
                    existing_hidden = []
            
            all_hidden = list(set(existing_hidden + columns_to_hide))
            df[meta_col] = str(all_hidden)
            
            return df, f"Marked columns as hidden: {columns_to_hide}"

        elif operation_name == 'unhide_all':
            target = params.get('target', 'all')  # rows, columns, all
            messages = []
            
            if target in ['rows', 'all'] and '_row_hidden' in df.columns:
                hidden_count = df['_row_hidden'].sum() if df['_row_hidden'].dtype == bool else 0
                df['_row_hidden'] = False
                messages.append(f"Unhidden {hidden_count} rows")
            
            if target in ['columns', 'all'] and '_hidden_columns' in df.columns:
                df['_hidden_columns'] = '[]'
                messages.append("Unhidden all columns")
            
            return df, "; ".join(messages) if messages else "Nothing to unhide"

        elif operation_name == 'sort_rows':
            sort_by = params.get('sort_by') or params.get('column') or column
            order = params.get('order', 'asc').lower()
            ascending = order != 'desc'
            
            if isinstance(sort_by, str):
                sort_by = [s.strip() for s in sort_by.split(',')]
            
            valid_cols = [c for c in sort_by if c in df.columns]
            if valid_cols:
                df = df.sort_values(by=valid_cols, ascending=ascending).reset_index(drop=True)
                return df, f"Sorted by {valid_cols} ({'ascending' if ascending else 'descending'})"
            
            return df, f"Column(s) not found: {sort_by}"

        elif operation_name == 'filter_data':
            col = params.get('column') or column
            operator = params.get('operator', 'equals')
            value = params.get('value', '')
            original_count = len(df)
            
            if col not in df.columns:
                return df, f"Column '{col}' not found"
            
            if operator == 'equals':
                df = df[df[col].astype(str).str.lower() == str(value).lower()].reset_index(drop=True)
            elif operator == 'not_equals':
                df = df[df[col].astype(str).str.lower() != str(value).lower()].reset_index(drop=True)
            elif operator == 'contains':
                df = df[df[col].astype(str).str.contains(str(value), case=False, na=False)].reset_index(drop=True)
            elif operator == 'starts_with':
                df = df[df[col].astype(str).str.lower().str.startswith(str(value).lower())].reset_index(drop=True)
            elif operator == 'ends_with':
                df = df[df[col].astype(str).str.lower().str.endswith(str(value).lower())].reset_index(drop=True)
            elif operator == 'greater_than':
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df[df[col] > float(value)].reset_index(drop=True)
            elif operator == 'less_than':
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df = df[df[col] < float(value)].reset_index(drop=True)
            elif operator == 'not_empty':
                df = df[df[col].notna() & (df[col].astype(str).str.strip() != '')].reset_index(drop=True)
            elif operator == 'is_empty':
                df = df[df[col].isna() | (df[col].astype(str).str.strip() == '')].reset_index(drop=True)
            elif operator == 'in_list':
                values = [v.strip().lower() for v in str(value).split(',')]
                df = df[df[col].astype(str).str.lower().isin(values)].reset_index(drop=True)
            
            kept_count = len(df)
            return df, f"Filter by {col} {operator} '{value}': kept {kept_count}/{original_count} rows"

        elif operation_name == 'transpose':
            use_first_row_as_headers = params.get('use_first_row_as_headers', False)
            use_first_col_as_headers = params.get('use_first_column_as_headers', True)
            
            if use_first_col_as_headers and len(df.columns) > 0:
                first_col = df.columns[0]
                df_transposed = df.set_index(first_col).T
                df_transposed = df_transposed.reset_index()
                df_transposed = df_transposed.rename(columns={'index': first_col})
            else:
                df_transposed = df.T.reset_index()
                df_transposed.columns = ['Original_Column'] + [f'Row_{i}' for i in range(len(df))]
            
            return df_transposed, f"Transposed data: {len(df)} rows × {len(df.columns)} cols → {len(df_transposed)} rows × {len(df_transposed.columns)} cols"

        elif operation_name == 'group_rows':
            group_by = params.get('group_by') or params.get('column') or column
            add_subtotals = params.get('add_subtotals', False)
            subtotal_columns = params.get('subtotal_columns', [])
            
            if group_by not in df.columns:
                return df, f"Column '{group_by}' not found"
            
            # Sort by group column first
            df = df.sort_values(by=group_by).reset_index(drop=True)
            
            # Add group level indicator
            df['_group'] = df[group_by]
            df['_group_level'] = 1
            
            if add_subtotals and subtotal_columns:
                if isinstance(subtotal_columns, str):
                    subtotal_columns = [c.strip() for c in subtotal_columns.split(',')]
                
                # Calculate subtotals for each group
                subtotals = []
                for group_name, group_df in df.groupby(group_by):
                    subtotal_row = {col: '' for col in df.columns}
                    subtotal_row[group_by] = f"Subtotal: {group_name}"
                    subtotal_row['_group'] = group_name
                    subtotal_row['_group_level'] = 0
                    
                    for col in subtotal_columns:
                        if col in df.columns:
                            try:
                                subtotal_row[col] = group_df[col].sum()
                            except:
                                subtotal_row[col] = 'N/A'
                    
                    subtotals.append((group_df.index[-1] + 0.5, subtotal_row))
                
                # Insert subtotal rows
                for idx, row in sorted(subtotals, key=lambda x: x[0], reverse=True):
                    df = pd.concat([df.iloc[:int(idx)+1], pd.DataFrame([row]), df.iloc[int(idx)+1:]]).reset_index(drop=True)
            
            return df, f"Grouped by '{group_by}'" + (f" with subtotals on {subtotal_columns}" if add_subtotals else "")

        elif operation_name == 'freeze_panes':
            freeze_rows = int(params.get('freeze_rows', 1))
            freeze_columns = int(params.get('freeze_columns', 0))
            
            # Store freeze info in metadata column
            df['_freeze_panes'] = f"rows:{freeze_rows},cols:{freeze_columns}"
            
            return df, f"Set freeze panes: {freeze_rows} row(s), {freeze_columns} column(s) (applied on Excel export)"
        
        elif operation_name == 'validate_email':
            # Get column from multiple possible sources
            col = params.get('column') or params.get('emailColumn') or params.get('email_column')
            if not col:
                col = column  # Use the column passed from operation level
            
            if not col:
                raise ValueError(f"No column specified for validate_email. Params: {params}, Column arg: {column}")
            
            # Use col instead of column for the rest of the operation
            if col not in df.columns:
                cols_lower = {c.lower(): c for c in df.columns}
                if col.lower() in cols_lower:
                    col = cols_lower[col.lower()]
                else:
                    raise ValueError(f"Column '{col}' not found. Available: {list(df.columns)}")
            
            import re
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            
            status_column = f"{col}_validation"
            df[status_column] = df[col].apply(
                lambda x: 'VALID' if pd.notna(x) and re.match(email_pattern, str(x).strip()) 
                else ('BLANK' if pd.isna(x) or str(x).strip() == '' else 'INVALID')
            )
            
            valid_count = (df[status_column] == 'VALID').sum()
            invalid_count = (df[status_column] == 'INVALID').sum()
            blank_count = (df[status_column] == 'BLANK').sum()
            
            return df, f"Email validation completed: {valid_count} valid, {invalid_count} invalid, {blank_count} blank"
        
        # ============================================
        # DATA VALIDATION OPERATIONS (12 operations)
        # ============================================
        
        elif operation_name == 'validate_phone':
            column = params.get('column')
            country_code = params.get('country_code', 'IN')
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.validate_phone(df, column, country_code, add_col)
            operation_result = {}
            operation_result['validation_stats'] = validation_results
            return df, f"Phone validation completed: {validation_results['valid_phones']} valid, {validation_results['invalid_phones']} invalid"
            
        elif operation_name == 'validate_date':
            column = params.get('column')
            date_format = params.get('date_format', 'auto')
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.validate_date(df, column, date_format, add_col)
            operation_result = {}
            operation_result['validation_stats'] = validation_results
            return df, f"Date validation completed: {validation_results['valid_dates']} valid, {validation_results['invalid_dates']} invalid"
            
        elif operation_name == 'check_for_blanks':
            column = params.get('column')
            action = params.get('action', 'flag')
            fill_value = params.get('fill_value')
            df, validation_results = DataValidationOperations.check_for_blanks(df, column, action, fill_value)
            operation_result = {}
            operation_result['validation_stats'] = validation_results
            return df, f"Blank check completed: {validation_results['blank_cells']} blank cells found"
            
        elif operation_name == 'check_data_type':
            column = params.get('column')
            expected_type = params.get('expected_type', 'number')
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.check_data_type(df, column, expected_type, add_col)
            operation_result = {}
            operation_result['validation_stats'] = validation_results
            return df, f"Data type check completed: {validation_results['valid_type']} valid, {validation_results['invalid_type']} invalid"
            
        elif operation_name == 'validate_range':
            column = params.get('column')
            min_value = params.get('min_value')
            max_value = params.get('max_value')
            if min_value is not None:
                min_value = float(min_value)
            if max_value is not None:
                max_value = float(max_value)
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.validate_range(df, column, min_value, max_value, add_col)
            operation_result = {}
            operation_result['validation_stats'] = validation_results
            return df, f"Range validation completed: {validation_results['valid_in_range']} in range, {validation_results['below_minimum']} below min, {validation_results['above_maximum']} above max"
            
        elif operation_name == 'check_duplicates':
            column = params.get('column')
            action = params.get('action', 'flag')
            keep = params.get('keep', 'first')
            df, validation_results = DataValidationOperations.check_duplicates(df, column, action, keep)
            operation_result = {}
            operation_result['validation_stats'] = validation_results
            return df, f"Duplicate check completed: {validation_results['duplicate_rows']} duplicates found"
            
        elif operation_name == 'validate_length':
            column = params.get('column')
            min_length = params.get('min_length')
            max_length = params.get('max_length')
            exact_length = params.get('exact_length')
            if min_length is not None:
                min_length = int(min_length)
            if max_length is not None:
                max_length = int(max_length)
            if exact_length is not None:
                exact_length = int(exact_length)
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.validate_length(df, column, min_length, max_length, exact_length, add_col)
            operation_result = {}
            operation_result['validation_stats'] = validation_results
            return df, f"Length validation completed: {validation_results['valid_length']} valid, {validation_results['too_short']} too short, {validation_results['too_long']} too long"
            
        elif operation_name == 'check_required_fields':
            columns = params.get('columns', [])
            if isinstance(columns, str):
                columns = [c.strip() for c in columns.split(',')]
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.check_required_fields(df, columns, add_col)
            operation_result = {}
            operation_result['validation_stats'] = validation_results
            return df, f"Required fields check completed: {validation_results['complete_rows']} complete rows, {validation_results['incomplete_rows']} incomplete"
            
        elif operation_name == 'validate_pan_aadhaar':
            column = params.get('column')
            id_type = params.get('id_type', 'PAN')
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.validate_pan_aadhaar(df, column, id_type, add_col)
            operation_result = {}
            operation_result['validation_stats'] = validation_results
            return df, f"{id_type} validation completed: {validation_results['valid_ids']} valid, {validation_results['invalid_ids']} invalid"
            
        elif operation_name == 'highlight_errors':
            column = params.get('column')
            error_type = params.get('error_type', 'any')
            create_col = params.get('create_error_column', True)
            df, validation_results = DataValidationOperations.highlight_errors(df, column, error_type, create_col)
            operation_result = {}
            operation_result['validation_stats'] = validation_results
            return df, f"Error highlighting completed: {validation_results['cells_with_errors']} errors found"
            
        elif operation_name == 'create_error_report':
            columns = params.get('columns')
            if isinstance(columns, str):
                columns = [c.strip() for c in columns.split(',')]
            output_column = params.get('output_column', 'error_summary')
            df, validation_results = DataValidationOperations.create_error_report(df, columns, output_column)
            operation_result = {}
            operation_result['validation_stats'] = validation_results
            return df, f"Error report created: {validation_results['rows_with_errors']} rows with errors"
        
        # ============================================
        # FORMATTING MACROS (15 operations)
        # ============================================
        # Note: Formatting operations require Excel output
        # They set a flag to indicate the result should be saved as .xlsx

        # 1. Auto-fit Column Width
        elif operation_name == 'autofit_column_width':
            self._formatting_ops.append({
                'type': 'autofit_column_width',
                'column': column or 'all'
            })
            self._requires_excel_output = True
            return df, f"Auto-fit column width scheduled for '{column or 'all columns'}'"

        # 2. Auto-fit Row Height
        elif operation_name == 'autofit_row_height':
            row_range = params.get('row_range', 'all')
            self._formatting_ops.append({
                'type': 'autofit_row_height',
                'row_range': row_range
            })
            self._requires_excel_output = True
            return df, f"Auto-fit row height scheduled for '{row_range}'"

        # 3. Apply Bold/Italic
        elif operation_name == 'apply_bold_italic':
            if column and column in df.columns:
                style = params.get('style', 'bold')
                row_range = params.get('row_range', 'all')
                self._formatting_ops.append({
                    'type': 'apply_bold_italic',
                    'column': column,
                    'style': style,
                    'row_range': row_range
                })
                self._requires_excel_output = True
                return df, f"Bold/italic formatting scheduled for '{column}'"
            return df, "Column not found for bold/italic operation"

        # 4. Change Font
        elif operation_name == 'change_font':
            if column and column in df.columns:
                font_name = params.get('font_name', 'Calibri')
                self._formatting_ops.append({
                    'type': 'change_font',
                    'column': column,
                    'font_name': font_name
                })
                self._requires_excel_output = True
                return df, f"Font change to '{font_name}' scheduled for '{column}'"
            return df, "Column not found for font change operation"

        # 5. Change Font Size
        elif operation_name == 'change_font_size':
            if column and column in df.columns:
                font_size = params.get('font_size', 11)
                self._formatting_ops.append({
                    'type': 'change_font_size',
                    'column': column,
                    'font_size': font_size
                })
                self._requires_excel_output = True
                return df, f"Font size change to {font_size} scheduled for '{column}'"
            return df, "Column not found for font size operation"

        # 6. Apply Cell Color
        elif operation_name == 'apply_cell_color':
            if column and column in df.columns:
                color = params.get('color', 'yellow')
                row_condition = params.get('row_condition', 'all')
                self._formatting_ops.append({
                    'type': 'apply_cell_color',
                    'column': column,
                    'color': color,
                    'row_condition': row_condition
                })
                self._requires_excel_output = True
                return df, f"Cell color '{color}' scheduled for '{column}'"
            return df, "Column not found for cell color operation"

        # 7. Apply Text Color
        elif operation_name == 'apply_text_color':
            if column and column in df.columns:
                color = params.get('color', 'red')
                condition = params.get('condition', None)
                condition_value = params.get('condition_value', None)
                self._formatting_ops.append({
                    'type': 'apply_text_color',
                    'column': column,
                    'color': color,
                    'condition': condition,
                    'condition_value': condition_value
                })
                self._requires_excel_output = True
                return df, f"Text color '{color}' scheduled for '{column}'"
            return df, "Column not found for text color operation"

        # 8. Add Borders
        elif operation_name == 'add_borders':
            border_style = params.get('border_style', 'thin')
            self._formatting_ops.append({
                'type': 'add_borders',
                'column': column or 'all',
                'border_style': border_style
            })
            self._requires_excel_output = True
            return df, f"Borders '{border_style}' scheduled for '{column or 'all columns'}'"

        # 9. Merge Cells
        elif operation_name == 'merge_cells':
            cell_range = params.get('cell_range', '')
            if cell_range:
                self._formatting_ops.append({
                    'type': 'merge_cells',
                    'cell_range': cell_range
                })
                self._requires_excel_output = True
                return df, f"Cell merge scheduled for range '{cell_range}'"
            return df, "Cell range not specified for merge operation"

        # 10. Unmerge Cells
        elif operation_name == 'unmerge_cells':
            cell_range = params.get('cell_range', '')
            if cell_range:
                self._formatting_ops.append({
                    'type': 'unmerge_cells',
                    'cell_range': cell_range
                })
                self._requires_excel_output = True
                return df, f"Cell unmerge scheduled for range '{cell_range}'"
            return df, "Cell range not specified for unmerge operation"

        # 11. Align Text
        elif operation_name == 'align_text':
            if column and column in df.columns:
                horizontal = params.get('horizontal', 'center')
                vertical = params.get('vertical', 'center')
                self._formatting_ops.append({
                    'type': 'align_text',
                    'column': column,
                    'horizontal': horizontal,
                    'vertical': vertical
                })
                self._requires_excel_output = True
                return df, f"Text alignment '{horizontal}/{vertical}' scheduled for '{column}'"
            return df, "Column not found for text alignment operation"

        # 12. Apply Number Format
        elif operation_name == 'apply_number_format':
            if column and column in df.columns:
                format_type = params.get('format_type', 'comma')
                self._formatting_ops.append({
                    'type': 'apply_number_format',
                    'column': column,
                    'format_type': format_type
                })
                self._requires_excel_output = True
                return df, f"Number format '{format_type}' scheduled for '{column}'"
            return df, "Column not found for number format operation"

        # 13. Conditional Formatting
        elif operation_name == 'conditional_formatting':
            if column and column in df.columns:
                condition = params.get('condition', 'less_than')
                value = params.get('value', 0)
                color = params.get('color', 'red')
                self._formatting_ops.append({
                    'type': 'conditional_formatting',
                    'column': column,
                    'condition': condition,
                    'value': value,
                    'color': color
                })
                self._requires_excel_output = True
                return df, f"Conditional formatting scheduled for '{column}'"
            return df, "Column not found for conditional formatting operation"

        # 14. Copy Formatting
        elif operation_name == 'copy_formatting':
            source_column = params.get('source_column') or column
            target_column = params.get('target_column', '')
            if source_column and target_column and source_column in df.columns and target_column in df.columns:
                self._formatting_ops.append({
                    'type': 'copy_formatting',
                    'source_column': source_column,
                    'target_column': target_column
                })
                self._requires_excel_output = True
                return df, f"Copy formatting from '{source_column}' to '{target_column}' scheduled"
            return df, "Source or target column not found for copy formatting operation"

        # 15. Clear Formatting
        elif operation_name == 'clear_formatting':
            self._formatting_ops.append({
                'type': 'clear_formatting',
                'column': column or 'all'
            })
            self._requires_excel_output = True
            return df, f"Clear formatting scheduled for '{column or 'all columns'}'"
        
        
        else:
            return df, f"Operation '{operation_name}' not implemented yet"
    
    def _save_cleaned_dataset(self, df, original_dataset, workflow, user):
        """Save processed DataFrame as new dataset with optional formatting"""
        import uuid
        from datasets.models import Dataset
        
        # Generate unique filename
        safe_name = workflow.name.replace(' ', '_').replace('/', '_')[:50]
        
        # Determine file extension based on formatting
        if self._requires_excel_output:
            new_filename = f"{safe_name}_{uuid.uuid4().hex[:8]}_cleaned.xlsx"
            file_type = 'xlsx'
        else:
            new_filename = f"{safe_name}_{uuid.uuid4().hex[:8]}_cleaned.csv"
            file_type = 'csv'
        
        # Create path
        relative_path = os.path.join('datasets', str(user.id), new_filename)
        full_path = os.path.join(settings.MEDIA_ROOT, relative_path)
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        # Save file with or without formatting
        if self._requires_excel_output and self._formatting_ops:
            # Create Excel workbook with formatting
            wb, ws = dataframe_to_excel_with_formatting(df, full_path)
            
            # Apply all formatting operations
            formatter = FormattingOperations(wb, ws)
            
            for fmt_op in self._formatting_ops:
                fmt_type = fmt_op.get('type')
                
                if fmt_type == 'autofit_column_width':
                    formatter.autofit_column_width(fmt_op.get('column'))
                elif fmt_type == 'autofit_row_height':
                    formatter.autofit_row_height(fmt_op.get('row_range'))
                elif fmt_type == 'apply_bold_italic':
                    formatter.apply_bold_italic(
                        fmt_op.get('column'),
                        fmt_op.get('style', 'bold'),
                        fmt_op.get('row_range')
                    )
                elif fmt_type == 'change_font':
                    formatter.change_font(
                        fmt_op.get('column'),
                        fmt_op.get('font_name', 'Calibri')
                    )
                elif fmt_type == 'change_font_size':
                    formatter.change_font_size(
                        fmt_op.get('column'),
                        fmt_op.get('font_size', 11)
                    )
                elif fmt_type == 'apply_cell_color':
                    formatter.apply_cell_color(
                        fmt_op.get('column'),
                        fmt_op.get('color', 'yellow'),
                        fmt_op.get('row_condition')
                    )
                elif fmt_type == 'apply_text_color':
                    formatter.apply_text_color(
                        fmt_op.get('column'),
                        fmt_op.get('color', 'red'),
                        fmt_op.get('condition'),
                        fmt_op.get('condition_value')
                    )
                elif fmt_type == 'add_borders':
                    formatter.add_borders(
                        fmt_op.get('column'),
                        fmt_op.get('border_style', 'thin')
                    )
                elif fmt_type == 'merge_cells':
                    formatter.merge_cells(fmt_op.get('cell_range'))
                elif fmt_type == 'unmerge_cells':
                    formatter.unmerge_cells(fmt_op.get('cell_range'))
                elif fmt_type == 'align_text':
                    formatter.align_text(
                        fmt_op.get('column'),
                        fmt_op.get('horizontal', 'center'),
                        fmt_op.get('vertical', 'center')
                    )
                elif fmt_type == 'apply_number_format':
                    formatter.apply_number_format(
                        fmt_op.get('column'),
                        fmt_op.get('format_type', 'comma')
                    )
                elif fmt_type == 'conditional_formatting':
                    formatter.conditional_formatting(
                        fmt_op.get('column'),
                        fmt_op.get('condition', 'less_than'),
                        fmt_op.get('value', 0),
                        fmt_op.get('color', 'red')
                    )
                elif fmt_type == 'copy_formatting':
                    formatter.copy_formatting(
                        fmt_op.get('source_column'),
                        fmt_op.get('target_column')
                    )
                elif fmt_type == 'clear_formatting':
                    formatter.clear_formatting(fmt_op.get('column'))
            
            # Save workbook
            wb.save(full_path)
        elif self._requires_excel_output:
            # Excel output without formatting ops
            df.to_excel(full_path, index=False, engine='openpyxl')
        else:
            # CSV output
            df.to_csv(full_path, index=False)
        
        # Create Dataset record
        new_dataset = Dataset.objects.create(
            user=user,
            name=f"{original_dataset.name}_{workflow.name}_cleaned",
            original_filename=new_filename,
            file_path=relative_path,
            file_type=file_type,
            file_size=os.path.getsize(full_path),
            row_count=len(df),
            column_count=len(df.columns),
            schema=make_json_serializable({'columns': list(df.columns)}),
            data=make_json_serializable(df.head(100).to_dict('records'))
        )
        
        return new_dataset

    def get_available_operations(self):
        """Return list of available operations for UI"""
        operations = DataCleaner.get_operations()
        base_operations = [
            {
                'id': op_id,
                'name': op_info['name'],
                'requires_column': op_info.get('column_types') != ['any'] and 'any' not in op_info.get('column_types', []),
                'description': op_info['description'],
                'category': op_info.get('category', 'general')
            }
            for op_id, op_info in operations.items()
        ]
        
        # Add new text transformation operations
        text_transformations = [
            {
                'id': 'convert_uppercase',
                'name': 'Convert to UPPERCASE',
                'description': 'Convert all text to capital letters',
                'category': 'text_transformation',
                'requires_column': True,
                'params': []
            },
            {
                'id': 'convert_lowercase',
                'name': 'Convert to lowercase',
                'description': 'Convert all text to small letters',
                'category': 'text_transformation',
                'requires_column': True,
                'params': []
            },
            {
                'id': 'convert_titlecase',
                'name': 'Convert to Title Case',
                'description': 'Capitalize first letter of each word',
                'category': 'text_transformation',
                'requires_column': True,
                'params': []
            },
            {
                'id': 'convert_sentencecase',
                'name': 'Convert to Sentence case',
                'description': 'Capitalize only first letter',
                'category': 'text_transformation',
                'requires_column': True,
                'params': []
            },
            {
                'id': 'find_and_replace',
                'name': 'Find and Replace',
                'description': 'Replace specific text with another',
                'category': 'text_transformation',
                'requires_column': True,
                'params': [
                    {'name': 'find_value', 'type': 'string', 'label': 'Find', 'required': True},
                    {'name': 'replace_value', 'type': 'string', 'label': 'Replace with', 'required': True}
                ]
            },
            {
                'id': 'add_prefix',
                'name': 'Add Prefix',
                'description': 'Add text before each value',
                'category': 'text_transformation',
                'requires_column': True,
                'params': [
                    {'name': 'prefix_value', 'type': 'string', 'label': 'Prefix', 'required': True}
                ]
            },
            {
                'id': 'add_suffix',
                'name': 'Add Suffix',
                'description': 'Add text after each value',
                'category': 'text_transformation',
                'requires_column': True,
                'params': [
                    {'name': 'suffix_value', 'type': 'string', 'label': 'Suffix', 'required': True}
                ]
            },
            {
                'id': 'extract_numbers',
                'name': 'Extract Numbers',
                'description': 'Keep only numeric characters',
                'category': 'text_transformation',
                'requires_column': True,
                'params': []
            },
            {
                'id': 'extract_text',
                'name': 'Extract Text',
                'description': 'Keep only letter characters',
                'category': 'text_transformation',
                'requires_column': True,
                'params': []
            },
            {
                'id': 'split_text',
                'name': 'Split Text',
                'description': 'Split text into multiple columns',
                'category': 'text_transformation',
                'requires_column': True,
                'params': [
                    {'name': 'delimiter', 'type': 'string', 'label': 'Split by', 'required': True, 'default': ','},
                    {'name': 'output_column', 'type': 'string', 'label': 'New column name', 'required': False}
                ]
            },
            {
                'id': 'reverse_text',
                'name': 'Reverse Text',
                'description': 'Reverse the text in each cell',
                'category': 'text_transformation',
                'requires_column': True,
                'params': []
            }
        ]
        
        # Number Operations (11 operations - excluding round_numbers which is in DataCleaner)
        number_operations = [
            {'id': 'format_currency', 'name': 'Format as Currency', 'category': 'number_operations', 'requires_column': True, 'params': [
                {'name': 'currency_symbol', 'type': 'select', 'label': 'Currency', 'required': True, 'default': '₹', 'options': [
                    {'value': '₹', 'label': '₹ (INR)'},
                    {'value': '$', 'label': '$ (USD)'},
                    {'value': '€', 'label': '€ (EUR)'},
                    {'value': '£', 'label': '£ (GBP)'}
                ]},
                {'name': 'decimal_places', 'type': 'number', 'label': 'Decimal Places', 'required': True, 'default': 2}
            ]},
            {'id': 'format_percentage', 'name': 'Format as Percentage', 'category': 'number_operations', 'requires_column': True, 'params': [
                {'name': 'decimal_places', 'type': 'number', 'label': 'Decimal Places', 'required': True, 'default': 2}
            ]},
            {'id': 'add_subtract_value', 'name': 'Add/Subtract Value', 'category': 'number_operations', 'requires_column': True, 'params': [
                {'name': 'operation', 'type': 'select', 'label': 'Operation', 'required': True, 'default': 'add', 'options': [
                    {'value': 'add', 'label': 'Add (+)'},
                    {'value': 'subtract', 'label': 'Subtract (-)'}
                ]},
                {'name': 'value', 'type': 'number', 'label': 'Value', 'required': True, 'default': 0}
            ]},
            {'id': 'multiply_divide', 'name': 'Multiply/Divide', 'category': 'number_operations', 'requires_column': True, 'params': [
                {'name': 'operation', 'type': 'select', 'label': 'Operation', 'required': True, 'default': 'multiply', 'options': [
                    {'value': 'multiply', 'label': 'Multiply (×)'},
                    {'value': 'divide', 'label': 'Divide (÷)'}
                ]},
                {'name': 'value', 'type': 'number', 'label': 'Value', 'required': True, 'default': 1}
            ]},
            {'id': 'calculate_sum', 'name': 'Calculate Sum', 'category': 'number_operations', 'requires_column': True, 'params': [
                {'name': 'output_type', 'type': 'select', 'label': 'Output As', 'required': True, 'default': 'new_row', 'options': [
                    {'value': 'new_row', 'label': 'Add as new row'},
                    {'value': 'new_column', 'label': 'Add as new column'}
                ]}
            ]},
            {'id': 'calculate_average', 'name': 'Calculate Average', 'category': 'number_operations', 'requires_column': True, 'params': [
                {'name': 'output_type', 'type': 'select', 'label': 'Output As', 'required': True, 'default': 'new_row', 'options': [
                    {'value': 'new_row', 'label': 'Add as new row'},
                    {'value': 'new_column', 'label': 'Add as new column'}
                ]}
            ]},
            {'id': 'find_min_max', 'name': 'Find Min/Max', 'category': 'number_operations', 'requires_column': True, 'params': [
                {'name': 'operation', 'type': 'select', 'label': 'Find', 'required': True, 'default': 'max', 'options': [
                    {'value': 'max', 'label': 'Maximum'},
                    {'value': 'min', 'label': 'Minimum'}
                ]},
                {'name': 'output_type', 'type': 'select', 'label': 'Output As', 'required': True, 'default': 'new_row', 'options': [
                    {'value': 'new_row', 'label': 'Add as new row'},
                    {'value': 'new_column', 'label': 'Add as new column'}
                ]}
            ]},
            {'id': 'remove_decimals', 'name': 'Remove Decimals', 'category': 'number_operations', 'requires_column': True, 'params': []},
            {'id': 'negative_to_positive', 'name': 'Negative to Positive', 'category': 'number_operations', 'requires_column': True, 'params': []},
            {'id': 'number_to_words', 'name': 'Number to Words', 'category': 'number_operations', 'requires_column': True, 'params': [
                {'name': 'language', 'type': 'select', 'label': 'Language', 'required': True, 'default': 'en', 'options': [
                    {'value': 'en', 'label': 'English'},
                    {'value': 'hi', 'label': 'Hindi'}
                ]}
            ]},
            {'id': 'generate_sequence', 'name': 'Generate Sequence', 'category': 'number_operations', 'requires_column': False, 'params': [
                {'name': 'column_name', 'type': 'string', 'label': 'Column Name', 'required': True, 'default': 'serial_no'},
                {'name': 'start_value', 'type': 'number', 'label': 'Start From', 'required': True, 'default': 1},
                {'name': 'step', 'type': 'number', 'label': 'Step', 'required': True, 'default': 1}
            ]}
        ]
        
        # Date/Time Operations (12 operations)
        date_operations = [
            {'id': 'standardize_date_format', 'name': 'Standardize Date Format', 'category': 'date_operations', 'requires_column': True, 'params': [
                {'name': 'output_format', 'type': 'select', 'label': 'Output Format', 'default': 'DD/MM/YYYY', 'options': [
                    {'value': 'DD/MM/YYYY', 'label': 'DD/MM/YYYY (31/12/2024)'},
                    {'value': 'MM/DD/YYYY', 'label': 'MM/DD/YYYY (12/31/2024)'},
                    {'value': 'YYYY-MM-DD', 'label': 'YYYY-MM-DD (2024-12-31)'},
                    {'value': 'DD-MM-YYYY', 'label': 'DD-MM-YYYY (31-12-2024)'},
                    {'value': 'DD MMM YYYY', 'label': 'DD MMM YYYY (31 Dec 2024)'},
                    {'value': 'MMM DD, YYYY', 'label': 'MMM DD, YYYY (Dec 31, 2024)'},
                ]}
            ]},
            {'id': 'extract_year', 'name': 'Extract Year', 'category': 'date_operations', 'requires_column': True, 'params': [
                {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': '', 'placeholder': 'Leave empty to use column_year'}
            ]},
            {'id': 'extract_month', 'name': 'Extract Month', 'category': 'date_operations', 'requires_column': True, 'params': [
                {'name': 'output_type', 'type': 'select', 'label': 'Output As', 'default': 'name', 'options': [
                    {'value': 'name', 'label': 'Full Name (January)'},
                    {'value': 'short_name', 'label': 'Short Name (Jan)'},
                    {'value': 'number', 'label': 'Number (1)'},
                ]},
                {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': '', 'placeholder': 'Leave empty to use column_month'}
            ]},
            {'id': 'extract_day', 'name': 'Extract Day', 'category': 'date_operations', 'requires_column': True, 'params': [
                {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': '', 'placeholder': 'Leave empty to use column_day'}
            ]},
            {'id': 'calculate_age', 'name': 'Calculate Age', 'category': 'date_operations', 'requires_column': True, 'params': [
                {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': 'age'}
            ]},
            {'id': 'add_subtract_days', 'name': 'Add/Subtract Days', 'category': 'date_operations', 'requires_column': True, 'params': [
                {'name': 'operation', 'type': 'select', 'label': 'Operation', 'default': 'add', 'options': [
                    {'value': 'add', 'label': 'Add Days (+)'},
                    {'value': 'subtract', 'label': 'Subtract Days (-)'},
                ]},
                {'name': 'days', 'type': 'number', 'label': 'Number of Days', 'default': 0},
                {'name': 'output_format', 'type': 'select', 'label': 'Output Format', 'default': 'DD/MM/YYYY', 'options': [
                    {'value': 'DD/MM/YYYY', 'label': 'DD/MM/YYYY'},
                    {'value': 'YYYY-MM-DD', 'label': 'YYYY-MM-DD'},
                ]}
            ]},
            {'id': 'find_day_of_week', 'name': 'Find Day of Week', 'category': 'date_operations', 'requires_column': True, 'params': [
                {'name': 'output_type', 'type': 'select', 'label': 'Output As', 'default': 'name', 'options': [
                    {'value': 'name', 'label': 'Full Name (Monday)'},
                    {'value': 'short_name', 'label': 'Short Name (Mon)'},
                    {'value': 'number', 'label': 'Number (1-7)'},
                ]},
                {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': '', 'placeholder': 'Leave empty to use column_weekday'}
            ]},
            {'id': 'calculate_duration', 'name': 'Calculate Duration', 'category': 'date_operations', 'requires_column': True, 'params': [
                {'name': 'end_column', 'type': 'column_select', 'label': 'End Date Column', 'required': True},
                {'name': 'unit', 'type': 'select', 'label': 'Calculate In', 'default': 'days', 'options': [
                    {'value': 'days', 'label': 'Days'},
                    {'value': 'weeks', 'label': 'Weeks'},
                    {'value': 'months', 'label': 'Months'},
                    {'value': 'years', 'label': 'Years'},
                ]},
                {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': 'duration_days'}
            ]},
            {'id': 'insert_current_date', 'name': 'Insert Current Date', 'category': 'date_operations', 'requires_column': False, 'params': [
                {'name': 'column_name', 'type': 'string', 'label': 'Column Name', 'default': 'current_date'},
                {'name': 'date_format', 'type': 'select', 'label': 'Date Format', 'default': 'DD/MM/YYYY', 'options': [
                    {'value': 'DD/MM/YYYY', 'label': 'DD/MM/YYYY'},
                    {'value': 'YYYY-MM-DD', 'label': 'YYYY-MM-DD'},
                    {'value': 'DD MMM YYYY', 'label': 'DD MMM YYYY'},
                ]}
            ]},
            {'id': 'insert_current_time', 'name': 'Insert Current Time', 'category': 'date_operations', 'requires_column': False, 'params': [
                {'name': 'column_name', 'type': 'string', 'label': 'Column Name', 'default': 'current_time'},
                {'name': 'time_format', 'type': 'select', 'label': 'Time Format', 'default': 'HH:MM:SS', 'options': [
                    {'value': 'HH:MM:SS', 'label': '24-hour (14:30:00)'},
                    {'value': 'HH:MM', 'label': '24-hour short (14:30)'},
                    {'value': 'hh:mm:ss AM/PM', 'label': '12-hour (02:30:00 PM)'},
                    {'value': 'hh:mm AM/PM', 'label': '12-hour short (02:30 PM)'},
                ]}
            ]},
            {'id': 'convert_text_to_date', 'name': 'Convert Text to Date', 'category': 'date_operations', 'requires_column': True, 'params': [
                {'name': 'output_format', 'type': 'select', 'label': 'Output Format', 'default': 'DD/MM/YYYY', 'options': [
                    {'value': 'DD/MM/YYYY', 'label': 'DD/MM/YYYY'},
                    {'value': 'MM/DD/YYYY', 'label': 'MM/DD/YYYY'},
                    {'value': 'YYYY-MM-DD', 'label': 'YYYY-MM-DD'},
                ]}
            ]},
            {'id': 'quarter_calculation', 'name': 'Quarter Calculation', 'category': 'date_operations', 'requires_column': True, 'params': [
                {'name': 'output_format', 'type': 'select', 'label': 'Output Format', 'default': 'Q1', 'options': [
                    {'value': 'Q1', 'label': 'Q1, Q2, Q3, Q4'},
                    {'value': 'number', 'label': '1, 2, 3, 4'},
                    {'value': 'Q1 YYYY', 'label': 'Q1 2024'},
                ]},
                {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': '', 'placeholder': 'Leave empty to use column_quarter'}
            ]},
        ]
        
        # Formatting Macros (15 operations)
        formatting_operations = [
            {'id': 'autofit_column_width', 'name': 'Auto-fit Column Width', 'category': 'formatting', 'requires_column': False, 'params': [
                {'name': 'column', 'type': 'column_select', 'label': 'Column (leave empty for all)', 'required': False}
            ]},
            {'id': 'autofit_row_height', 'name': 'Auto-fit Row Height', 'category': 'formatting', 'requires_column': False, 'params': [
                {'name': 'row_range', 'type': 'string', 'label': 'Row Range (e.g., 1-10 or all)', 'default': 'all'}
            ]},
            {'id': 'apply_bold_italic', 'name': 'Apply Bold/Italic', 'category': 'formatting', 'requires_column': True, 'params': [
                {'name': 'style', 'type': 'select', 'label': 'Style', 'default': 'bold', 'options': [
                    {'value': 'bold', 'label': 'Bold'},
                    {'value': 'italic', 'label': 'Italic'},
                    {'value': 'both', 'label': 'Bold + Italic'},
                ]},
                {'name': 'row_range', 'type': 'select', 'label': 'Apply To', 'default': 'all', 'options': [
                    {'value': 'header', 'label': 'Header Only'},
                    {'value': 'data', 'label': 'Data Only'},
                    {'value': 'all', 'label': 'All Rows'},
                ]}
            ]},
            {'id': 'change_font', 'name': 'Change Font', 'category': 'formatting', 'requires_column': True, 'params': [
                {'name': 'font_name', 'type': 'select', 'label': 'Font', 'default': 'Calibri', 'options': [
                    {'value': 'Calibri', 'label': 'Calibri'},
                    {'value': 'Arial', 'label': 'Arial'},
                    {'value': 'Times New Roman', 'label': 'Times New Roman'},
                    {'value': 'Verdana', 'label': 'Verdana'},
                    {'value': 'Georgia', 'label': 'Georgia'},
                    {'value': 'Tahoma', 'label': 'Tahoma'},
                    {'value': 'Trebuchet MS', 'label': 'Trebuchet MS'},
                    {'value': 'Courier New', 'label': 'Courier New'},
                ]}
            ]},
            {'id': 'change_font_size', 'name': 'Change Font Size', 'category': 'formatting', 'requires_column': True, 'params': [
                {'name': 'font_size', 'type': 'select', 'label': 'Size', 'default': '11', 'options': [
                    {'value': '8', 'label': '8'},
                    {'value': '9', 'label': '9'},
                    {'value': '10', 'label': '10'},
                    {'value': '11', 'label': '11'},
                    {'value': '12', 'label': '12'},
                    {'value': '14', 'label': '14'},
                    {'value': '16', 'label': '16'},
                    {'value': '18', 'label': '18'},
                    {'value': '20', 'label': '20'},
                    {'value': '24', 'label': '24'},
                    {'value': '28', 'label': '28'},
                    {'value': '36', 'label': '36'},
                ]}
            ]},
            {'id': 'apply_cell_color', 'name': 'Apply Cell Color', 'category': 'formatting', 'requires_column': True, 'params': [
                {'name': 'color', 'type': 'select', 'label': 'Background Color', 'default': 'yellow', 'options': [
                    {'value': 'yellow', 'label': 'Yellow'},
                    {'value': 'light_green', 'label': 'Light Green'},
                    {'value': 'light_blue', 'label': 'Light Blue'},
                    {'value': 'light_red', 'label': 'Light Red'},
                    {'value': 'orange', 'label': 'Orange'},
                    {'value': 'cyan', 'label': 'Cyan'},
                    {'value': 'pink', 'label': 'Pink'},
                    {'value': 'gray', 'label': 'Gray'},
                ]},
                {'name': 'row_condition', 'type': 'select', 'label': 'Apply To', 'default': 'all', 'options': [
                    {'value': 'header', 'label': 'Header Only'},
                    {'value': 'data', 'label': 'Data Only'},
                    {'value': 'all', 'label': 'All Rows'},
                ]}
            ]},
            {'id': 'apply_text_color', 'name': 'Apply Text Color', 'category': 'formatting', 'requires_column': True, 'params': [
                {'name': 'color', 'type': 'select', 'label': 'Text Color', 'default': 'red', 'options': [
                    {'value': 'red', 'label': 'Red'},
                    {'value': 'green', 'label': 'Green'},
                    {'value': 'blue', 'label': 'Blue'},
                    {'value': 'orange', 'label': 'Orange'},
                    {'value': 'purple', 'label': 'Purple'},
                    {'value': 'black', 'label': 'Black'},
                ]},
                {'name': 'condition', 'type': 'select', 'label': 'Condition (Optional)', 'default': '', 'options': [
                    {'value': '', 'label': 'Always Apply'},
                    {'value': 'negative', 'label': 'Negative Numbers Only'},
                    {'value': 'less_than', 'label': 'Less Than Value'},
                    {'value': 'greater_than', 'label': 'Greater Than Value'},
                ]},
                {'name': 'condition_value', 'type': 'number', 'label': 'Condition Value', 'default': 0}
            ]},
            {'id': 'add_borders', 'name': 'Add Borders', 'category': 'formatting', 'requires_column': False, 'params': [
                {'name': 'column', 'type': 'column_select', 'label': 'Column (leave empty for all)', 'required': False},
                {'name': 'border_style', 'type': 'select', 'label': 'Border Style', 'default': 'thin', 'options': [
                    {'value': 'thin', 'label': 'Thin'},
                    {'value': 'medium', 'label': 'Medium'},
                    {'value': 'thick', 'label': 'Thick'},
                    {'value': 'dashed', 'label': 'Dashed'},
                    {'value': 'dotted', 'label': 'Dotted'},
                ]}
            ]},
            {'id': 'merge_cells', 'name': 'Merge Cells', 'category': 'formatting', 'requires_column': False, 'params': [
                {'name': 'cell_range', 'type': 'string', 'label': 'Cell Range (e.g., A1:D1)', 'required': True, 'placeholder': 'A1:D1'}
            ]},
            {'id': 'unmerge_cells', 'name': 'Unmerge Cells', 'category': 'formatting', 'requires_column': False, 'params': [
                {'name': 'cell_range', 'type': 'string', 'label': 'Cell Range (e.g., A1:D1)', 'required': True, 'placeholder': 'A1:D1'}
            ]},
            {'id': 'align_text', 'name': 'Align Text', 'category': 'formatting', 'requires_column': True, 'params': [
                {'name': 'horizontal', 'type': 'select', 'label': 'Horizontal Alignment', 'default': 'center', 'options': [
                    {'value': 'left', 'label': 'Left'},
                    {'value': 'center', 'label': 'Center'},
                    {'value': 'right', 'label': 'Right'},
                ]},
                {'name': 'vertical', 'type': 'select', 'label': 'Vertical Alignment', 'default': 'center', 'options': [
                    {'value': 'top', 'label': 'Top'},
                    {'value': 'center', 'label': 'Center'},
                    {'value': 'bottom', 'label': 'Bottom'},
                ]}
            ]},
            {'id': 'apply_number_format', 'name': 'Apply Number Format', 'category': 'formatting', 'requires_column': True, 'params': [
                {'name': 'format_type', 'type': 'select', 'label': 'Format Type', 'default': 'comma', 'options': [
                    {'value': 'comma', 'label': 'Comma (1,000)'},
                    {'value': 'comma_decimal', 'label': 'Comma with Decimal (1,000.00)'},
                    {'value': 'currency_usd', 'label': 'USD Currency ($1,000.00)'},
                    {'value': 'currency_inr', 'label': 'INR Currency (₹1,000.00)'},
                    {'value': 'currency_eur', 'label': 'EUR Currency (€1,000.00)'},
                    {'value': 'percentage', 'label': 'Percentage (50.00%)'},
                    {'value': 'percentage_whole', 'label': 'Percentage Whole (50%)'},
                    {'value': 'decimal_2', 'label': '2 Decimal Places'},
                    {'value': 'decimal_4', 'label': '4 Decimal Places'},
                    {'value': 'scientific', 'label': 'Scientific Notation'},
                ]}
            ]},
            {'id': 'conditional_formatting', 'name': 'Conditional Formatting', 'category': 'formatting', 'requires_column': True, 'params': [
                {'name': 'condition', 'type': 'select', 'label': 'Condition', 'default': 'less_than', 'options': [
                    {'value': 'less_than', 'label': 'Less Than (<)'},
                    {'value': 'greater_than', 'label': 'Greater Than (>)'},
                    {'value': 'equal', 'label': 'Equal To (=)'},
                    {'value': 'not_equal', 'label': 'Not Equal To (≠)'},
                    {'value': 'less_than_or_equal', 'label': 'Less Than or Equal (≤)'},
                    {'value': 'greater_than_or_equal', 'label': 'Greater Than or Equal (≥)'},
                ]},
                {'name': 'value', 'type': 'number', 'label': 'Value', 'default': 0},
                {'name': 'color', 'type': 'select', 'label': 'Highlight Color', 'default': 'red', 'options': [
                    {'value': 'red', 'label': 'Red'},
                    {'value': 'yellow', 'label': 'Yellow'},
                    {'value': 'green', 'label': 'Green'},
                    {'value': 'orange', 'label': 'Orange'},
                    {'value': 'light_red', 'label': 'Light Red'},
                    {'value': 'light_green', 'label': 'Light Green'},
                ]}
            ]},
            {'id': 'copy_formatting', 'name': 'Copy Formatting', 'category': 'formatting', 'requires_column': True, 'params': [
                {'name': 'target_column', 'type': 'column_select', 'label': 'Target Column', 'required': True}
            ]},
            {'id': 'clear_formatting', 'name': 'Clear Formatting', 'category': 'formatting', 'requires_column': False, 'params': [
                {'name': 'column', 'type': 'column_select', 'label': 'Column (leave empty for all)', 'required': False}
            ]},
        ]
        
        # ===== DATA VALIDATION OPERATIONS (12) =====
        validation_operations = [
            {
                'id': 'validate_email',
                'name': 'Validate Email',
                'description': 'Check if email addresses are valid format',
                'category': 'validation',
                'requires_column': True,
                'params': [
                    {'name': 'column', 'type': 'column', 'required': True, 'label': 'Email Column'},
                    {'name': 'add_validation_column', 'type': 'boolean', 'required': False, 'label': 'Add Status Column', 'default': True},
                ]
            },
            {
                'id': 'validate_phone',
                'name': 'Validate Phone',
                'description': 'Check if phone numbers are valid (10 digits for India)',
                'category': 'validation',
                'requires_column': True,
                'params': [
                    {'name': 'column', 'type': 'column', 'required': True, 'label': 'Phone Column'},
                    {'name': 'country_code', 'type': 'select', 'required': False, 'label': 'Country', 'default': 'IN',
                     'options': [
                         {'value': 'IN', 'label': 'India (10 digits)'},
                         {'value': 'US', 'label': 'USA (10 digits)'},
                         {'value': 'UK', 'label': 'UK (10-11 digits)'},
                         {'value': 'GENERIC', 'label': 'Generic (10-15 digits)'},
                     ]},
                ]
            },
            {
                'id': 'validate_date',
                'name': 'Validate Date',
                'description': 'Check if dates are in valid format',
                'category': 'validation',
                'requires_column': True,
                'params': [
                    {'name': 'column', 'type': 'column', 'required': True, 'label': 'Date Column'},
                    {'name': 'date_format', 'type': 'select', 'required': False, 'label': 'Expected Format', 'default': 'auto',
                     'options': [
                         {'value': 'auto', 'label': 'Auto Detect'},
                         {'value': 'DD/MM/YYYY', 'label': 'DD/MM/YYYY'},
                         {'value': 'MM/DD/YYYY', 'label': 'MM/DD/YYYY'},
                         {'value': 'YYYY-MM-DD', 'label': 'YYYY-MM-DD'},
                         {'value': 'DD-MM-YYYY', 'label': 'DD-MM-YYYY'},
                     ]},
                ]
            },
            {
                'id': 'check_for_blanks',
                'name': 'Check for Blanks',
                'description': 'Find empty/blank cells in a column',
                'category': 'validation',
                'requires_column': True,
                'params': [
                    {'name': 'column', 'type': 'column', 'required': True, 'label': 'Column to Check'},
                    {'name': 'action', 'type': 'select', 'required': False, 'label': 'Action', 'default': 'flag',
                     'options': [
                         {'value': 'flag', 'label': 'Flag blanks (add status column)'},
                         {'value': 'fill', 'label': 'Fill blanks with value'},
                         {'value': 'remove', 'label': 'Remove blank rows'},
                     ]},
                    {'name': 'fill_value', 'type': 'text', 'required': False, 'label': 'Fill Value (if action=fill)'},
                ]
            },
            {
                'id': 'check_data_type',
                'name': 'Check Data Type',
                'description': 'Verify column contains expected data type',
                'category': 'validation',
                'requires_column': True,
                'params': [
                    {'name': 'column', 'type': 'column', 'required': True, 'label': 'Column to Check'},
                    {'name': 'expected_type', 'type': 'select', 'required': True, 'label': 'Expected Type',
                     'options': [
                         {'value': 'number', 'label': 'Number (any)'},
                         {'value': 'integer', 'label': 'Integer (whole number)'},
                         {'value': 'decimal', 'label': 'Decimal'},
                         {'value': 'text', 'label': 'Text (not number)'},
                         {'value': 'boolean', 'label': 'Boolean (Yes/No/True/False)'},
                     ]},
                ]
            },
            {
                'id': 'validate_range',
                'name': 'Validate Range',
                'description': 'Check if values are within min-max range',
                'category': 'validation',
                'requires_column': True,
                'params': [
                    {'name': 'column', 'type': 'column', 'required': True, 'label': 'Column to Check'},
                    {'name': 'min_value', 'type': 'number', 'required': False, 'label': 'Minimum Value'},
                    {'name': 'max_value', 'type': 'number', 'required': False, 'label': 'Maximum Value'},
                ]
            },
            {
                'id': 'check_duplicates',
                'name': 'Check Duplicates',
                'description': 'Find duplicate values in a column',
                'category': 'validation',
                'requires_column': True,
                'params': [
                    {'name': 'column', 'type': 'column', 'required': True, 'label': 'Column to Check'},
                    {'name': 'action', 'type': 'select', 'required': False, 'label': 'Action', 'default': 'flag',
                     'options': [
                         {'value': 'flag', 'label': 'Flag duplicates'},
                         {'value': 'remove', 'label': 'Remove duplicates'},
                     ]},
                    {'name': 'keep', 'type': 'select', 'required': False, 'label': 'Keep', 'default': 'first',
                     'options': [
                         {'value': 'first', 'label': 'Keep first occurrence'},
                         {'value': 'last', 'label': 'Keep last occurrence'},
                         {'value': 'none', 'label': 'Remove all duplicates'},
                     ]},
                ]
            },
            {
                'id': 'validate_length',
                'name': 'Validate Length',
                'description': 'Check text length (e.g., PIN code = 6 digits)',
                'category': 'validation',
                'requires_column': True,
                'params': [
                    {'name': 'column', 'type': 'column', 'required': True, 'label': 'Column to Check'},
                    {'name': 'exact_length', 'type': 'number', 'required': False, 'label': 'Exact Length'},
                    {'name': 'min_length', 'type': 'number', 'required': False, 'label': 'Minimum Length'},
                    {'name': 'max_length', 'type': 'number', 'required': False, 'label': 'Maximum Length'},
                ]
            },
            {
                'id': 'check_required_fields',
                'name': 'Check Required Fields',
                'description': 'Verify mandatory fields are not empty',
                'category': 'validation',
                'requires_column': False,
                'params': [
                    {'name': 'columns', 'type': 'text', 'required': True, 'label': 'Required Columns (comma-separated)'},
                ]
            },
            {
                'id': 'validate_pan_aadhaar',
                'name': 'Validate PAN/Aadhaar',
                'description': 'Validate Indian ID formats (PAN, Aadhaar, GST)',
                'category': 'validation',
                'requires_column': True,
                'params': [
                    {'name': 'column', 'type': 'column', 'required': True, 'label': 'ID Column'},
                    {'name': 'id_type', 'type': 'select', 'required': True, 'label': 'ID Type',
                     'options': [
                         {'value': 'PAN', 'label': 'PAN Card (ABCDE1234F)'},
                         {'value': 'AADHAAR', 'label': 'Aadhaar (12 digits)'},
                         {'value': 'GST', 'label': 'GST Number'},
                         {'value': 'PASSPORT', 'label': 'Passport'},
                         {'value': 'VOTER_ID', 'label': 'Voter ID'},
                         {'value': 'IFSC', 'label': 'Bank IFSC Code'},
                     ]},
                ]
            },
            {
                'id': 'highlight_errors',
                'name': 'Highlight Errors',
                'description': 'Mark cells with errors',
                'category': 'validation',
                'requires_column': True,
                'params': [
                    {'name': 'column', 'type': 'column', 'required': True, 'label': 'Column to Check'},
                    {'name': 'error_type', 'type': 'select', 'required': False, 'label': 'Error Type', 'default': 'any',
                     'options': [
                         {'value': 'any', 'label': 'Any Error'},
                         {'value': 'blank', 'label': 'Blank Cells'},
                         {'value': 'invalid_email', 'label': 'Invalid Email'},
                         {'value': 'invalid_number', 'label': 'Invalid Number'},
                     ]},
                ]
            },
            {
                'id': 'create_error_report',
                'name': 'Create Error Report',
                'description': 'Generate summary of all errors in selected columns',
                'category': 'validation',
                'requires_column': False,
                'params': [
                    {'name': 'columns', 'type': 'text', 'required': False, 'label': 'Columns to Check (blank=all)'},
                    {'name': 'output_column', 'type': 'text', 'required': False, 'label': 'Output Column Name', 'default': 'error_summary'},
                ]
            },
        ]
        
        return base_operations + text_transformations + number_operations + date_operations + formatting_operations + validation_operations + row_column_operations
        # Row/Column Operations (12 operations)
        row_column_operations = [
            {
                'id': 'insert_rows',
                'name': 'Insert Rows',
                'description': 'Insert new blank rows at specified positions',
                'category': 'row_column',
                'requires_column': False,
                'params': [
                    {'name': 'position', 'type': 'select', 'options': ['after_every_n', 'at_index', 'at_end'], 'default': 'at_end'},
                    {'name': 'interval', 'type': 'number', 'label': 'After every N rows', 'default': 5, 'show_if': {'position': 'after_every_n'}},
                    {'name': 'index', 'type': 'number', 'label': 'Row index', 'default': 0, 'show_if': {'position': 'at_index'}},
                    {'name': 'count', 'type': 'number', 'label': 'Number of rows to insert', 'default': 1}
                ]
            },
            {
                'id': 'insert_columns',
                'name': 'Insert Columns',
                'description': 'Insert new columns at specified positions',
                'category': 'row_column',
                'requires_column': False,
                'params': [
                    {'name': 'column_name', 'type': 'text', 'label': 'New column name', 'required': True},
                    {'name': 'position', 'type': 'select', 'options': ['at_end', 'before', 'after', 'at_index'], 'default': 'at_end'},
                    {'name': 'reference_column', 'type': 'column', 'label': 'Reference column', 'show_if': {'position': ['before', 'after']}},
                    {'name': 'default_value', 'type': 'text', 'label': 'Default value', 'default': ''}
                ]
            },
            {
                'id': 'delete_rows',
                'name': 'Delete Rows',
                'description': 'Delete rows based on conditions',
                'category': 'row_column',
                'requires_column': False,
                'params': [
                    {'name': 'condition', 'type': 'select', 'options': ['blank_rows', 'duplicate_rows', 'by_value', 'by_index'], 'default': 'blank_rows'},
                    {'name': 'column', 'type': 'column', 'label': 'Column to check', 'show_if': {'condition': 'by_value'}},
                    {'name': 'value', 'type': 'text', 'label': 'Value to match', 'show_if': {'condition': 'by_value'}},
                    {'name': 'indices', 'type': 'text', 'label': 'Row indices (comma-separated)', 'show_if': {'condition': 'by_index'}}
                ]
            },
            {
                'id': 'delete_columns',
                'name': 'Delete Columns',
                'description': 'Delete specified columns',
                'category': 'row_column',
                'requires_column': False,
                'params': [
                    {'name': 'condition', 'type': 'select', 'options': ['by_name', 'empty_columns'], 'default': 'by_name'},
                    {'name': 'columns', 'type': 'text', 'label': 'Column names (comma-separated)', 'show_if': {'condition': 'by_name'}}
                ]
            },
            {
                'id': 'hide_rows',
                'name': 'Hide Rows',
                'description': 'Mark rows as hidden based on conditions',
                'category': 'row_column',
                'requires_column': True,
                'params': [
                    {'name': 'condition', 'type': 'select', 'options': ['by_value', 'by_index'], 'default': 'by_value'},
                    {'name': 'value', 'type': 'text', 'label': 'Value to match', 'show_if': {'condition': 'by_value'}},
                    {'name': 'indices', 'type': 'text', 'label': 'Row indices (comma-separated)', 'show_if': {'condition': 'by_index'}}
                ]
            },
            {
                'id': 'hide_columns',
                'name': 'Hide Columns',
                'description': 'Mark columns as hidden',
                'category': 'row_column',
                'requires_column': False,
                'params': [
                    {'name': 'columns', 'type': 'text', 'label': 'Column names to hide (comma-separated)', 'required': True}
                ]
            },
            {
                'id': 'unhide_all',
                'name': 'Unhide All',
                'description': 'Show all hidden rows and columns',
                'category': 'row_column',
                'requires_column': False,
                'params': [
                    {'name': 'target', 'type': 'select', 'options': ['all', 'rows', 'columns'], 'default': 'all'}
                ]
            },
            {
                'id': 'sort_rows',
                'name': 'Sort Rows',
                'description': 'Sort data by column values',
                'category': 'row_column',
                'requires_column': True,
                'params': [
                    {'name': 'order', 'type': 'select', 'options': ['asc', 'desc'], 'default': 'asc', 'label': 'Sort Order'}
                ]
            },
            {
                'id': 'filter_data',
                'name': 'Filter Data',
                'description': 'Filter rows by condition',
                'category': 'row_column',
                'requires_column': True,
                'params': [
                    {'name': 'operator', 'type': 'select', 'options': ['equals', 'not_equals', 'contains', 'starts_with', 'ends_with', 'greater_than', 'less_than', 'not_empty', 'is_empty', 'in_list'], 'default': 'equals'},
                    {'name': 'value', 'type': 'text', 'label': 'Value', 'placeholder': 'Enter value or comma-separated list'}
                ]
            },
            {
                'id': 'transpose',
                'name': 'Transpose',
                'description': 'Swap rows and columns',
                'category': 'row_column',
                'requires_column': False,
                'params': [
                    {'name': 'use_first_column_as_headers', 'type': 'boolean', 'label': 'Use first column as new headers', 'default': True}
                ]
            },
            {
                'id': 'group_rows',
                'name': 'Group Rows',
                'description': 'Group rows by column value',
                'category': 'row_column',
                'requires_column': True,
                'params': [
                    {'name': 'add_subtotals', 'type': 'boolean', 'label': 'Add subtotals', 'default': False},
                    {'name': 'subtotal_columns', 'type': 'text', 'label': 'Columns for subtotals (comma-separated)', 'show_if': {'add_subtotals': True}}
                ]
            },
            {
                'id': 'freeze_panes',
                'name': 'Freeze Panes',
                'description': 'Set freeze panes for Excel export',
                'category': 'row_column',
                'requires_column': False,
                'params': [
                    {'name': 'freeze_rows', 'type': 'number', 'label': 'Rows to freeze', 'default': 1},
                    {'name': 'freeze_columns', 'type': 'number', 'label': 'Columns to freeze', 'default': 0}
                ]
            }
        ]