"""
Data Validation Helper Functions for SmartSheet Pro
12 validation operations for data quality checks
"""

import re
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple


class DataValidationOperations:
    """
    Data Validation Macros - 12 operations for checking data quality
    """
    
    # ===== 1. VALIDATE EMAIL =====
    @staticmethod
    def validate_email(df: pd.DataFrame, column: str, 
                       add_validation_column: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Validate email addresses in a column.
        Adds a validation status column showing valid/invalid.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        # Email regex pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        # Validate each email
        def check_email(value):
            if pd.isna(value) or str(value).strip() == '':
                return 'BLANK'
            value_str = str(value).strip().lower()
            if re.match(email_pattern, value_str):
                return 'VALID'
            return 'INVALID'
        
        validation_results = df[column].apply(check_email)
        
        # Count results
        valid_count = (validation_results == 'VALID').sum()
        invalid_count = (validation_results == 'INVALID').sum()
        blank_count = (validation_results == 'BLANK').sum()
        
        if add_validation_column:
            df[f'{column}_email_status'] = validation_results
        
        results = {
            'total_rows': len(df),
            'valid_emails': int(valid_count),
            'invalid_emails': int(invalid_count),
            'blank_cells': int(blank_count),
            'validation_rate': round(valid_count / len(df) * 100, 2) if len(df) > 0 else 0
        }
        
        return df, results
    
    # ===== 2. VALIDATE PHONE =====
    @staticmethod
    def validate_phone(df: pd.DataFrame, column: str, 
                       country_code: str = 'IN',
                       add_validation_column: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Validate phone numbers in a column.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        # Phone patterns by country
        phone_patterns = {
            'IN': r'^[6-9]\d{9}$',  # India: 10 digits starting with 6-9
            'US': r'^\d{10}$',       # US: 10 digits
            'UK': r'^\d{10,11}$',    # UK: 10-11 digits
            'GENERIC': r'^\d{10,15}$'  # Generic: 10-15 digits
        }
        
        pattern = phone_patterns.get(country_code.upper(), phone_patterns['GENERIC'])
        
        def check_phone(value):
            if pd.isna(value) or str(value).strip() == '':
                return 'BLANK'
            # Remove common formatting characters
            cleaned = re.sub(r'[\s\-\(\)\+]', '', str(value))
            # Remove country code if present
            if cleaned.startswith('91') and len(cleaned) == 12:
                cleaned = cleaned[2:]
            elif cleaned.startswith('+91') and len(cleaned) == 13:
                cleaned = cleaned[3:]
            
            if re.match(pattern, cleaned):
                return 'VALID'
            return 'INVALID'
        
        validation_results = df[column].apply(check_phone)
        
        valid_count = (validation_results == 'VALID').sum()
        invalid_count = (validation_results == 'INVALID').sum()
        blank_count = (validation_results == 'BLANK').sum()
        
        if add_validation_column:
            df[f'{column}_phone_status'] = validation_results
        
        results = {
            'total_rows': len(df),
            'valid_phones': int(valid_count),
            'invalid_phones': int(invalid_count),
            'blank_cells': int(blank_count),
            'country_code': country_code
        }
        
        return df, results
    
    # ===== 3. VALIDATE DATE =====
    @staticmethod
    def validate_date(df: pd.DataFrame, column: str,
                      date_format: str = 'auto',
                      add_validation_column: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Validate date values in a column.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        format_mappings = {
            'DD/MM/YYYY': '%d/%m/%Y',
            'MM/DD/YYYY': '%m/%d/%Y',
            'YYYY-MM-DD': '%Y-%m-%d',
            'DD-MM-YYYY': '%d-%m-%Y',
            'YYYY/MM/DD': '%Y/%m/%d',
        }
        
        def check_date(value):
            if pd.isna(value) or str(value).strip() == '':
                return 'BLANK'
            
            value_str = str(value).strip()
            
            # If auto, try multiple formats
            if date_format == 'auto':
                formats_to_try = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y', 
                                  '%Y/%m/%d', '%d.%m.%Y', '%Y-%m-%d %H:%M:%S']
                for fmt in formats_to_try:
                    try:
                        datetime.strptime(value_str.split()[0] if ' ' in value_str else value_str, fmt)
                        return 'VALID'
                    except ValueError:
                        continue
                # Try pandas parsing as last resort
                try:
                    pd.to_datetime(value_str)
                    return 'VALID'
                except:
                    return 'INVALID'
            else:
                # Use specific format
                fmt = format_mappings.get(date_format, date_format)
                try:
                    datetime.strptime(value_str, fmt)
                    return 'VALID'
                except ValueError:
                    return 'INVALID'
        
        validation_results = df[column].apply(check_date)
        
        valid_count = (validation_results == 'VALID').sum()
        invalid_count = (validation_results == 'INVALID').sum()
        blank_count = (validation_results == 'BLANK').sum()
        
        if add_validation_column:
            df[f'{column}_date_status'] = validation_results
        
        results = {
            'total_rows': len(df),
            'valid_dates': int(valid_count),
            'invalid_dates': int(invalid_count),
            'blank_cells': int(blank_count),
            'format_checked': date_format
        }
        
        return df, results
    
    # ===== 4. CHECK FOR BLANKS =====
    @staticmethod
    def check_for_blanks(df: pd.DataFrame, column: str,
                         action: str = 'flag',
                         fill_value: Any = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Find and optionally handle blank/empty cells.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        # Check for blanks (NaN, None, empty string, whitespace only)
        def is_blank(value):
            if pd.isna(value):
                return True
            if isinstance(value, str) and value.strip() == '':
                return True
            return False
        
        blank_mask = df[column].apply(is_blank)
        blank_count = blank_mask.sum()
        original_rows = len(df)
        
        if action == 'flag':
            df[f'{column}_is_blank'] = blank_mask.map({True: 'BLANK', False: 'HAS_VALUE'})
        elif action == 'fill' and fill_value is not None:
            df.loc[blank_mask, column] = fill_value
        elif action == 'remove':
            df = df[~blank_mask].reset_index(drop=True)
        
        results = {
            'total_rows': original_rows,
            'blank_cells': int(blank_count),
            'non_blank_cells': int(original_rows - blank_count),
            'blank_percentage': round(blank_count / original_rows * 100, 2) if original_rows > 0 else 0,
            'action_taken': action,
            'rows_after_action': len(df)
        }
        
        return df, results
    
    # ===== 5. CHECK DATA TYPE =====
    @staticmethod
    def check_data_type(df: pd.DataFrame, column: str,
                        expected_type: str = 'number',
                        add_validation_column: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Check if column values match expected data type.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        def check_type(value):
            if pd.isna(value) or str(value).strip() == '':
                return 'BLANK'
            
            value_str = str(value).strip()
            
            if expected_type == 'number':
                try:
                    float(value_str.replace(',', ''))
                    return 'VALID'
                except ValueError:
                    return 'INVALID'
                    
            elif expected_type == 'integer':
                try:
                    val = float(value_str.replace(',', ''))
                    if val == int(val):
                        return 'VALID'
                    return 'INVALID'
                except ValueError:
                    return 'INVALID'
                    
            elif expected_type == 'decimal':
                try:
                    val = float(value_str.replace(',', ''))
                    if '.' in value_str or val != int(val):
                        return 'VALID'
                    return 'INVALID'
                except ValueError:
                    return 'INVALID'
                    
            elif expected_type == 'text':
                # Check if it's NOT a number (pure text)
                try:
                    float(value_str.replace(',', ''))
                    return 'INVALID'  # It's a number, not text
                except ValueError:
                    return 'VALID'  # It's text
                    
            elif expected_type == 'boolean':
                if value_str.lower() in ['true', 'false', 'yes', 'no', '1', '0', 'y', 'n']:
                    return 'VALID'
                return 'INVALID'
            
            return 'UNKNOWN'
        
        validation_results = df[column].apply(check_type)
        
        valid_count = (validation_results == 'VALID').sum()
        invalid_count = (validation_results == 'INVALID').sum()
        blank_count = (validation_results == 'BLANK').sum()
        
        if add_validation_column:
            df[f'{column}_type_status'] = validation_results
        
        results = {
            'total_rows': len(df),
            'valid_type': int(valid_count),
            'invalid_type': int(invalid_count),
            'blank_cells': int(blank_count),
            'expected_type': expected_type
        }
        
        return df, results
    
    # ===== 6. VALIDATE RANGE =====
    @staticmethod
    def validate_range(df: pd.DataFrame, column: str,
                       min_value: float = None,
                       max_value: float = None,
                       add_validation_column: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Check if numeric values are within specified range.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        def check_range(value):
            if pd.isna(value) or str(value).strip() == '':
                return 'BLANK'
            
            try:
                num_value = float(str(value).replace(',', ''))
                
                if min_value is not None and num_value < min_value:
                    return 'BELOW_MIN'
                if max_value is not None and num_value > max_value:
                    return 'ABOVE_MAX'
                return 'VALID'
            except ValueError:
                return 'NOT_A_NUMBER'
        
        validation_results = df[column].apply(check_range)
        
        valid_count = (validation_results == 'VALID').sum()
        below_min = (validation_results == 'BELOW_MIN').sum()
        above_max = (validation_results == 'ABOVE_MAX').sum()
        not_number = (validation_results == 'NOT_A_NUMBER').sum()
        blank_count = (validation_results == 'BLANK').sum()
        
        if add_validation_column:
            df[f'{column}_range_status'] = validation_results
        
        results = {
            'total_rows': len(df),
            'valid_in_range': int(valid_count),
            'below_minimum': int(below_min),
            'above_maximum': int(above_max),
            'not_a_number': int(not_number),
            'blank_cells': int(blank_count),
            'min_value': min_value,
            'max_value': max_value
        }
        
        return df, results
    
    # ===== 7. CHECK DUPLICATES =====
    @staticmethod
    def check_duplicates(df: pd.DataFrame, column: str,
                         action: str = 'flag',
                         keep: str = 'first') -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Find and optionally handle duplicate values.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        original_rows = len(df)
        
        # Find duplicates
        if keep == 'none':
            duplicate_mask = df.duplicated(subset=[column], keep=False)
        else:
            duplicate_mask = df.duplicated(subset=[column], keep=keep)
        
        duplicate_count = duplicate_mask.sum()
        unique_count = df[column].nunique()
        
        # Get list of duplicate values
        duplicate_values = df[duplicate_mask][column].unique().tolist()[:10]  # First 10
        
        if action == 'flag':
            df[f'{column}_is_duplicate'] = duplicate_mask.map({True: 'DUPLICATE', False: 'UNIQUE'})
        elif action == 'remove':
            df = df[~duplicate_mask].reset_index(drop=True)
        
        results = {
            'total_rows': original_rows,
            'duplicate_rows': int(duplicate_count),
            'unique_values': int(unique_count),
            'duplicate_percentage': round(duplicate_count / original_rows * 100, 2) if original_rows > 0 else 0,
            'sample_duplicates': duplicate_values,
            'action_taken': action,
            'rows_after_action': len(df)
        }
        
        return df, results
    
    # ===== 8. VALIDATE LENGTH =====
    @staticmethod
    def validate_length(df: pd.DataFrame, column: str,
                        min_length: int = None,
                        max_length: int = None,
                        exact_length: int = None,
                        add_validation_column: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Validate text length in a column.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        def check_length(value):
            if pd.isna(value) or str(value).strip() == '':
                return 'BLANK'
            
            length = len(str(value).strip())
            
            if exact_length is not None:
                return 'VALID' if length == exact_length else 'INVALID_LENGTH'
            
            if min_length is not None and length < min_length:
                return 'TOO_SHORT'
            if max_length is not None and length > max_length:
                return 'TOO_LONG'
            
            return 'VALID'
        
        validation_results = df[column].apply(check_length)
        
        valid_count = (validation_results == 'VALID').sum()
        too_short = (validation_results == 'TOO_SHORT').sum()
        too_long = (validation_results == 'TOO_LONG').sum()
        invalid_length = (validation_results == 'INVALID_LENGTH').sum()
        blank_count = (validation_results == 'BLANK').sum()
        
        if add_validation_column:
            df[f'{column}_length_status'] = validation_results
        
        results = {
            'total_rows': len(df),
            'valid_length': int(valid_count),
            'too_short': int(too_short),
            'too_long': int(too_long),
            'invalid_exact_length': int(invalid_length),
            'blank_cells': int(blank_count),
            'min_length': min_length,
            'max_length': max_length,
            'exact_length': exact_length
        }
        
        return df, results
    
    # ===== 9. CHECK REQUIRED FIELDS =====
    @staticmethod
    def check_required_fields(df: pd.DataFrame, columns: List[str],
                              add_validation_column: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Check if required fields have values (not blank).
        """
        # Validate columns exist
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columns not found: {missing_columns}")
        
        def is_blank(value):
            if pd.isna(value):
                return True
            if isinstance(value, str) and value.strip() == '':
                return True
            return False
        
        # Check each required column
        column_stats = {}
        for col in columns:
            blank_count = df[col].apply(is_blank).sum()
            column_stats[col] = {
                'blank_count': int(blank_count),
                'filled_count': int(len(df) - blank_count),
                'completion_rate': round((len(df) - blank_count) / len(df) * 100, 2) if len(df) > 0 else 0
            }
        
        # Row-level check: all required fields filled?
        def check_row(row):
            for col in columns:
                if is_blank(row[col]):
                    return 'INCOMPLETE'
            return 'COMPLETE'
        
        if add_validation_column:
            df['required_fields_status'] = df.apply(check_row, axis=1)
        
        complete_rows = (df.apply(check_row, axis=1) == 'COMPLETE').sum()
        incomplete_rows = len(df) - complete_rows
        
        results = {
            'total_rows': len(df),
            'complete_rows': int(complete_rows),
            'incomplete_rows': int(incomplete_rows),
            'completion_rate': round(complete_rows / len(df) * 100, 2) if len(df) > 0 else 0,
            'columns_checked': columns,
            'column_stats': column_stats
        }
        
        return df, results
    
    # ===== 10. VALIDATE PAN/AADHAAR =====
    @staticmethod
    def validate_pan_aadhaar(df: pd.DataFrame, column: str,
                             id_type: str = 'PAN',
                             add_validation_column: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Validate Indian ID formats (PAN, Aadhaar, GST, etc.)
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        # ID patterns for India
        id_patterns = {
            'PAN': r'^[A-Z]{5}[0-9]{4}[A-Z]$',  # ABCDE1234F
            'AADHAAR': r'^\d{12}$',               # 12 digits
            'GST': r'^\d{2}[A-Z]{5}\d{4}[A-Z]\d[Z][A-Z\d]$',  # 22ABCDE1234F1Z5
            'PASSPORT': r'^[A-Z]\d{7}$',          # A1234567
            'VOTER_ID': r'^[A-Z]{3}\d{7}$',       # ABC1234567
            'IFSC': r'^[A-Z]{4}0[A-Z0-9]{6}$',    # SBIN0001234
        }
        
        pattern = id_patterns.get(id_type.upper())
        if not pattern:
            raise ValueError(f"Unknown ID type: {id_type}. Supported: {list(id_patterns.keys())}")
        
        def check_id(value):
            if pd.isna(value) or str(value).strip() == '':
                return 'BLANK'
            
            value_str = str(value).strip().upper().replace(' ', '').replace('-', '')
            
            if re.match(pattern, value_str):
                return 'VALID'
            return 'INVALID'
        
        validation_results = df[column].apply(check_id)
        
        valid_count = (validation_results == 'VALID').sum()
        invalid_count = (validation_results == 'INVALID').sum()
        blank_count = (validation_results == 'BLANK').sum()
        
        if add_validation_column:
            df[f'{column}_{id_type.lower()}_status'] = validation_results
        
        results = {
            'total_rows': len(df),
            'valid_ids': int(valid_count),
            'invalid_ids': int(invalid_count),
            'blank_cells': int(blank_count),
            'id_type': id_type,
            'validation_rate': round(valid_count / len(df) * 100, 2) if len(df) > 0 else 0
        }
        
        return df, results
    
    # ===== 11. HIGHLIGHT ERRORS =====
    @staticmethod
    def highlight_errors(df: pd.DataFrame, column: str,
                         error_type: str = 'any',
                         create_error_column: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Mark/highlight cells with errors based on type.
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in dataset")
        
        def detect_error(value):
            if pd.isna(value) or str(value).strip() == '':
                return 'ERROR_BLANK'
            
            value_str = str(value).strip()
            
            if error_type == 'blank':
                return 'OK'  # Not blank
            
            elif error_type == 'invalid_email':
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                if not re.match(email_pattern, value_str.lower()):
                    return 'ERROR_INVALID_EMAIL'
                return 'OK'
            
            elif error_type == 'invalid_number':
                try:
                    float(value_str.replace(',', ''))
                    return 'OK'
                except ValueError:
                    return 'ERROR_NOT_NUMBER'
            
            elif error_type == 'any':
                # Check for common error patterns
                if value_str.upper() in ['#N/A', '#VALUE!', '#REF!', '#DIV/0!', '#NAME?', '#NULL!', 'ERROR', 'N/A', 'NA', 'NULL', 'NAN']:
                    return 'ERROR_VALUE'
                
                return 'OK'
            
            return 'OK'
        
        error_results = df[column].apply(detect_error)
        
        error_count = (error_results != 'OK').sum()
        ok_count = (error_results == 'OK').sum()
        
        # Get error breakdown
        error_breakdown = error_results.value_counts().to_dict()
        
        if create_error_column:
            df[f'{column}_error_flag'] = error_results
        
        results = {
            'total_rows': len(df),
            'cells_with_errors': int(error_count),
            'cells_ok': int(ok_count),
            'error_percentage': round(error_count / len(df) * 100, 2) if len(df) > 0 else 0,
            'error_breakdown': {k: int(v) for k, v in error_breakdown.items()},
            'error_type_checked': error_type
        }
        
        return df, results
    
    # ===== 12. CREATE ERROR REPORT =====
    @staticmethod
    def create_error_report(df: pd.DataFrame, columns: List[str] = None,
                            output_column: str = 'error_summary') -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Create a comprehensive error report for specified columns.
        Adds a summary column listing all errors found in each row.
        """
        if columns is None:
            columns = df.columns.tolist()
        
        # Validate columns exist
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columns not found: {missing_columns}")
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        def analyze_row(row):
            errors = []
            
            for col in columns:
                value = row[col]
                
                # Check blank
                if pd.isna(value) or str(value).strip() == '':
                    errors.append(f"{col}: BLANK")
                    continue
                
                value_str = str(value).strip()
                
                # Check for error values
                if value_str.upper() in ['#N/A', '#VALUE!', '#REF!', '#DIV/0!', '#NAME?', '#NULL!', 'ERROR', 'N/A', 'NA', 'NULL', 'NAN']:
                    errors.append(f"{col}: ERROR_VALUE ({value_str})")
                
                # Check email columns
                if 'email' in col.lower():
                    if not re.match(email_pattern, value_str.lower()):
                        errors.append(f"{col}: INVALID_EMAIL")
                
                # Check phone columns
                if 'phone' in col.lower() or 'mobile' in col.lower():
                    cleaned = re.sub(r'[\s\-\(\)\+]', '', value_str)
                    if not cleaned.isdigit() or len(cleaned) < 10:
                        errors.append(f"{col}: INVALID_PHONE")
            
            return '; '.join(errors) if errors else 'NO_ERRORS'
        
        df[output_column] = df.apply(analyze_row, axis=1)
        
        # Count statistics
        rows_with_errors = (df[output_column] != 'NO_ERRORS').sum()
        rows_ok = len(df) - rows_with_errors
        
        # Collect all unique error types
        all_errors = []
        for summary in df[output_column]:
            if summary != 'NO_ERRORS':
                all_errors.extend([e.split(':')[1].strip().split(' ')[0] for e in summary.split(';')])
        
        error_type_counts = pd.Series(all_errors).value_counts().to_dict()
        
        results = {
            'total_rows': len(df),
            'rows_with_errors': int(rows_with_errors),
            'rows_ok': int(rows_ok),
            'error_rate': round(rows_with_errors / len(df) * 100, 2) if len(df) > 0 else 0,
            'columns_checked': columns,
            'error_type_counts': {k: int(v) for k, v in error_type_counts.items()},
            'output_column': output_column
        }
        
        return df, results