import re
import statistics
from datetime import datetime
from collections import Counter
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

class DataCleaner:
    CLEANUP_OPERATIONS = {
        # String Operations
        'trim_whitespace': {
            'name': 'Trim Whitespace',
            'description': 'Remove leading/trailing whitespace from text columns',
            'category': 'string',
            'column_types': ['string', 'any']
        },
        'uppercase': {
            'name': 'Convert to Uppercase',
            'description': 'Convert text to uppercase',
            'category': 'string',
            'column_types': ['string']
        },
        'lowercase': {
            'name': 'Convert to Lowercase', 
            'description': 'Convert text to lowercase',
            'category': 'string',
            'column_types': ['string']
        },
        'title_case': {
            'name': 'Convert to Title Case',
            'description': 'Convert text to title case',
            'category': 'string',
            'column_types': ['string']
        },
        'remove_special_chars': {
            'name': 'Remove Special Characters',
            'description': 'Remove special characters from text',
            'category': 'string',
            'column_types': ['string']
        },
        'fix_inconsistent_casing': {
            'name': 'Fix Inconsistent Casing',
            'description': 'Standardize text casing for consistency',
            'category': 'string',
            'column_types': ['string']
        },
        'remove_leading_zeros': {
            'name': 'Remove Leading Zeros',
            'description': 'Remove leading zeros from numeric strings',
            'category': 'string',
            'column_types': ['string']
        },
        
        # Null/Empty Handling
        'fill_empty_with_value': {
            'name': 'Fill Empty with Value',
            'description': 'Fill empty cells with a specified value',
            'category': 'null_handling',
            'column_types': ['any']
        },
        'fill_empty_with_mean': {
            'name': 'Fill Empty with Mean',
            'description': 'Fill empty numeric cells with column mean',
            'category': 'null_handling',
            'column_types': ['integer', 'float']
        },
        'fill_empty_with_median': {
            'name': 'Fill Empty with Median',
            'description': 'Fill empty numeric cells with column median',
            'category': 'null_handling',
            'column_types': ['integer', 'float']
        },
        'fill_empty_with_mode': {
            'name': 'Fill Empty with Mode',
            'description': 'Fill empty cells with most common value',
            'category': 'null_handling',
            'column_types': ['any']
        },
        'remove_empty_rows': {
            'name': 'Remove Empty Rows',
            'description': 'Remove rows where selected columns are empty',
            'category': 'null_handling',
            'column_types': ['any']
        },
        
        # Numeric Operations
        'round_numbers': {
            'name': 'Round Numbers',
            'description': 'Round numeric values to specified decimal places',
            'category': 'numeric',
            'column_types': ['float']
        },
        'remove_outliers': {
            'name': 'Remove Outliers',
            'description': 'Remove statistical outliers (beyond 3 std dev)',
            'category': 'numeric',
            'column_types': ['integer', 'float']
        },
        'normalize_numbers': {
            'name': 'Normalize Numbers',
            'description': 'Normalize numeric columns to 0-1 range',
            'category': 'numeric',
            'column_types': ['integer', 'float']
        },
        
        # Date Operations
        'standardize_dates': {
            'name': 'Standardize Dates',
            'description': 'Convert dates to consistent format (YYYY-MM-DD)',
            'category': 'date',
            'column_types': ['date', 'datetime']
        },
        'extract_year': {
            'name': 'Extract Year',
            'description': 'Extract year from date column into new column',
            'category': 'date',
            'column_types': ['date', 'datetime']
        },
        'extract_month': {
            'name': 'Extract Month',
            'description': 'Extract month from date column into new column',
            'category': 'date',
            'column_types': ['date', 'datetime']
        },
        
        # Data Quality
        'remove_duplicates': {
            'name': 'Remove Duplicates',
            'description': 'Remove duplicate rows based on selected columns',
            'category': 'data_quality',
            'column_types': ['any']
        },
        'split_column': {
            'name': 'Split Column',
            'description': 'Split column by delimiter into multiple columns',
            'category': 'data_quality',
            'column_types': ['string']
        },
        'merge_columns': {
            'name': 'Merge Columns',
            'description': 'Merge multiple columns into one',
            'category': 'data_quality',
            'column_types': ['any']
        }
    }

    @staticmethod
    def get_operations():
        return DataCleaner.CLEANUP_OPERATIONS

    @staticmethod
    def preview_cleanup(data, operation, columns=None, options=None):
        """Preview changes without applying them"""
        if operation not in DataCleaner.CLEANUP_OPERATIONS:
            raise ValueError(f"Unknown operation: {operation}")
        
        # Create a copy for preview
        preview_data = [row.copy() for row in data[:100]]  # Limit to first 100 rows for preview
        
        # Apply operation to preview data
        result_data, affected_count = DataCleaner._apply_operation(preview_data, operation, columns, options)
        
        # Find sample changes
        sample_changes = []
        for i, (original, modified) in enumerate(zip(data[:100], result_data)):
            for col in (columns or original.keys()):
                if col in original and col in modified and original[col] != modified[col]:
                    sample_changes.append({
                        'row_index': i,
                        'column': col,
                        'before': original[col],
                        'after': modified[col]
                    })
                    if len(sample_changes) >= 10:
                        break
            if len(sample_changes) >= 10:
                break
        
        # Estimate total affected rows
        total_affected = int(affected_count * len(data) / min(len(data), 100))
        
        return {
            'affected_rows': total_affected,
            'sample_changes': sample_changes,
            'warnings': []
        }

    @staticmethod
    def apply_cleanup(data, operation, columns=None, options=None):
        """Apply cleanup operation to data"""
        if operation not in DataCleaner.CLEANUP_OPERATIONS:
            raise ValueError(f"Unknown operation: {operation}")
        
        result_data, affected_count = DataCleaner._apply_operation(data, operation, columns, options)
        return result_data, affected_count

    @staticmethod
    def _apply_operation(data, operation, columns=None, options=None):
        """Internal method to apply operations"""
        options = options or {}
        affected_count = 0
        
        if operation == 'trim_whitespace':
            return DataCleaner._trim_whitespace(data, columns)
        elif operation == 'uppercase':
            return DataCleaner._uppercase(data, columns)
        elif operation == 'lowercase':
            return DataCleaner._lowercase(data, columns)
        elif operation == 'title_case':
            return DataCleaner._title_case(data, columns)
        elif operation == 'remove_special_chars':
            return DataCleaner._remove_special_chars(data, columns, options)
        elif operation == 'fix_inconsistent_casing':
            return DataCleaner._fix_inconsistent_casing(data, columns)
        elif operation == 'remove_leading_zeros':
            return DataCleaner._remove_leading_zeros(data, columns)
        elif operation == 'fill_empty_with_value':
            return DataCleaner._fill_empty_with_value(data, columns, options)
        elif operation == 'fill_empty_with_mean':
            return DataCleaner._fill_empty_with_mean(data, columns)
        elif operation == 'fill_empty_with_median':
            return DataCleaner._fill_empty_with_median(data, columns)
        elif operation == 'fill_empty_with_mode':
            return DataCleaner._fill_empty_with_mode(data, columns)
        elif operation == 'remove_empty_rows':
            return DataCleaner._remove_empty_rows(data, columns)
        elif operation == 'round_numbers':
            return DataCleaner._round_numbers(data, columns, options)
        elif operation == 'remove_outliers':
            return DataCleaner._remove_outliers(data, columns)
        elif operation == 'normalize_numbers':
            return DataCleaner._normalize_numbers(data, columns)
        elif operation == 'standardize_dates':
            return DataCleaner._standardize_dates(data, columns, options)
        elif operation == 'extract_year':
            return DataCleaner._extract_year(data, columns)
        elif operation == 'extract_month':
            return DataCleaner._extract_month(data, columns)
        elif operation == 'remove_duplicates':
            return DataCleaner._remove_duplicates(data, columns, options)
        elif operation == 'split_column':
            return DataCleaner._split_column(data, columns, options)
        elif operation == 'merge_columns':
            return DataCleaner._merge_columns(data, columns, options)
        else:
            raise ValueError(f"Unknown operation: {operation}")

    @staticmethod
    def _trim_whitespace(data, columns):
        affected_count = 0
        for row in data:
            for col in (columns or row.keys()):
                if col in row and row[col] is not None:
                    original = str(row[col])
                    trimmed = original.strip()
                    if original != trimmed:
                        row[col] = trimmed
                        affected_count += 1
        return data, affected_count

    @staticmethod
    def _uppercase(data, columns):
        affected_count = 0
        for row in data:
            for col in (columns or row.keys()):
                if col in row and row[col] is not None:
                    original = str(row[col])
                    upper = original.upper()
                    if original != upper:
                        row[col] = upper
                        affected_count += 1
        return data, affected_count

    @staticmethod
    def _lowercase(data, columns):
        affected_count = 0
        for row in data:
            for col in (columns or row.keys()):
                if col in row and row[col] is not None:
                    original = str(row[col])
                    lower = original.lower()
                    if original != lower:
                        row[col] = lower
                        affected_count += 1
        return data, affected_count

    @staticmethod
    def _title_case(data, columns):
        affected_count = 0
        for row in data:
            for col in (columns or row.keys()):
                if col in row and row[col] is not None:
                    original = str(row[col])
                    title = original.title()
                    if original != title:
                        row[col] = title
                        affected_count += 1
        return data, affected_count

    @staticmethod
    def _remove_special_chars(data, columns, options):
        keep_spaces = options.get('keep_spaces', True)
        pattern = r'[^a-zA-Z0-9\s]' if keep_spaces else r'[^a-zA-Z0-9]'
        affected_count = 0
        
        for row in data:
            for col in (columns or row.keys()):
                if col in row and row[col] is not None:
                    original = str(row[col])
                    cleaned = re.sub(pattern, '', original)
                    if original != cleaned:
                        row[col] = cleaned
                        affected_count += 1
        return data, affected_count

    @staticmethod
    def _fix_inconsistent_casing(data, columns):
        affected_count = 0
        for col in (columns or []):
            # Find most common casing for each unique value
            value_counts = {}
            for row in data:
                if col in row and row[col] is not None:
                    val = str(row[col]).lower()
                    if val not in value_counts:
                        value_counts[val] = {}
                    original = str(row[col])
                    value_counts[val][original] = value_counts[val].get(original, 0) + 1
            
            # Create mapping to most common casing
            casing_map = {}
            for val, casings in value_counts.items():
                most_common = max(casings.items(), key=lambda x: x[1])[0]
                for casing in casings:
                    if casing != most_common:
                        casing_map[casing] = most_common
            
            # Apply mapping
            for row in data:
                if col in row and row[col] is not None:
                    original = str(row[col])
                    if original in casing_map:
                        row[col] = casing_map[original]
                        affected_count += 1
        
        return data, affected_count

    @staticmethod
    def _remove_leading_zeros(data, columns):
        affected_count = 0
        for row in data:
            for col in (columns or row.keys()):
                if col in row and row[col] is not None:
                    original = str(row[col])
                    cleaned = original.lstrip('0') or '0'
                    if original != cleaned:
                        row[col] = cleaned
                        affected_count += 1
        return data, affected_count

    @staticmethod
    def _fill_empty_with_value(data, columns, options):
        fill_value = options.get('value', '')
        affected_count = 0
        
        for row in data:
            for col in (columns or row.keys()):
                if col in row and (row[col] is None or str(row[col]).strip() == ''):
                    row[col] = fill_value
                    affected_count += 1
        return data, affected_count

    @staticmethod
    def _fill_empty_with_mean(data, columns):
        affected_count = 0
        
        for col in (columns or []):
            # Calculate mean
            values = []
            for row in data:
                if col in row and row[col] is not None:
                    try:
                        values.append(float(row[col]))
                    except (ValueError, TypeError):
                        pass
            
            if values:
                mean_val = statistics.mean(values)
                
                # Fill empty values
                for row in data:
                    if col in row and (row[col] is None or str(row[col]).strip() == ''):
                        row[col] = mean_val
                        affected_count += 1
        
        return data, affected_count

    @staticmethod
    def _fill_empty_with_median(data, columns):
        affected_count = 0
        
        for col in (columns or []):
            # Calculate median
            values = []
            for row in data:
                if col in row and row[col] is not None:
                    try:
                        values.append(float(row[col]))
                    except (ValueError, TypeError):
                        pass
            
            if values:
                median_val = statistics.median(values)
                
                # Fill empty values
                for row in data:
                    if col in row and (row[col] is None or str(row[col]).strip() == ''):
                        row[col] = median_val
                        affected_count += 1
        
        return data, affected_count

    @staticmethod
    def _fill_empty_with_mode(data, columns):
        affected_count = 0
        
        for col in (columns or []):
            # Calculate mode
            values = []
            for row in data:
                if col in row and row[col] is not None and str(row[col]).strip() != '':
                    values.append(row[col])
            
            if values:
                mode_val = Counter(values).most_common(1)[0][0]
                
                # Fill empty values
                for row in data:
                    if col in row and (row[col] is None or str(row[col]).strip() == ''):
                        row[col] = mode_val
                        affected_count += 1
        
        return data, affected_count

    @staticmethod
    def _remove_empty_rows(data, columns):
        original_count = len(data)
        
        filtered_data = []
        for row in data:
            has_value = False
            for col in (columns or row.keys()):
                if col in row and row[col] is not None and str(row[col]).strip() != '':
                    has_value = True
                    break
            if has_value:
                filtered_data.append(row)
        
        affected_count = original_count - len(filtered_data)
        return filtered_data, affected_count

    @staticmethod
    def _round_numbers(data, columns, options):
        decimals = options.get('decimals', 2)
        affected_count = 0
        
        for row in data:
            for col in (columns or row.keys()):
                if col in row and row[col] is not None:
                    try:
                        original = float(row[col])
                        rounded = round(original, decimals)
                        if original != rounded:
                            row[col] = rounded
                            affected_count += 1
                    except (ValueError, TypeError):
                        pass
        return data, affected_count

    @staticmethod
    def _remove_outliers(data, columns):
        affected_count = 0
        
        for col in (columns or []):
            # Calculate mean and std
            values = []
            for row in data:
                if col in row and row[col] is not None:
                    try:
                        values.append(float(row[col]))
                    except (ValueError, TypeError):
                        pass
            
            if len(values) > 1:
                mean_val = statistics.mean(values)
                std_val = statistics.stdev(values)
                threshold = 3 * std_val
                
                # Remove outliers
                filtered_data = []
                for row in data:
                    if col in row and row[col] is not None:
                        try:
                            val = float(row[col])
                            if abs(val - mean_val) <= threshold:
                                filtered_data.append(row)
                            else:
                                affected_count += 1
                        except (ValueError, TypeError):
                            filtered_data.append(row)
                    else:
                        filtered_data.append(row)
                data[:] = filtered_data
        
        return data, affected_count

    @staticmethod
    def _normalize_numbers(data, columns):
        affected_count = 0
        
        for col in (columns or []):
            # Find min and max
            values = []
            for row in data:
                if col in row and row[col] is not None:
                    try:
                        values.append(float(row[col]))
                    except (ValueError, TypeError):
                        pass
            
            if values:
                min_val = min(values)
                max_val = max(values)
                range_val = max_val - min_val
                
                if range_val > 0:
                    # Normalize values
                    for row in data:
                        if col in row and row[col] is not None:
                            try:
                                original = float(row[col])
                                normalized = (original - min_val) / range_val
                                row[col] = normalized
                                affected_count += 1
                            except (ValueError, TypeError):
                                pass
        
        return data, affected_count

    @staticmethod
    def _standardize_dates(data, columns, options):
        format_str = options.get('date_format', '%Y-%m-%d')
        affected_count = 0
        
        for row in data:
            for col in (columns or row.keys()):
                if col in row and row[col] is not None:
                    try:
                        if HAS_PANDAS:
                            dt = pd.to_datetime(str(row[col]))
                            formatted = dt.strftime(format_str)
                        else:
                            # Try common date formats
                            date_str = str(row[col])
                            dt = None
                            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                                try:
                                    dt = datetime.strptime(date_str, fmt)
                                    break
                                except ValueError:
                                    continue
                            if dt:
                                formatted = dt.strftime(format_str)
                            else:
                                continue
                        
                        if str(row[col]) != formatted:
                            row[col] = formatted
                            affected_count += 1
                    except:
                        pass
        return data, affected_count

    @staticmethod
    def _extract_year(data, columns):
        affected_count = 0
        
        for row in data:
            for col in (columns or []):
                if col in row and row[col] is not None:
                    try:
                        if HAS_PANDAS:
                            dt = pd.to_datetime(str(row[col]))
                            year = dt.year
                        else:
                            date_str = str(row[col])
                            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                                try:
                                    dt = datetime.strptime(date_str, fmt)
                                    year = dt.year
                                    break
                                except ValueError:
                                    continue
                            else:
                                continue
                        
                        new_col = f"{col}_year"
                        row[new_col] = year
                        affected_count += 1
                    except:
                        pass
        return data, affected_count

    @staticmethod
    def _extract_month(data, columns):
        affected_count = 0
        
        for row in data:
            for col in (columns or []):
                if col in row and row[col] is not None:
                    try:
                        if HAS_PANDAS:
                            dt = pd.to_datetime(str(row[col]))
                            month = dt.month
                        else:
                            date_str = str(row[col])
                            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                                try:
                                    dt = datetime.strptime(date_str, fmt)
                                    month = dt.month
                                    break
                                except ValueError:
                                    continue
                            else:
                                continue
                        
                        new_col = f"{col}_month"
                        row[new_col] = month
                        affected_count += 1
                    except:
                        pass
        return data, affected_count



    @staticmethod
    def _remove_duplicates(data, columns, options):
        key_columns = columns or list(data[0].keys()) if data else []
        keep = options.get('keep', 'first')  # first, last, none
        
        seen = set()
        result = []
        original_count = len(data)
        
        for i, row in enumerate(data):
            key = tuple(row.get(col) for col in key_columns)
            
            if key not in seen:
                seen.add(key)
                result.append(row)
            elif keep == 'last':
                # Remove previous occurrence
                result = [r for r in result if tuple(r.get(col) for col in key_columns) != key]
                result.append(row)
        
        affected_count = original_count - len(result)
        return result, affected_count

    @staticmethod
    def _split_column(data, columns, options):
        delimiter = options.get('delimiter', ',')
        affected_count = 0
        
        for col in (columns or []):
            for row in data:
                if col in row and row[col] is not None:
                    parts = str(row[col]).split(delimiter)
                    for i, part in enumerate(parts):
                        new_col = f"{col}_part_{i+1}"
                        row[new_col] = part.strip()
                    affected_count += 1
        
        return data, affected_count

    @staticmethod
    def _merge_columns(data, columns, options):
        separator = options.get('separator', ' ')
        new_column_name = options.get('new_column', 'merged_column')
        affected_count = 0
        
        if columns and len(columns) > 1:
            for row in data:
                values = []
                for col in columns:
                    if col in row and row[col] is not None:
                        values.append(str(row[col]))
                
                if values:
                    row[new_column_name] = separator.join(values)
                    affected_count += 1
        
        return data, affected_count









