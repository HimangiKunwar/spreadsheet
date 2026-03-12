    def get_available_operations(self):
        """Return list of available operations for UI"""
        
        # Data Cleanup Operations
        cleanup_operations = [
            {'id': 'trim_whitespace', 'name': 'Trim Whitespace', 'description': 'Remove leading/trailing spaces', 'category': 'cleanup', 'requiresColumn': True, 'params': []},
            {'id': 'remove_duplicates', 'name': 'Remove Duplicates', 'description': 'Remove duplicate rows', 'category': 'cleanup', 'requiresColumn': False, 'params': []},
            {'id': 'fill_empty', 'name': 'Fill Empty Cells', 'description': 'Fill empty cells with a value', 'category': 'cleanup', 'requiresColumn': True, 'params': [{'name': 'fill_value', 'type': 'text', 'label': 'Fill Value', 'default': ''}]},
            {'id': 'remove_empty_rows', 'name': 'Remove Empty Rows', 'description': 'Delete rows that are completely empty', 'category': 'cleanup', 'requiresColumn': False, 'params': []},
            {'id': 'standardize_case', 'name': 'Standardize Case', 'description': 'Convert text to upper/lower/title case', 'category': 'cleanup', 'requiresColumn': True, 'params': [{'name': 'case_type', 'type': 'select', 'options': ['upper', 'lower', 'title'], 'default': 'lower'}]}
        ]
        
        # Validation Operations
        validation_operations = [
            {'id': 'validate_email', 'name': 'Validate Email', 'description': 'Check if emails are valid format', 'category': 'validation', 'requiresColumn': True, 'params': []},
            {'id': 'validate_phone', 'name': 'Validate Phone', 'description': 'Check if phone numbers are valid', 'category': 'validation', 'requiresColumn': True, 'params': []},
            {'id': 'validate_date', 'name': 'Validate Date', 'description': 'Check if dates are valid', 'category': 'validation', 'requiresColumn': True, 'params': []},
            {'id': 'validate_number', 'name': 'Validate Number', 'description': 'Check if values are valid numbers', 'category': 'validation', 'requiresColumn': True, 'params': []}
        ]
        
        # Text Transformation Operations
        text_operations = [
            {'id': 'uppercase', 'name': 'To Uppercase', 'description': 'Convert text to UPPERCASE', 'category': 'text', 'requiresColumn': True, 'params': []},
            {'id': 'lowercase', 'name': 'To Lowercase', 'description': 'Convert text to lowercase', 'category': 'text', 'requiresColumn': True, 'params': []},
            {'id': 'titlecase', 'name': 'To Title Case', 'description': 'Convert Text To Title Case', 'category': 'text', 'requiresColumn': True, 'params': []},
            {'id': 'find_replace', 'name': 'Find & Replace', 'description': 'Find and replace text', 'category': 'text', 'requiresColumn': True, 'params': [{'name': 'find', 'type': 'text', 'label': 'Find'}, {'name': 'replace', 'type': 'text', 'label': 'Replace With'}]},
            {'id': 'extract_text', 'name': 'Extract Text', 'description': 'Extract part of text', 'category': 'text', 'requiresColumn': True, 'params': [{'name': 'start', 'type': 'number', 'label': 'Start Position', 'default': 0}, {'name': 'length', 'type': 'number', 'label': 'Length', 'default': 10}]}
        ]
        
        # Date Operations
        date_operations = [
            {'id': 'extract_year', 'name': 'Extract Year', 'description': 'Extract year from date', 'category': 'date', 'requiresColumn': True, 'params': []},
            {'id': 'extract_month', 'name': 'Extract Month', 'description': 'Extract month from date', 'category': 'date', 'requiresColumn': True, 'params': []},
            {'id': 'extract_day', 'name': 'Extract Day', 'description': 'Extract day from date', 'category': 'date', 'requiresColumn': True, 'params': []},
            {'id': 'format_date', 'name': 'Format Date', 'description': 'Change date format', 'category': 'date', 'requiresColumn': True, 'params': [{'name': 'format', 'type': 'select', 'options': ['YYYY-MM-DD', 'DD/MM/YYYY', 'MM/DD/YYYY', 'DD-MMM-YYYY'], 'default': 'YYYY-MM-DD'}]}
        ]
        
        # Number Operations
        number_operations = [
            {'id': 'round_number', 'name': 'Round Number', 'description': 'Round to decimal places', 'category': 'number', 'requiresColumn': True, 'params': [{'name': 'decimals', 'type': 'number', 'label': 'Decimal Places', 'default': 2}]},
            {'id': 'format_currency', 'name': 'Format as Currency', 'description': 'Format number as currency', 'category': 'number', 'requiresColumn': True, 'params': [{'name': 'symbol', 'type': 'select', 'options': ['₹', '$', '€', '£'], 'default': '₹'}]},
            {'id': 'calculate_percentage', 'name': 'Calculate Percentage', 'description': 'Convert to percentage', 'category': 'number', 'requiresColumn': True, 'params': []}
        ]
        
        # Formatting Operations  
        formatting_operations = [
            {'id': 'change_font', 'name': 'Change Font', 'description': 'Change font style', 'category': 'formatting', 'requiresColumn': True, 'params': [{'name': 'font_name', 'type': 'select', 'options': ['Arial', 'Times New Roman', 'Calibri', 'Verdana'], 'default': 'Arial'}]},
            {'id': 'highlight_cells', 'name': 'Highlight Cells', 'description': 'Add background color', 'category': 'formatting', 'requiresColumn': True, 'params': [{'name': 'color', 'type': 'select', 'options': ['yellow', 'green', 'red', 'blue', 'orange'], 'default': 'yellow'}]}
        ]
        
        # Row/Column Operations
        row_column_operations = [
            {'id': 'insert_rows', 'name': 'Insert Rows', 'description': 'Insert blank rows', 'category': 'row_column', 'requiresColumn': False, 'params': [{'name': 'position', 'type': 'select', 'options': ['at_end', 'at_index', 'after_every_n'], 'default': 'at_end'}, {'name': 'count', 'type': 'number', 'label': 'Number of rows', 'default': 1}]},
            {'id': 'insert_columns', 'name': 'Insert Columns', 'description': 'Insert new columns', 'category': 'row_column', 'requiresColumn': False, 'params': [{'name': 'column_name', 'type': 'text', 'label': 'Column Name'}, {'name': 'position', 'type': 'select', 'options': ['at_end', 'before', 'after'], 'default': 'at_end'}]},
            {'id': 'delete_rows', 'name': 'Delete Rows', 'description': 'Delete rows by condition', 'category': 'row_column', 'requiresColumn': False, 'params': [{'name': 'condition', 'type': 'select', 'options': ['blank_rows', 'duplicate_rows', 'by_value'], 'default': 'blank_rows'}]},
            {'id': 'delete_columns', 'name': 'Delete Columns', 'description': 'Delete specified columns', 'category': 'row_column', 'requiresColumn': False, 'params': [{'name': 'condition', 'type': 'select', 'options': ['by_name', 'empty_columns'], 'default': 'by_name'}, {'name': 'columns', 'type': 'text', 'label': 'Column names (comma-separated)'}]},
            {'id': 'sort_rows', 'name': 'Sort Rows', 'description': 'Sort data by column', 'category': 'row_column', 'requiresColumn': True, 'params': [{'name': 'order', 'type': 'select', 'options': ['asc', 'desc'], 'default': 'asc'}]},
            {'id': 'filter_data', 'name': 'Filter Data', 'description': 'Filter rows by condition', 'category': 'row_column', 'requiresColumn': True, 'params': [{'name': 'operator', 'type': 'select', 'options': ['equals', 'contains', 'greater_than', 'less_than', 'not_empty'], 'default': 'equals'}, {'name': 'value', 'type': 'text', 'label': 'Value'}]},
            {'id': 'hide_rows', 'name': 'Hide Rows', 'description': 'Mark rows as hidden', 'category': 'row_column', 'requiresColumn': True, 'params': [{'name': 'condition', 'type': 'select', 'options': ['by_value', 'by_index'], 'default': 'by_value'}, {'name': 'value', 'type': 'text', 'label': 'Value to match'}]},
            {'id': 'hide_columns', 'name': 'Hide Columns', 'description': 'Mark columns as hidden', 'category': 'row_column', 'requiresColumn': False, 'params': [{'name': 'columns', 'type': 'text', 'label': 'Columns to hide (comma-separated)'}]},
            {'id': 'unhide_all', 'name': 'Unhide All', 'description': 'Show all hidden rows/columns', 'category': 'row_column', 'requiresColumn': False, 'params': [{'name': 'target', 'type': 'select', 'options': ['all', 'rows', 'columns'], 'default': 'all'}]},
            {'id': 'transpose', 'name': 'Transpose', 'description': 'Swap rows and columns', 'category': 'row_column', 'requiresColumn': False, 'params': []},
            {'id': 'group_rows', 'name': 'Group Rows', 'description': 'Group rows by column', 'category': 'row_column', 'requiresColumn': True, 'params': [{'name': 'add_subtotals', 'type': 'boolean', 'label': 'Add subtotals', 'default': False}]},
            {'id': 'freeze_panes', 'name': 'Freeze Panes', 'description': 'Freeze rows/columns', 'category': 'row_column', 'requiresColumn': False, 'params': [{'name': 'freeze_rows', 'type': 'number', 'label': 'Rows to freeze', 'default': 1}, {'name': 'freeze_columns', 'type': 'number', 'label': 'Columns to freeze', 'default': 0}]}
        ]
        
        # Combine all operations
        all_operations = []
        all_operations.extend(cleanup_operations)
        all_operations.extend(validation_operations)
        all_operations.extend(text_operations)
        all_operations.extend(date_operations)
        all_operations.extend(number_operations)
        all_operations.extend(formatting_operations)
        all_operations.extend(row_column_operations)
        
        return all_operations