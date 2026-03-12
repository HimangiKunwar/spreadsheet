# Sheet Operations for SmartSheet Pro
# This file contains the 12 new sheet operations to be added to services.py

sheet_operations = [
    {
        'id': 'add_sheet',
        'name': 'Add New Sheet',
        'description': 'Create a new worksheet in the workbook',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'sheet_name', 'type': 'text', 'label': 'New Sheet Name', 'required': True, 'default': 'New_Sheet'},
            {'name': 'position', 'type': 'select', 'label': 'Position', 'options': ['end', 'start', 'after_current'], 'default': 'end'},
            {'name': 'template_type', 'type': 'select', 'label': 'Template', 'options': ['blank', 'copy_current_structure', 'with_headers'], 'default': 'blank'}
        ]
    },
    {
        'id': 'delete_sheet',
        'name': 'Delete Sheet',
        'description': 'Remove a worksheet from the workbook',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'sheet_name', 'type': 'text', 'label': 'Sheet Name to Delete', 'required': True},
            {'name': 'confirm_delete', 'type': 'boolean', 'label': 'Confirm Deletion', 'default': True}
        ]
    },
    {
        'id': 'rename_sheet',
        'name': 'Rename Sheet',
        'description': 'Change the name of a worksheet',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'old_name', 'type': 'text', 'label': 'Current Sheet Name', 'required': True},
            {'name': 'new_name', 'type': 'text', 'label': 'New Sheet Name', 'required': True}
        ]
    },
    {
        'id': 'copy_sheet',
        'name': 'Copy Sheet',
        'description': 'Create a duplicate of a worksheet',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'source_sheet', 'type': 'text', 'label': 'Source Sheet Name', 'required': True},
            {'name': 'new_name', 'type': 'text', 'label': 'Copy Name', 'required': True, 'default': 'Sheet_Copy'},
            {'name': 'position', 'type': 'select', 'label': 'Position', 'options': ['after_source', 'end', 'start'], 'default': 'after_source'}
        ]
    },
    {
        'id': 'move_sheet',
        'name': 'Move Sheet',
        'description': 'Change the position/order of a worksheet',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'sheet_name', 'type': 'text', 'label': 'Sheet Name to Move', 'required': True},
            {'name': 'new_position', 'type': 'select', 'label': 'Move To', 'options': ['first', 'last', 'left', 'right'], 'default': 'first'},
            {'name': 'position_index', 'type': 'number', 'label': 'Specific Position (0-based)', 'required': False, 'default': 0}
        ]
    },
    {
        'id': 'hide_sheet',
        'name': 'Hide Sheet',
        'description': 'Hide a worksheet from view',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'sheet_name', 'type': 'text', 'label': 'Sheet Name to Hide', 'required': True},
            {'name': 'hide_type', 'type': 'select', 'label': 'Hide Type', 'options': ['hidden', 'very_hidden'], 'default': 'hidden'},
            {'name': 'unhide', 'type': 'boolean', 'label': 'Unhide Instead', 'default': False}
        ]
    },
    {
        'id': 'protect_sheet',
        'name': 'Protect Sheet',
        'description': 'Lock a worksheet to prevent editing',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'sheet_name', 'type': 'text', 'label': 'Sheet Name to Protect', 'required': True},
            {'name': 'password', 'type': 'text', 'label': 'Password (optional)', 'required': False, 'default': ''},
            {'name': 'allow_select_cells', 'type': 'boolean', 'label': 'Allow Selecting Cells', 'default': True},
            {'name': 'allow_format_cells', 'type': 'boolean', 'label': 'Allow Formatting Cells', 'default': False},
            {'name': 'unprotect', 'type': 'boolean', 'label': 'Unprotect Instead', 'default': False}
        ]
    },
    {
        'id': 'compare_sheets',
        'name': 'Compare Sheets',
        'description': 'Find differences between two worksheets',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'sheet1_name', 'type': 'text', 'label': 'First Sheet Name', 'required': True},
            {'name': 'sheet2_name', 'type': 'text', 'label': 'Second Sheet Name', 'required': True},
            {'name': 'compare_by', 'type': 'select', 'label': 'Compare By', 'options': ['cell_by_cell', 'row_by_row', 'key_column'], 'default': 'cell_by_cell'},
            {'name': 'key_column', 'type': 'text', 'label': 'Key Column (for key_column compare)', 'required': False},
            {'name': 'output_sheet', 'type': 'text', 'label': 'Output Differences To', 'default': 'Comparison_Results'}
        ]
    },
    {
        'id': 'merge_sheets',
        'name': 'Merge Sheets',
        'description': 'Combine multiple worksheets into one',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'sheet_names', 'type': 'text', 'label': 'Sheet Names (comma-separated)', 'required': True, 'placeholder': 'Sheet1, Sheet2, Sheet3'},
            {'name': 'target_sheet', 'type': 'text', 'label': 'Target Sheet Name', 'default': 'Merged_Data'},
            {'name': 'merge_type', 'type': 'select', 'label': 'Merge Type', 'options': ['append_rows', 'side_by_side', 'union'], 'default': 'append_rows'},
            {'name': 'include_headers', 'type': 'boolean', 'label': 'Include Headers from Each Sheet', 'default': False},
            {'name': 'add_source_column', 'type': 'boolean', 'label': 'Add Source Sheet Column', 'default': True}
        ]
    },
    {
        'id': 'create_index',
        'name': 'Create Index',
        'description': 'Create an index sheet listing all worksheets',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'index_sheet_name', 'type': 'text', 'label': 'Index Sheet Name', 'default': 'Index'},
            {'name': 'include_row_count', 'type': 'boolean', 'label': 'Include Row Count', 'default': True},
            {'name': 'include_column_count', 'type': 'boolean', 'label': 'Include Column Count', 'default': True},
            {'name': 'include_description', 'type': 'boolean', 'label': 'Include Description Column', 'default': False},
            {'name': 'position', 'type': 'select', 'label': 'Index Position', 'options': ['first', 'last'], 'default': 'first'}
        ]
    },
    {
        'id': 'link_sheets',
        'name': 'Link Sheets',
        'description': 'Create data references between worksheets',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'source_sheet', 'type': 'text', 'label': 'Source Sheet Name', 'required': True},
            {'name': 'source_range', 'type': 'text', 'label': 'Source Range (e.g., A1:D10)', 'required': True, 'default': 'A1:A10'},
            {'name': 'target_sheet', 'type': 'text', 'label': 'Target Sheet Name', 'required': True},
            {'name': 'target_start_cell', 'type': 'text', 'label': 'Target Start Cell', 'default': 'A1'},
            {'name': 'link_type', 'type': 'select', 'label': 'Link Type', 'options': ['formula_reference', 'copy_values', 'mirror'], 'default': 'formula_reference'}
        ]
    },
    {
        'id': 'copy_to_file',
        'name': 'Copy to Other File',
        'description': 'Copy a sheet to another dataset/workbook',
        'category': 'sheet_operations',
        'requiresColumn': False,
        'params': [
            {'name': 'sheet_name', 'type': 'text', 'label': 'Sheet Name to Copy', 'required': True},
            {'name': 'target_dataset_id', 'type': 'text', 'label': 'Target Dataset ID', 'required': True},
            {'name': 'new_sheet_name', 'type': 'text', 'label': 'New Sheet Name in Target', 'required': False},
            {'name': 'copy_formatting', 'type': 'boolean', 'label': 'Copy Formatting', 'default': True},
            {'name': 'copy_formulas', 'type': 'boolean', 'label': 'Copy Formulas (or values only)', 'default': False}
        ]
    }
]

# Sheet operation handlers for _apply_operation method
sheet_operation_handlers = """
        # ===== SHEET OPERATIONS =====
        elif operation_id == 'add_sheet':
            sheet_name = params.get('sheet_name', 'New_Sheet')
            position = params.get('position', 'end')
            template_type = params.get('template_type', 'blank')
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            new_sheet_data = pd.DataFrame()
            if template_type == 'copy_current_structure':
                new_sheet_data = pd.DataFrame(columns=df.columns)
            elif template_type == 'with_headers':
                new_sheet_data = pd.DataFrame(columns=df.columns)
            
            df.attrs['add_sheet'] = {
                'name': sheet_name,
                'position': position,
                'data': new_sheet_data.to_dict() if not new_sheet_data.empty else {}
            }
            message = f"New sheet '{sheet_name}' will be created at {position}"

        elif operation_id == 'delete_sheet':
            sheet_name = params.get('sheet_name', '')
            confirm_delete = params.get('confirm_delete', True)
            
            if not sheet_name:
                message = "No sheet name specified"
            elif not confirm_delete:
                message = "Deletion not confirmed - operation skipped"
            else:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['delete_sheet'] = {'name': sheet_name}
                message = f"Sheet '{sheet_name}' marked for deletion"

        elif operation_id == 'rename_sheet':
            old_name = params.get('old_name', '')
            new_name = params.get('new_name', '')
            
            if old_name and new_name:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['rename_sheet'] = {'old_name': old_name, 'new_name': new_name}
                message = f"Sheet '{old_name}' will be renamed to '{new_name}'"
            else:
                message = "Both old and new names are required"

        elif operation_id == 'copy_sheet':
            source_sheet = params.get('source_sheet', '')
            new_name = params.get('new_name', 'Sheet_Copy')
            position = params.get('position', 'after_source')
            
            if source_sheet:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['copy_sheet'] = {
                    'source': source_sheet,
                    'new_name': new_name,
                    'position': position
                }
                message = f"Sheet '{source_sheet}' will be copied as '{new_name}'"
            else:
                message = "Source sheet name is required"

        elif operation_id == 'move_sheet':
            sheet_name = params.get('sheet_name', '')
            new_position = params.get('new_position', 'first')
            position_index = int(params.get('position_index', 0))
            
            if sheet_name:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['move_sheet'] = {
                    'name': sheet_name,
                    'position': new_position,
                    'index': position_index
                }
                message = f"Sheet '{sheet_name}' will be moved to {new_position}"
            else:
                message = "Sheet name is required"

        elif operation_id == 'hide_sheet':
            sheet_name = params.get('sheet_name', '')
            hide_type = params.get('hide_type', 'hidden')
            unhide = params.get('unhide', False)
            
            if sheet_name:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['hide_sheet'] = {
                    'name': sheet_name,
                    'hide_type': hide_type,
                    'unhide': unhide
                }
                action = 'unhidden' if unhide else f'hidden ({hide_type})'
                message = f"Sheet '{sheet_name}' will be {action}"
            else:
                message = "Sheet name is required"

        elif operation_id == 'protect_sheet':
            sheet_name = params.get('sheet_name', '')
            password = params.get('password', '')
            allow_select = params.get('allow_select_cells', True)
            allow_format = params.get('allow_format_cells', False)
            unprotect = params.get('unprotect', False)
            
            if sheet_name:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['protect_sheet'] = {
                    'name': sheet_name,
                    'password': password,
                    'allow_select': allow_select,
                    'allow_format': allow_format,
                    'unprotect': unprotect
                }
                action = 'unprotected' if unprotect else 'protected'
                message = f"Sheet '{sheet_name}' will be {action}"
            else:
                message = "Sheet name is required"

        elif operation_id == 'compare_sheets':
            sheet1_name = params.get('sheet1_name', '')
            sheet2_name = params.get('sheet2_name', '')
            compare_by = params.get('compare_by', 'cell_by_cell')
            key_column = params.get('key_column', '')
            output_sheet = params.get('output_sheet', 'Comparison_Results')
            
            if sheet1_name and sheet2_name:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['compare_sheets'] = {
                    'sheet1': sheet1_name,
                    'sheet2': sheet2_name,
                    'compare_by': compare_by,
                    'key_column': key_column,
                    'output_sheet': output_sheet
                }
                message = f"Comparison between '{sheet1_name}' and '{sheet2_name}' configured (results to '{output_sheet}')"
            else:
                message = "Both sheet names are required for comparison"

        elif operation_id == 'merge_sheets':
            sheet_names_str = params.get('sheet_names', '')
            target_sheet = params.get('target_sheet', 'Merged_Data')
            merge_type = params.get('merge_type', 'append_rows')
            include_headers = params.get('include_headers', False)
            add_source_column = params.get('add_source_column', True)
            
            sheet_names = [s.strip() for s in sheet_names_str.split(',') if s.strip()]
            
            if sheet_names:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['merge_sheets'] = {
                    'sheets': sheet_names,
                    'target': target_sheet,
                    'merge_type': merge_type,
                    'include_headers': include_headers,
                    'add_source_column': add_source_column
                }
                message = f"Will merge {len(sheet_names)} sheets into '{target_sheet}' ({merge_type})"
            else:
                message = "At least one sheet name is required"

        elif operation_id == 'create_index':
            index_sheet_name = params.get('index_sheet_name', 'Index')
            include_row_count = params.get('include_row_count', True)
            include_column_count = params.get('include_column_count', True)
            include_description = params.get('include_description', False)
            position = params.get('position', 'first')
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            index_data = {
                'Sheet Name': ['Current_Sheet'],
                'Rows': [len(df)],
                'Columns': [len(df.columns)]
            }
            
            df.attrs['create_index'] = {
                'name': index_sheet_name,
                'include_row_count': include_row_count,
                'include_column_count': include_column_count,
                'include_description': include_description,
                'position': position,
                'index_data': index_data
            }
            message = f"Index sheet '{index_sheet_name}' will be created at {position}"

        elif operation_id == 'link_sheets':
            source_sheet = params.get('source_sheet', '')
            source_range = params.get('source_range', 'A1:A10')
            target_sheet = params.get('target_sheet', '')
            target_start_cell = params.get('target_start_cell', 'A1')
            link_type = params.get('link_type', 'formula_reference')
            
            if source_sheet and target_sheet:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['link_sheets'] = {
                    'source_sheet': source_sheet,
                    'source_range': source_range,
                    'target_sheet': target_sheet,
                    'target_cell': target_start_cell,
                    'link_type': link_type
                }
                message = f"Link created: {source_sheet}!{source_range} → {target_sheet}!{target_start_cell} ({link_type})"
            else:
                message = "Source and target sheet names are required"

        elif operation_id == 'copy_to_file':
            sheet_name = params.get('sheet_name', '')
            target_dataset_id = params.get('target_dataset_id', '')
            new_sheet_name = params.get('new_sheet_name', '') or sheet_name
            copy_formatting = params.get('copy_formatting', True)
            copy_formulas = params.get('copy_formulas', False)
            
            if sheet_name and target_dataset_id:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['copy_to_file'] = {
                    'sheet_name': sheet_name,
                    'target_dataset_id': target_dataset_id,
                    'new_sheet_name': new_sheet_name,
                    'copy_formatting': copy_formatting,
                    'copy_formulas': copy_formulas
                }
                message = f"Sheet '{sheet_name}' will be copied to dataset {target_dataset_id} as '{new_sheet_name}'"
            else:
                message = "Sheet name and target dataset ID are required"
"""