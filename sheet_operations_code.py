        # ============================================
        # CATEGORY 9: SHEET OPERATIONS (12)
        # ============================================
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
        
        # Combine all operations
        all_operations.extend(cleanup_operations)
        all_operations.extend(sheet_operations)
        
        return all_operations