class RuleEngine:
    OPERATORS = {
        'equals': lambda a, b: str(a).strip() == str(b).strip(),
        'not_equals': lambda a, b: str(a).strip() != str(b).strip(),
        'contains': lambda a, b: str(b).strip().lower() in str(a).strip().lower(),
        'not_contains': lambda a, b: str(b).strip().lower() not in str(a).strip().lower(),
        'starts_with': lambda a, b: str(a).strip().lower().startswith(str(b).strip().lower()),
        'ends_with': lambda a, b: str(a).strip().lower().endswith(str(b).strip().lower()),
        'greater_than': lambda a, b: float(a) > float(b),
        'less_than': lambda a, b: float(a) < float(b),
        'greater_than_or_equal': lambda a, b: float(a) >= float(b),
        'less_than_or_equal': lambda a, b: float(a) <= float(b),
        'is_empty': lambda a, b: RuleEngine._is_empty(a),
        'is_not_empty': lambda a, b: not RuleEngine._is_empty(a),
        'between': lambda a, b: RuleEngine._between(a, b),
        'in_list': lambda a, b: str(a).strip().lower() in [x.strip().lower() for x in str(b).split(',')],
    }

    @staticmethod
    def _is_empty(value):
        """Check if a value is considered empty"""
        # None values
        if value is None:
            return True
        
        # String representations of null
        if isinstance(value, str):
            stripped = value.strip().lower()
            if stripped in ['', 'nan', 'none', 'null', 'n/a', 'na']:
                return True
        
        # Check for NaN (float)
        try:
            import math
            if isinstance(value, float) and math.isnan(value):
                return True
        except (TypeError, ValueError):
            pass
        
        # Check pandas NaN if available
        try:
            import pandas as pd
            if pd.isna(value):
                return True
        except (ImportError, TypeError):
            pass
        
        return False

    @staticmethod
    def _between(value, range_str):
        try:
            low, high = map(float, range_str.split(','))
            return low <= float(value) <= high
        except:
            return False

    @staticmethod
    def preview_affected_rows(data, rule_config):
        affected_indices = []
        
        for i, row in enumerate(data):
            if RuleEngine._evaluate_conditions(row, rule_config['conditions']):
                affected_indices.append(i)
        
        return {
            'affected_indices': affected_indices,
            'affected_count': len(affected_indices),
            'total_rows': len(data),
            'preview_rows': [data[i] for i in affected_indices[:10]]  # First 10 for preview
        }

    @staticmethod
    def execute_rule(data, rule_config):
        affected_indices = []
        undo_data = {'original_values': []}
        
        # Validate input data
        if not isinstance(data, list):
            raise ValueError("Data must be a list of dictionaries")
        
        if not rule_config or 'conditions' not in rule_config or 'action' not in rule_config:
            raise ValueError("Rule config must contain 'conditions' and 'action'")
        
        for i, row in enumerate(data):
            if not isinstance(row, dict):
                continue  # Skip non-dict rows
                
            if RuleEngine._evaluate_conditions(row, rule_config['conditions']):
                affected_indices.append(i)
                
                # Store original values for undo
                action = rule_config['action']
                if action['type'] in ['set_value', 'increment', 'decrement', 'concatenate', 
                                    'clear', 'copy_from', 'uppercase', 'lowercase', 'trim']:
                    original_value = row.get(action['column'])
                    undo_data['original_values'].append({
                        'index': i,
                        'column': action['column'],
                        'value': original_value
                    })
                
                # Apply action
                RuleEngine._apply_action(row, action)
        
        return {
            'affected_indices': affected_indices,
            'affected_count': len(affected_indices),
            'undo_data': undo_data,
            'modified_data': data
        }

    @staticmethod
    def _evaluate_conditions(row, conditions):
        if not conditions:
            return True
        
        results = []
        
        for condition in conditions:
            column = condition['column']
            operator = condition['operator']
            value = condition['value']
            
            row_value = row.get(column)
            
            try:
                if operator in RuleEngine.OPERATORS:
                    result = RuleEngine.OPERATORS[operator](row_value, value)
                else:
                    result = False
            except Exception as e:
                result = False
            
            results.append(result)
        
        # Apply logic (AND/OR) - simplified for now
        return all(results)  # Default to AND logic

    @staticmethod
    def _apply_action(row, action):
        try:
            action_type = action.get('type')
            column = action.get('column')
            
            if not action_type or not column:
                return  # Skip invalid actions
            
            if action_type == 'set_value':
                row[column] = action.get('value', '')
            
            elif action_type == 'increment':
                try:
                    current = float(row.get(column, 0))
                    increment_value = float(action.get('value', 0))
                    row[column] = current + increment_value
                except (ValueError, TypeError):
                    pass  # Skip if conversion fails
            
            elif action_type == 'decrement':
                try:
                    current = float(row.get(column, 0))
                    decrement_value = float(action.get('value', 0))
                    row[column] = current - decrement_value
                except (ValueError, TypeError):
                    pass  # Skip if conversion fails
            
            elif action_type == 'concatenate':
                current = str(row.get(column, ''))
                concat_value = str(action.get('value', ''))
                row[column] = current + concat_value
            
            elif action_type == 'clear':
                row[column] = None
            
            elif action_type == 'copy_from':
                source_column = action.get('source_column')
                if source_column:
                    row[column] = row.get(source_column)
            
            elif action_type == 'uppercase':
                if row.get(column) is not None:
                    row[column] = str(row[column]).upper()
            
            elif action_type == 'lowercase':
                if row.get(column) is not None:
                    row[column] = str(row[column]).lower()
            
            elif action_type == 'trim':
                if row.get(column) is not None:
                    row[column] = str(row[column]).strip()
                    
        except Exception as e:
            # Log the error but don't fail the entire operation
            print(f"Error applying action {action_type} to column {column}: {str(e)}")
            pass

    @staticmethod
    def undo_operation(data, undo_data):
        for item in undo_data['original_values']:
            index = item['index']
            column = item['column']
            value = item['value']
            
            if index < len(data):
                data[index][column] = value
        
        return data