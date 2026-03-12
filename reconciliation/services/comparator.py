try:
    from fuzzywuzzy import fuzz
    HAS_FUZZYWUZZY = True
except ImportError:
    HAS_FUZZYWUZZY = False
from django.utils import timezone

class DatasetComparator:
    @staticmethod
    def compare_datasets(source_data, target_data, source_keys, target_keys, 
                        compare_columns, fuzzy_match=False, fuzzy_threshold=80):
        
        # Build target index
        target_index = {}
        for i, row in enumerate(target_data):
            key = DatasetComparator._build_key(row, target_keys)
            target_index[key] = {'index': i, 'row': row}
        
        matches = []
        mismatches = []
        source_only = []
        matched_target_indices = set()
        
        # Process source rows
        for source_idx, source_row in enumerate(source_data):
            source_key = DatasetComparator._build_key(source_row, source_keys)
            
            # Try exact match first
            target_match = target_index.get(source_key)
            
            # Try fuzzy match if enabled and no exact match
            if not target_match and fuzzy_match:
                target_match = DatasetComparator._find_fuzzy_match(
                    source_key, target_index, fuzzy_threshold
                )
            
            if target_match:
                target_idx = target_match['index']
                target_row = target_match['row']
                matched_target_indices.add(target_idx)
                
                # Compare values
                differences = DatasetComparator._compare_values(
                    source_row, target_row, compare_columns
                )
                
                if differences:
                    mismatches.append({
                        'source_index': source_idx,
                        'target_index': target_idx,
                        'key': source_key,
                        'differences': differences,
                        'source_row': source_row,
                        'target_row': target_row
                    })
                else:
                    matches.append({
                        'source_index': source_idx,
                        'target_index': target_idx,
                        'key': source_key,
                        'row': source_row
                    })
            else:
                source_only.append({
                    'index': source_idx,
                    'key': source_key,
                    'row': source_row
                })
        
        # Find target-only rows
        target_only = []
        for key, target_info in target_index.items():
            if target_info['index'] not in matched_target_indices:
                target_only.append({
                    'index': target_info['index'],
                    'key': key,
                    'row': target_info['row']
                })
        
        # Calculate summary
        total_source = len(source_data)
        total_target = len(target_data)
        match_count = len(matches)
        mismatch_count = len(mismatches)
        source_only_count = len(source_only)
        target_only_count = len(target_only)
        match_rate = (match_count / total_source * 100) if total_source > 0 else 0
        
        return {
            'matches': matches,
            'mismatches': mismatches,
            'source_only': source_only,
            'target_only': target_only,
            'summary': {
                'total_source': total_source,
                'total_target': total_target,
                'match_count': match_count,
                'mismatch_count': mismatch_count,
                'source_only_count': source_only_count,
                'target_only_count': target_only_count,
                'match_rate': round(match_rate, 2)
            }
        }
    
    @staticmethod
    def _build_key(row, key_columns):
        values = []
        for col in key_columns:
            value = row.get(col, '')
            if value is None:
                value = ''
            values.append(str(value).lower().strip())
        return '|||'.join(values)
    
    @staticmethod
    def _find_fuzzy_match(source_key, target_index, threshold):
        if not HAS_FUZZYWUZZY:
            return None
            
        best_match = None
        best_score = 0
        
        for target_key, target_info in target_index.items():
            score = fuzz.ratio(source_key, target_key)
            if score >= threshold and score > best_score:
                best_score = score
                best_match = target_info
        
        return best_match
    
    @staticmethod
    def _compare_values(source_row, target_row, compare_columns):
        differences = []
        
        for col in compare_columns:
            source_val = source_row.get(col)
            target_val = target_row.get(col)
            
            # Normalize values for comparison
            source_str = str(source_val).strip().lower() if source_val is not None else ''
            target_str = str(target_val).strip().lower() if target_val is not None else ''
            
            if source_str != target_str:
                differences.append({
                    'column': col,
                    'source': source_val,
                    'target': target_val
                })
        
        return differences