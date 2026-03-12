import re
from datetime import datetime
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

class TypeDetector:
    @staticmethod
    def detect_column_types(data):
        if not data:
            return []
        
        columns = list(data[0].keys())
        schema = []
        
        for col in columns:
            values = [row.get(col) for row in data if row.get(col) is not None and str(row.get(col)).strip()]
            col_type, confidence = TypeDetector._detect_type(values)
            null_count = sum(1 for row in data if row.get(col) is None or str(row.get(col)).strip() == '')
            sample_values = list(set(str(v) for v in values[:10]))
            
            schema.append({
                'name': col,
                'type': col_type,
                'confidence': confidence,
                'null_count': null_count,
                'sample_values': sample_values
            })
        
        return schema

    @staticmethod
    def _detect_type(values):
        if not values:
            return 'string', 0.0
        
        total = len(values)
        type_scores = {}
        
        # Test each type
        type_scores['integer'] = TypeDetector._test_integer(values) / total
        type_scores['float'] = TypeDetector._test_float(values) / total
        type_scores['boolean'] = TypeDetector._test_boolean(values) / total
        type_scores['date'] = TypeDetector._test_date(values) / total
        type_scores['datetime'] = TypeDetector._test_datetime(values) / total
        type_scores['email'] = TypeDetector._test_email(values) / total
        type_scores['phone'] = TypeDetector._test_phone(values) / total
        type_scores['url'] = TypeDetector._test_url(values) / total
        type_scores['currency'] = TypeDetector._test_currency(values) / total
        type_scores['percentage'] = TypeDetector._test_percentage(values) / total
        
        # Find best match
        best_type = max(type_scores, key=type_scores.get)
        confidence = type_scores[best_type]
        
        # Default to string if confidence is too low
        if confidence < 0.7:
            return 'string', 1.0
        
        return best_type, confidence

    @staticmethod
    def _test_integer(values):
        count = 0
        for v in values:
            try:
                int(str(v).replace(',', ''))
                count += 1
            except:
                pass
        return count

    @staticmethod
    def _test_float(values):
        count = 0
        for v in values:
            try:
                float(str(v).replace(',', ''))
                count += 1
            except:
                pass
        return count

    @staticmethod
    def _test_boolean(values):
        bool_values = {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n'}
        return sum(1 for v in values if str(v).lower().strip() in bool_values)

    @staticmethod
    def _test_date(values):
        count = 0
        for v in values:
            try:
                if HAS_PANDAS:
                    pd.to_datetime(str(v))
                else:
                    # Simple date parsing without pandas
                    datetime.strptime(str(v), '%Y-%m-%d')
                count += 1
            except:
                pass
        return count

    @staticmethod
    def _test_datetime(values):
        count = 0
        for v in values:
            try:
                if HAS_PANDAS:
                    dt = pd.to_datetime(str(v))
                    if dt.time() != datetime.min.time():
                        count += 1
                else:
                    # Simple datetime parsing without pandas
                    dt = datetime.strptime(str(v), '%Y-%m-%d %H:%M:%S')
                    if dt.time() != datetime.min.time():
                        count += 1
            except:
                pass
        return count

    @staticmethod
    def _test_email(values):
        pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
        return sum(1 for v in values if re.match(pattern, str(v)))

    @staticmethod
    def _test_phone(values):
        pattern = r'^[\+]?[1-9]?[\d\s\-\(\)]{7,15}$'
        return sum(1 for v in values if re.match(pattern, str(v).strip()))

    @staticmethod
    def _test_url(values):
        pattern = r'^https?://'
        return sum(1 for v in values if re.match(pattern, str(v)))

    @staticmethod
    def _test_currency(values):
        pattern = r'^[\$€£¥]?\d+\.?\d*$'
        return sum(1 for v in values if re.match(pattern, str(v).strip()))

    @staticmethod
    def _test_percentage(values):
        pattern = r'^\d+\.?\d*\s*%$'
        return sum(1 for v in values if re.match(pattern, str(v).strip()))