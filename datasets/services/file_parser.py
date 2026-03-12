"""
Fixed FileParser for SmartSheet Pro
Handles JSON serialization properly for pandas DataFrames
"""
import json
import traceback
from typing import List, Dict, Any

import pandas as pd
import numpy as np


class FileParser:
    """Parse uploaded files and return JSON-serializable data"""
    
    SUPPORTED_TYPES = ['csv', 'tsv', 'xlsx', 'xls', 'json']
    
    @staticmethod
    def parse_file(file_path: str, file_type: str) -> List[Dict[str, Any]]:
        """
        Parse file and return JSON-serializable list of dicts.
        
        Args:
            file_path: Path to the uploaded file
            file_type: File extension (csv, xlsx, etc.)
            
        Returns:
            List of dictionaries representing rows
            
        Raises:
            ValueError: For unsupported file types or parsing errors
            ImportError: For missing dependencies
        """
        file_type = file_type.lower()
        
        if file_type not in FileParser.SUPPORTED_TYPES:
            raise ValueError(f"Unsupported file type: {file_type}. Supported: {FileParser.SUPPORTED_TYPES}")
        
        try:
            # Parse based on file type
            if file_type == 'csv':
                df = pd.read_csv(file_path, encoding='utf-8')
            elif file_type == 'tsv':
                df = pd.read_csv(file_path, sep='\t', encoding='utf-8')
            elif file_type in ['xlsx', 'xls']:
                df = pd.read_excel(file_path, engine='openpyxl')
            elif file_type == 'json':
                df = pd.read_json(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            # Convert DataFrame to JSON-serializable format
            data = FileParser._make_json_serializable(df)
            
            # Validate that data is actually JSON serializable
            FileParser._validate_json_serializable(data)
            
            return data
            
        except pd.errors.EmptyDataError:
            raise ValueError("The uploaded file is empty")
        except pd.errors.ParserError as e:
            raise ValueError(f"Error parsing file: {str(e)}")
        except UnicodeDecodeError:
            # Try with different encoding
            try:
                if file_type == 'csv':
                    df = pd.read_csv(file_path, encoding='latin-1')
                elif file_type == 'tsv':
                    df = pd.read_csv(file_path, sep='\t', encoding='latin-1')
                else:
                    raise
                data = FileParser._make_json_serializable(df)
                FileParser._validate_json_serializable(data)
                return data
            except Exception as inner_e:
                raise ValueError(f"Encoding error: Could not decode file. {str(inner_e)}")
        except Exception as e:
            # Log the actual error for debugging
            print(f"FileParser Error: {traceback.format_exc()}")
            raise ValueError(f"Error parsing file: {type(e).__name__}: {str(e)}")
    
    @staticmethod
    def _make_json_serializable(df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Convert pandas DataFrame to JSON-serializable list of dicts.
        
        Handles:
        - NaN, NaT, None values
        - Infinity values
        - numpy integer/float types
        - pandas Timestamp
        - bytes
        """
        # Step 1: Replace problematic values at DataFrame level
        df = df.replace({np.nan: None, pd.NaT: None})
        df = df.replace([np.inf, -np.inf], None)
        
        # Step 2: Convert to list of dicts
        records = df.to_dict(orient='records')
        
        # Step 3: Convert each value to JSON-serializable type
        cleaned_records = []
        for record in records:
            cleaned_record = {}
            for key, value in record.items():
                cleaned_record[str(key)] = FileParser._convert_value(value)
            cleaned_records.append(cleaned_record)
        
        return cleaned_records
    
    @staticmethod
    def _convert_value(val: Any) -> Any:
        """Convert a single value to JSON-serializable type"""
        # None check
        if val is None:
            return None
        
        # Check for pandas NA
        if pd.isna(val):
            return None
        
        # numpy integers
        if isinstance(val, (np.integer, np.int64, np.int32, np.int16, np.int8)):
            return int(val)
        
        # numpy floats
        if isinstance(val, (np.floating, np.float64, np.float32, np.float16)):
            if np.isnan(val) or np.isinf(val):
                return None
            return float(val)
        
        # numpy booleans
        if isinstance(val, np.bool_):
            return bool(val)
        
        # pandas Timestamp
        if isinstance(val, pd.Timestamp):
            return val.isoformat()
        
        # datetime objects
        if hasattr(val, 'isoformat'):
            return val.isoformat()
        
        # bytes
        if isinstance(val, bytes):
            return val.decode('utf-8', errors='replace')
        
        # numpy arrays
        if isinstance(val, np.ndarray):
            return val.tolist()
        
        # Already JSON-serializable types
        if isinstance(val, (str, int, float, bool, list, dict)):
            return val
        
        # Fallback: convert to string
        return str(val)
    
    @staticmethod
    def _validate_json_serializable(data: List[Dict]) -> None:
        """Validate that data can be serialized to JSON"""
        try:
            json.dumps(data)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Data contains non-JSON-serializable values: {str(e)}")
    
    @staticmethod
    def get_preview(file_path: str, file_type: str, rows: int = 10) -> List[Dict[str, Any]]:
        """Get preview of first N rows"""
        data = FileParser.parse_file(file_path, file_type)
        return data[:rows]
    
    @staticmethod
    def get_column_names(file_path: str, file_type: str) -> List[str]:
        """Get column names without loading full file"""
        file_type = file_type.lower()
        
        try:
            if file_type == 'csv':
                df = pd.read_csv(file_path, nrows=0)
            elif file_type == 'tsv':
                df = pd.read_csv(file_path, sep='\t', nrows=0)
            elif file_type in ['xlsx', 'xls']:
                df = pd.read_excel(file_path, engine='openpyxl', nrows=0)
            elif file_type == 'json':
                df = pd.read_json(file_path)
                return list(df.columns)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            
            return [str(col) for col in df.columns]
        except Exception as e:
            raise ValueError(f"Error reading column names: {str(e)}")