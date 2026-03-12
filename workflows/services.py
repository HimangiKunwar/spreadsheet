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
    
    def get_available_operations(self):
        """Return all available operations organized by category"""
        
        all_operations = []
        
        # ============================================
        # CATEGORY 1: DATA CLEANUP OPERATIONS (5)
        # ============================================
        cleanup_operations = [
            {
                'id': 'trim_whitespace',
                'name': 'Trim Whitespace',
                'description': 'Remove leading/trailing spaces',
                'category': 'cleanup',
                'requiresColumn': True,
                'params': []
            },
            {
                'id': 'remove_duplicates',
                'name': 'Remove Duplicates',
                'description': 'Remove duplicate rows',
                'category': 'cleanup',
                'requiresColumn': False,
                'params': []
            },
            {
                'id': 'fill_empty',
                'name': 'Fill Empty Cells',
                'description': 'Fill empty cells with a value',
                'category': 'cleanup',
                'requiresColumn': True,
                'params': [
                    {'name': 'fill_value', 'type': 'text', 'label': 'Fill Value', 'default': ''}
                ]
            },
            {
                'id': 'remove_empty_rows',
                'name': 'Remove Empty Rows',
                'description': 'Delete rows that are completely empty',
                'category': 'cleanup',
                'requiresColumn': False,
                'params': []
            },
            {
                'id': 'standardize_case',
                'name': 'Standardize Case',
                'description': 'Convert text to upper/lower/title case',
                'category': 'cleanup',
                'requiresColumn': True,
                'params': [
                    {'name': 'case_type', 'type': 'select', 'options': ['upper', 'lower', 'title'], 'default': 'lower'}
                ]
            },
        ]
        
        # ============================================
        # CATEGORY 2: VALIDATION OPERATIONS (12)
        # ============================================
        validation_operations = [
            {
                'id': 'validate_email',
                'name': 'Validate Email',
                'description': 'Check if email addresses are valid format',
                'category': 'validation',
                'requiresColumn': True,
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
                'requiresColumn': True,
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
                'requiresColumn': True,
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
                'id': 'validate_number',
                'name': 'Validate Number',
                'description': 'Check if values are valid numbers',
                'category': 'validation',
                'requiresColumn': True,
                'params': []
            },
            {
                'id': 'check_for_blanks',
                'name': 'Check for Blanks',
                'description': 'Find empty/blank cells in a column',
                'category': 'validation',
                'requiresColumn': True,
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
                'requiresColumn': True,
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
                'requiresColumn': True,
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
                'requiresColumn': True,
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
                'requiresColumn': True,
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
                'requiresColumn': False,
                'params': [
                    {'name': 'columns', 'type': 'text', 'required': True, 'label': 'Required Columns (comma-separated)'},
                ]
            },
            {
                'id': 'validate_pan_aadhaar',
                'name': 'Validate PAN/Aadhaar',
                'description': 'Validate Indian ID formats (PAN, Aadhaar, GST)',
                'category': 'validation',
                'requiresColumn': True,
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
                'requiresColumn': True,
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
                'requiresColumn': False,
                'params': [
                    {'name': 'columns', 'type': 'text', 'required': False, 'label': 'Columns to Check (blank=all)'},
                    {'name': 'output_column', 'type': 'text', 'required': False, 'label': 'Output Column Name', 'default': 'error_summary'},
                ]
            },
        ]
        
        # ============================================
        # CATEGORY 3: TEXT TRANSFORMATION OPERATIONS (12)
        # ============================================
        text_operations = [
            {
                'id': 'convert_uppercase',
                'name': 'Convert to UPPERCASE',
                'description': 'Convert all text to capital letters',
                'category': 'text_transformation',
                'requiresColumn': True,
                'params': []
            },
            {
                'id': 'convert_lowercase',
                'name': 'Convert to lowercase',
                'description': 'Convert all text to small letters',
                'category': 'text_transformation',
                'requiresColumn': True,
                'params': []
            },
            {
                'id': 'convert_titlecase',
                'name': 'Convert to Title Case',
                'description': 'Capitalize first letter of each word',
                'category': 'text_transformation',
                'requiresColumn': True,
                'params': []
            },
            {
                'id': 'convert_sentencecase',
                'name': 'Convert to Sentence case',
                'description': 'Capitalize only first letter',
                'category': 'text_transformation',
                'requiresColumn': True,
                'params': []
            },
            {
                'id': 'find_and_replace',
                'name': 'Find and Replace',
                'description': 'Replace specific text with another',
                'category': 'text_transformation',
                'requiresColumn': True,
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
                'requiresColumn': True,
                'params': [
                    {'name': 'prefix_value', 'type': 'string', 'label': 'Prefix', 'required': True}
                ]
            },
            {
                'id': 'add_suffix',
                'name': 'Add Suffix',
                'description': 'Add text after each value',
                'category': 'text_transformation',
                'requiresColumn': True,
                'params': [
                    {'name': 'suffix_value', 'type': 'string', 'label': 'Suffix', 'required': True}
                ]
            },
            {
                'id': 'extract_numbers',
                'name': 'Extract Numbers',
                'description': 'Keep only numeric characters',
                'category': 'text_transformation',
                'requiresColumn': True,
                'params': []
            },
            {
                'id': 'extract_text',
                'name': 'Extract Text',
                'description': 'Keep only letter characters',
                'category': 'text_transformation',
                'requiresColumn': True,
                'params': []
            },
            {
                'id': 'split_text',
                'name': 'Split Text',
                'description': 'Split text into multiple columns',
                'category': 'text_transformation',
                'requiresColumn': True,
                'params': [
                    {'name': 'delimiter', 'type': 'string', 'label': 'Split by', 'required': True, 'default': ','},
                    {'name': 'output_column', 'type': 'string', 'label': 'New column name', 'required': False}
                ]
            },
            {
                'id': 'merge_columns',
                'name': 'Merge Columns',
                'description': 'Combine multiple columns into one',
                'category': 'text_transformation',
                'requiresColumn': False,
                'params': [
                    {'name': 'columns_to_merge', 'type': 'text', 'label': 'Columns (comma-separated)', 'required': True},
                    {'name': 'separator', 'type': 'string', 'label': 'Separator', 'required': False, 'default': ' '},
                    {'name': 'output_column', 'type': 'string', 'label': 'Output column name', 'required': False, 'default': 'merged'}
                ]
            },
            {
                'id': 'reverse_text',
                'name': 'Reverse Text',
                'description': 'Reverse the text in each cell',
                'category': 'text_transformation',
                'requiresColumn': True,
                'params': []
            },
        ]
        
        # ============================================
        # CATEGORY 4: NUMBER OPERATIONS (12)
        # ============================================
        number_operations = [
            {
                'id': 'round_numbers',
                'name': 'Round Numbers',
                'description': 'Round numbers to specified decimal places',
                'category': 'number_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'decimal_places', 'type': 'number', 'label': 'Decimal Places', 'required': True, 'default': 2}
                ]
            },
            {
                'id': 'format_currency',
                'name': 'Format as Currency',
                'description': 'Format number as currency with symbol',
                'category': 'number_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'currency_symbol', 'type': 'select', 'label': 'Currency', 'required': True, 'default': '₹',
                     'options': [
                         {'value': '₹', 'label': '₹ (INR)'},
                         {'value': '$', 'label': '$ (USD)'},
                         {'value': '€', 'label': '€ (EUR)'},
                         {'value': '£', 'label': '£ (GBP)'}
                     ]},
                    {'name': 'decimal_places', 'type': 'number', 'label': 'Decimal Places', 'required': True, 'default': 2}
                ]
            },
            {
                'id': 'format_percentage',
                'name': 'Format as Percentage',
                'description': 'Convert number to percentage format',
                'category': 'number_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'decimal_places', 'type': 'number', 'label': 'Decimal Places', 'required': True, 'default': 2}
                ]
            },
            {
                'id': 'add_subtract_value',
                'name': 'Add/Subtract Value',
                'description': 'Add or subtract a value from all numbers',
                'category': 'number_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'operation', 'type': 'select', 'label': 'Operation', 'required': True, 'default': 'add',
                     'options': [
                         {'value': 'add', 'label': 'Add (+)'},
                         {'value': 'subtract', 'label': 'Subtract (-)'}
                     ]},
                    {'name': 'value', 'type': 'number', 'label': 'Value', 'required': True, 'default': 0}
                ]
            },
            {
                'id': 'multiply_divide',
                'name': 'Multiply/Divide',
                'description': 'Multiply or divide all numbers by a value',
                'category': 'number_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'operation', 'type': 'select', 'label': 'Operation', 'required': True, 'default': 'multiply',
                     'options': [
                         {'value': 'multiply', 'label': 'Multiply (×)'},
                         {'value': 'divide', 'label': 'Divide (÷)'}
                     ]},
                    {'name': 'value', 'type': 'number', 'label': 'Value', 'required': True, 'default': 1}
                ]
            },
            {
                'id': 'calculate_sum',
                'name': 'Calculate Sum',
                'description': 'Calculate sum of column and add as row/column',
                'category': 'number_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'output_type', 'type': 'select', 'label': 'Output As', 'required': True, 'default': 'new_row',
                     'options': [
                         {'value': 'new_row', 'label': 'Add as new row'},
                         {'value': 'new_column', 'label': 'Add as new column'}
                     ]}
                ]
            },
            {
                'id': 'calculate_average',
                'name': 'Calculate Average',
                'description': 'Calculate average of column and add as row/column',
                'category': 'number_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'output_type', 'type': 'select', 'label': 'Output As', 'required': True, 'default': 'new_row',
                     'options': [
                         {'value': 'new_row', 'label': 'Add as new row'},
                         {'value': 'new_column', 'label': 'Add as new column'}
                     ]}
                ]
            },
            {
                'id': 'find_min_max',
                'name': 'Find Min/Max',
                'description': 'Find minimum or maximum value in column',
                'category': 'number_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'operation', 'type': 'select', 'label': 'Find', 'required': True, 'default': 'max',
                     'options': [
                         {'value': 'max', 'label': 'Maximum'},
                         {'value': 'min', 'label': 'Minimum'}
                     ]},
                    {'name': 'output_type', 'type': 'select', 'label': 'Output As', 'required': True, 'default': 'new_row',
                     'options': [
                         {'value': 'new_row', 'label': 'Add as new row'},
                         {'value': 'new_column', 'label': 'Add as new column'}
                     ]}
                ]
            },
            {
                'id': 'remove_decimals',
                'name': 'Remove Decimals',
                'description': 'Convert decimal numbers to integers',
                'category': 'number_operations',
                'requiresColumn': True,
                'params': []
            },
            {
                'id': 'negative_to_positive',
                'name': 'Negative to Positive',
                'description': 'Convert all negative numbers to positive (absolute value)',
                'category': 'number_operations',
                'requiresColumn': True,
                'params': []
            },
            {
                'id': 'number_to_words',
                'name': 'Number to Words',
                'description': 'Convert numbers to words (e.g., 100 → One Hundred)',
                'category': 'number_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'language', 'type': 'select', 'label': 'Language', 'required': True, 'default': 'en',
                     'options': [
                         {'value': 'en', 'label': 'English'},
                         {'value': 'hi', 'label': 'Hindi'}
                     ]}
                ]
            },
            {
                'id': 'generate_sequence',
                'name': 'Generate Sequence',
                'description': 'Generate sequential numbers in a new column',
                'category': 'number_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'column_name', 'type': 'string', 'label': 'Column Name', 'required': True, 'default': 'serial_no'},
                    {'name': 'start_value', 'type': 'number', 'label': 'Start From', 'required': True, 'default': 1},
                    {'name': 'step', 'type': 'number', 'label': 'Step', 'required': True, 'default': 1}
                ]
            },
        ]
        
        # ============================================
        # CATEGORY 5: DATE/TIME OPERATIONS (12)
        # ============================================
        date_operations = [
            {
                'id': 'standardize_date_format',
                'name': 'Standardize Date Format',
                'description': 'Convert dates to a standard format',
                'category': 'date_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'output_format', 'type': 'select', 'label': 'Output Format', 'default': 'DD/MM/YYYY',
                     'options': [
                         {'value': 'DD/MM/YYYY', 'label': 'DD/MM/YYYY (31/12/2024)'},
                         {'value': 'MM/DD/YYYY', 'label': 'MM/DD/YYYY (12/31/2024)'},
                         {'value': 'YYYY-MM-DD', 'label': 'YYYY-MM-DD (2024-12-31)'},
                         {'value': 'DD-MM-YYYY', 'label': 'DD-MM-YYYY (31-12-2024)'},
                         {'value': 'DD MMM YYYY', 'label': 'DD MMM YYYY (31 Dec 2024)'},
                         {'value': 'MMM DD, YYYY', 'label': 'MMM DD, YYYY (Dec 31, 2024)'},
                     ]}
                ]
            },
            {
                'id': 'extract_year',
                'name': 'Extract Year',
                'description': 'Extract year from date column',
                'category': 'date_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': '', 'placeholder': 'Leave empty to use column_year'}
                ]
            },
            {
                'id': 'extract_month',
                'name': 'Extract Month',
                'description': 'Extract month from date column',
                'category': 'date_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'output_type', 'type': 'select', 'label': 'Output As', 'default': 'name',
                     'options': [
                         {'value': 'name', 'label': 'Full Name (January)'},
                         {'value': 'short_name', 'label': 'Short Name (Jan)'},
                         {'value': 'number', 'label': 'Number (1)'},
                     ]},
                    {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': '', 'placeholder': 'Leave empty to use column_month'}
                ]
            },
            {
                'id': 'extract_day',
                'name': 'Extract Day',
                'description': 'Extract day from date column',
                'category': 'date_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': '', 'placeholder': 'Leave empty to use column_day'}
                ]
            },
            {
                'id': 'calculate_age',
                'name': 'Calculate Age',
                'description': 'Calculate age from date of birth',
                'category': 'date_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': 'age'}
                ]
            },
            {
                'id': 'add_subtract_days',
                'name': 'Add/Subtract Days',
                'description': 'Add or subtract days from dates',
                'category': 'date_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'operation', 'type': 'select', 'label': 'Operation', 'default': 'add',
                     'options': [
                         {'value': 'add', 'label': 'Add Days (+)'},
                         {'value': 'subtract', 'label': 'Subtract Days (-)'},
                     ]},
                    {'name': 'days', 'type': 'number', 'label': 'Number of Days', 'default': 0},
                    {'name': 'output_format', 'type': 'select', 'label': 'Output Format', 'default': 'DD/MM/YYYY',
                     'options': [
                         {'value': 'DD/MM/YYYY', 'label': 'DD/MM/YYYY'},
                         {'value': 'YYYY-MM-DD', 'label': 'YYYY-MM-DD'},
                     ]}
                ]
            },
            {
                'id': 'find_day_of_week',
                'name': 'Find Day of Week',
                'description': 'Get day name from date',
                'category': 'date_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'output_type', 'type': 'select', 'label': 'Output As', 'default': 'name',
                     'options': [
                         {'value': 'name', 'label': 'Full Name (Monday)'},
                         {'value': 'short_name', 'label': 'Short Name (Mon)'},
                         {'value': 'number', 'label': 'Number (1-7)'},
                     ]},
                    {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': '', 'placeholder': 'Leave empty to use column_weekday'}
                ]
            },
            {
                'id': 'calculate_duration',
                'name': 'Calculate Duration',
                'description': 'Calculate duration between two date columns',
                'category': 'date_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'end_column', 'type': 'column_select', 'label': 'End Date Column', 'required': True},
                    {'name': 'unit', 'type': 'select', 'label': 'Calculate In', 'default': 'days',
                     'options': [
                         {'value': 'days', 'label': 'Days'},
                         {'value': 'weeks', 'label': 'Weeks'},
                         {'value': 'months', 'label': 'Months'},
                         {'value': 'years', 'label': 'Years'},
                     ]},
                    {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': 'duration_days'}
                ]
            },
            {
                'id': 'insert_current_date',
                'name': 'Insert Current Date',
                'description': 'Add column with current date',
                'category': 'date_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'column_name', 'type': 'string', 'label': 'Column Name', 'default': 'current_date'},
                    {'name': 'date_format', 'type': 'select', 'label': 'Date Format', 'default': 'DD/MM/YYYY',
                     'options': [
                         {'value': 'DD/MM/YYYY', 'label': 'DD/MM/YYYY'},
                         {'value': 'YYYY-MM-DD', 'label': 'YYYY-MM-DD'},
                         {'value': 'DD MMM YYYY', 'label': 'DD MMM YYYY'},
                     ]}
                ]
            },
            {
                'id': 'insert_current_time',
                'name': 'Insert Current Time',
                'description': 'Add column with current time',
                'category': 'date_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'column_name', 'type': 'string', 'label': 'Column Name', 'default': 'current_time'},
                    {'name': 'time_format', 'type': 'select', 'label': 'Time Format', 'default': 'HH:MM:SS',
                     'options': [
                         {'value': 'HH:MM:SS', 'label': '24-hour (14:30:00)'},
                         {'value': 'HH:MM', 'label': '24-hour short (14:30)'},
                         {'value': 'hh:mm:ss AM/PM', 'label': '12-hour (02:30:00 PM)'},
                         {'value': 'hh:mm AM/PM', 'label': '12-hour short (02:30 PM)'},
                     ]}
                ]
            },
            {
                'id': 'convert_text_to_date',
                'name': 'Convert Text to Date',
                'description': 'Parse text strings as dates',
                'category': 'date_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'output_format', 'type': 'select', 'label': 'Output Format', 'default': 'DD/MM/YYYY',
                     'options': [
                         {'value': 'DD/MM/YYYY', 'label': 'DD/MM/YYYY'},
                         {'value': 'MM/DD/YYYY', 'label': 'MM/DD/YYYY'},
                         {'value': 'YYYY-MM-DD', 'label': 'YYYY-MM-DD'},
                     ]}
                ]
            },
            {
                'id': 'quarter_calculation',
                'name': 'Quarter Calculation',
                'description': 'Extract quarter from date',
                'category': 'date_operations',
                'requiresColumn': True,
                'params': [
                    {'name': 'output_format', 'type': 'select', 'label': 'Output Format', 'default': 'Q1',
                     'options': [
                         {'value': 'Q1', 'label': 'Q1, Q2, Q3, Q4'},
                         {'value': 'number', 'label': '1, 2, 3, 4'},
                         {'value': 'Q1 YYYY', 'label': 'Q1 2024'},
                     ]},
                    {'name': 'output_column', 'type': 'string', 'label': 'Output Column Name', 'default': '', 'placeholder': 'Leave empty to use column_quarter'}
                ]
            },
        ]
        
        # ============================================
        # CATEGORY 6: ROW/COLUMN OPERATIONS (12)
        # ============================================
        row_column_operations = [
            {
                'id': 'insert_rows',
                'name': 'Insert Rows',
                'description': 'Insert new blank rows at specified positions',
                'category': 'row_column',
                'requiresColumn': False,
                'params': [
                    {'name': 'position', 'type': 'select', 'options': ['after_every_n', 'at_index', 'at_end'], 'default': 'at_end'},
                    {'name': 'interval', 'type': 'number', 'label': 'After every N rows', 'default': 5},
                    {'name': 'index', 'type': 'number', 'label': 'Row index', 'default': 0},
                    {'name': 'count', 'type': 'number', 'label': 'Number of rows to insert', 'default': 1}
                ]
            },
            {
                'id': 'insert_columns',
                'name': 'Insert Columns',
                'description': 'Insert new columns at specified positions',
                'category': 'row_column',
                'requiresColumn': False,
                'params': [
                    {'name': 'column_name', 'type': 'text', 'label': 'New column name', 'required': True},
                    {'name': 'position', 'type': 'select', 'options': ['at_end', 'before', 'after', 'at_index'], 'default': 'at_end'},
                    {'name': 'reference_column', 'type': 'column', 'label': 'Reference column'},
                    {'name': 'default_value', 'type': 'text', 'label': 'Default value', 'default': ''}
                ]
            },
            {
                'id': 'delete_rows',
                'name': 'Delete Rows',
                'description': 'Delete rows based on conditions',
                'category': 'row_column',
                'requiresColumn': False,
                'params': [
                    {'name': 'condition', 'type': 'select', 'options': ['blank_rows', 'duplicate_rows', 'by_value', 'by_index'], 'default': 'blank_rows'},
                    {'name': 'column', 'type': 'column', 'label': 'Column to check'},
                    {'name': 'value', 'type': 'text', 'label': 'Value to match'},
                    {'name': 'indices', 'type': 'text', 'label': 'Row indices (comma-separated)'}
                ]
            },
            {
                'id': 'delete_columns',
                'name': 'Delete Columns',
                'description': 'Delete specified columns',
                'category': 'row_column',
                'requiresColumn': False,
                'params': [
                    {'name': 'condition', 'type': 'select', 'options': ['by_name', 'empty_columns'], 'default': 'by_name'},
                    {'name': 'columns', 'type': 'text', 'label': 'Column names (comma-separated)'}
                ]
            },
            {
                'id': 'hide_rows',
                'name': 'Hide Rows',
                'description': 'Mark rows as hidden based on conditions',
                'category': 'row_column',
                'requiresColumn': True,
                'params': [
                    {'name': 'condition', 'type': 'select', 'options': ['by_value', 'by_index'], 'default': 'by_value'},
                    {'name': 'value', 'type': 'text', 'label': 'Value to match'},
                    {'name': 'indices', 'type': 'text', 'label': 'Row indices (comma-separated)'}
                ]
            },
            {
                'id': 'hide_columns',
                'name': 'Hide Columns',
                'description': 'Mark columns as hidden',
                'category': 'row_column',
                'requiresColumn': False,
                'params': [
                    {'name': 'columns', 'type': 'text', 'label': 'Column names to hide (comma-separated)', 'required': True}
                ]
            },
            {
                'id': 'unhide_all',
                'name': 'Unhide All',
                'description': 'Show all hidden rows and columns',
                'category': 'row_column',
                'requiresColumn': False,
                'params': [
                    {'name': 'target', 'type': 'select', 'options': ['all', 'rows', 'columns'], 'default': 'all'}
                ]
            },
            {
                'id': 'sort_rows',
                'name': 'Sort Rows',
                'description': 'Sort data by column values',
                'category': 'row_column',
                'requiresColumn': True,
                'params': [
                    {'name': 'order', 'type': 'select', 'options': ['asc', 'desc'], 'default': 'asc', 'label': 'Sort Order'}
                ]
            },
            {
                'id': 'filter_data',
                'name': 'Filter Data',
                'description': 'Filter rows by condition',
                'category': 'row_column',
                'requiresColumn': True,
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
                'requiresColumn': False,
                'params': [
                    {'name': 'use_first_column_as_headers', 'type': 'boolean', 'label': 'Use first column as new headers', 'default': True}
                ]
            },
            {
                'id': 'group_rows',
                'name': 'Group Rows',
                'description': 'Group rows by column value',
                'category': 'row_column',
                'requiresColumn': True,
                'params': [
                    {'name': 'add_subtotals', 'type': 'boolean', 'label': 'Add subtotals', 'default': False},
                    {'name': 'subtotal_columns', 'type': 'text', 'label': 'Columns for subtotals (comma-separated)'}
                ]
            },
            {
                'id': 'freeze_panes',
                'name': 'Freeze Panes',
                'description': 'Set freeze panes for Excel export',
                'category': 'row_column',
                'requiresColumn': False,
                'params': [
                    {'name': 'freeze_rows', 'type': 'number', 'label': 'Rows to freeze', 'default': 1},
                    {'name': 'freeze_columns', 'type': 'number', 'label': 'Columns to freeze', 'default': 0}
                ]
            },
        ]
        
        # ============================================
        # CATEGORY 7: FORMATTING OPERATIONS (15)
        # ============================================
        formatting_operations = [
            {
                'id': 'autofit_column_width',
                'name': 'Auto-fit Column Width',
                'description': 'Automatically adjust column width to fit content',
                'category': 'formatting',
                'requiresColumn': False,
                'params': [
                    {'name': 'column', 'type': 'column_select', 'label': 'Column (leave empty for all)', 'required': False}
                ]
            },
            {
                'id': 'autofit_row_height',
                'name': 'Auto-fit Row Height',
                'description': 'Automatically adjust row height to fit content',
                'category': 'formatting',
                'requiresColumn': False,
                'params': [
                    {'name': 'row_range', 'type': 'string', 'label': 'Row Range (e.g., 1-10 or all)', 'default': 'all'}
                ]
            },
            {
                'id': 'apply_bold_italic',
                'name': 'Apply Bold/Italic',
                'description': 'Apply bold or italic formatting to text',
                'category': 'formatting',
                'requiresColumn': True,
                'params': [
                    {'name': 'style', 'type': 'select', 'label': 'Style', 'default': 'bold',
                     'options': [
                         {'value': 'bold', 'label': 'Bold'},
                         {'value': 'italic', 'label': 'Italic'},
                         {'value': 'both', 'label': 'Bold + Italic'},
                     ]},
                    {'name': 'row_range', 'type': 'select', 'label': 'Apply To', 'default': 'all',
                     'options': [
                         {'value': 'header', 'label': 'Header Only'},
                         {'value': 'data', 'label': 'Data Only'},
                         {'value': 'all', 'label': 'All Rows'},
                     ]}
                ]
            },
            {
                'id': 'change_font',
                'name': 'Change Font',
                'description': 'Change font family',
                'category': 'formatting',
                'requiresColumn': True,
                'params': [
                    {'name': 'font_name', 'type': 'select', 'label': 'Font', 'default': 'Calibri',
                     'options': [
                         {'value': 'Calibri', 'label': 'Calibri'},
                         {'value': 'Arial', 'label': 'Arial'},
                         {'value': 'Times New Roman', 'label': 'Times New Roman'},
                         {'value': 'Verdana', 'label': 'Verdana'},
                         {'value': 'Georgia', 'label': 'Georgia'},
                         {'value': 'Tahoma', 'label': 'Tahoma'},
                         {'value': 'Trebuchet MS', 'label': 'Trebuchet MS'},
                         {'value': 'Courier New', 'label': 'Courier New'},
                     ]}
                ]
            },
            {
                'id': 'change_font_size',
                'name': 'Change Font Size',
                'description': 'Change font size',
                'category': 'formatting',
                'requiresColumn': True,
                'params': [
                    {'name': 'font_size', 'type': 'select', 'label': 'Size', 'default': '11',
                     'options': [
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
                ]
            },
            {
                'id': 'apply_cell_color',
                'name': 'Apply Cell Color',
                'description': 'Apply background color to cells',
                'category': 'formatting',
                'requiresColumn': True,
                'params': [
                    {'name': 'color', 'type': 'select', 'label': 'Background Color', 'default': 'yellow',
                     'options': [
                         {'value': 'yellow', 'label': 'Yellow'},
                         {'value': 'light_green', 'label': 'Light Green'},
                         {'value': 'light_blue', 'label': 'Light Blue'},
                         {'value': 'light_red', 'label': 'Light Red'},
                         {'value': 'orange', 'label': 'Orange'},
                         {'value': 'cyan', 'label': 'Cyan'},
                         {'value': 'pink', 'label': 'Pink'},
                         {'value': 'gray', 'label': 'Gray'},
                     ]},
                    {'name': 'row_condition', 'type': 'select', 'label': 'Apply To', 'default': 'all',
                     'options': [
                         {'value': 'header', 'label': 'Header Only'},
                         {'value': 'data', 'label': 'Data Only'},
                         {'value': 'all', 'label': 'All Rows'},
                     ]}
                ]
            },
            {
                'id': 'apply_text_color',
                'name': 'Apply Text Color',
                'description': 'Change text/font color',
                'category': 'formatting',
                'requiresColumn': True,
                'params': [
                    {'name': 'color', 'type': 'select', 'label': 'Text Color', 'default': 'red',
                     'options': [
                         {'value': 'red', 'label': 'Red'},
                         {'value': 'green', 'label': 'Green'},
                         {'value': 'blue', 'label': 'Blue'},
                         {'value': 'orange', 'label': 'Orange'},
                         {'value': 'purple', 'label': 'Purple'},
                         {'value': 'black', 'label': 'Black'},
                     ]},
                    {'name': 'condition', 'type': 'select', 'label': 'Condition (Optional)', 'default': '',
                     'options': [
                         {'value': '', 'label': 'Always Apply'},
                         {'value': 'negative', 'label': 'Negative Numbers Only'},
                         {'value': 'less_than', 'label': 'Less Than Value'},
                         {'value': 'greater_than', 'label': 'Greater Than Value'},
                     ]},
                    {'name': 'condition_value', 'type': 'number', 'label': 'Condition Value', 'default': 0}
                ]
            },
            {
                'id': 'add_borders',
                'name': 'Add Borders',
                'description': 'Add borders to cells',
                'category': 'formatting',
                'requiresColumn': False,
                'params': [
                    {'name': 'column', 'type': 'column_select', 'label': 'Column (leave empty for all)', 'required': False},
                    {'name': 'border_style', 'type': 'select', 'label': 'Border Style', 'default': 'thin',
                     'options': [
                         {'value': 'thin', 'label': 'Thin'},
                         {'value': 'medium', 'label': 'Medium'},
                         {'value': 'thick', 'label': 'Thick'},
                         {'value': 'dashed', 'label': 'Dashed'},
                         {'value': 'dotted', 'label': 'Dotted'},
                     ]}
                ]
            },
            {
                'id': 'merge_cells',
                'name': 'Merge Cells',
                'description': 'Merge a range of cells',
                'category': 'formatting',
                'requiresColumn': False,
                'params': [
                    {'name': 'cell_range', 'type': 'string', 'label': 'Cell Range (e.g., A1:D1)', 'required': True, 'placeholder': 'A1:D1'}
                ]
            },
            {
                'id': 'unmerge_cells',
                'name': 'Unmerge Cells',
                'description': 'Unmerge previously merged cells',
                'category': 'formatting',
                'requiresColumn': False,
                'params': [
                    {'name': 'cell_range', 'type': 'string', 'label': 'Cell Range (e.g., A1:D1)', 'required': True, 'placeholder': 'A1:D1'}
                ]
            },
            {
                'id': 'align_text',
                'name': 'Align Text',
                'description': 'Set text alignment',
                'category': 'formatting',
                'requiresColumn': True,
                'params': [
                    {'name': 'horizontal', 'type': 'select', 'label': 'Horizontal Alignment', 'default': 'center',
                     'options': [
                         {'value': 'left', 'label': 'Left'},
                         {'value': 'center', 'label': 'Center'},
                         {'value': 'right', 'label': 'Right'},
                     ]},
                    {'name': 'vertical', 'type': 'select', 'label': 'Vertical Alignment', 'default': 'center',
                     'options': [
                         {'value': 'top', 'label': 'Top'},
                         {'value': 'center', 'label': 'Center'},
                         {'value': 'bottom', 'label': 'Bottom'},
                     ]}
                ]
            },
            {
                'id': 'apply_number_format',
                'name': 'Apply Number Format',
                'description': 'Apply number formatting (comma, currency, etc.)',
                'category': 'formatting',
                'requiresColumn': True,
                'params': [
                    {'name': 'format_type', 'type': 'select', 'label': 'Format Type', 'default': 'comma',
                     'options': [
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
                ]
            },
            {
                'id': 'conditional_formatting',
                'name': 'Conditional Formatting',
                'description': 'Apply formatting based on cell values',
                'category': 'formatting',
                'requiresColumn': True,
                'params': [
                    {'name': 'condition', 'type': 'select', 'label': 'Condition', 'default': 'less_than',
                     'options': [
                         {'value': 'less_than', 'label': 'Less Than (<)'},
                         {'value': 'greater_than', 'label': 'Greater Than (>)'},
                         {'value': 'equal', 'label': 'Equal To (=)'},
                         {'value': 'not_equal', 'label': 'Not Equal To (≠)'},
                         {'value': 'less_than_or_equal', 'label': 'Less Than or Equal (≤)'},
                         {'value': 'greater_than_or_equal', 'label': 'Greater Than or Equal (≥)'},
                     ]},
                    {'name': 'value', 'type': 'number', 'label': 'Value', 'default': 0},
                    {'name': 'color', 'type': 'select', 'label': 'Highlight Color', 'default': 'red',
                     'options': [
                         {'value': 'red', 'label': 'Red'},
                         {'value': 'yellow', 'label': 'Yellow'},
                         {'value': 'green', 'label': 'Green'},
                         {'value': 'orange', 'label': 'Orange'},
                         {'value': 'light_red', 'label': 'Light Red'},
                         {'value': 'light_green', 'label': 'Light Green'},
                     ]}
                ]
            },
            {
                'id': 'copy_formatting',
                'name': 'Copy Formatting',
                'description': 'Copy formatting from one column to another',
                'category': 'formatting',
                'requiresColumn': True,
                'params': [
                    {'name': 'target_column', 'type': 'column_select', 'label': 'Target Column', 'required': True}
                ]
            },
            {
                'id': 'clear_formatting',
                'name': 'Clear Formatting',
                'description': 'Remove all formatting from cells',
                'category': 'formatting',
                'requiresColumn': False,
                'params': [
                    {'name': 'column', 'type': 'column_select', 'label': 'Column (leave empty for all)', 'required': False}
                ]
            },
        ]
        
        # ============================================
        # CATEGORY 8: FILE OPERATIONS (12)
        # ============================================
        file_operations = [
            {
                'id': 'import_csv',
                'name': 'Import CSV',
                'description': 'Import data from a CSV file',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'source_path', 'type': 'text', 'label': 'Source File Path', 'required': True},
                    {'name': 'encoding', 'type': 'select', 'label': 'Encoding', 'options': ['utf-8', 'latin-1', 'cp1252'], 'default': 'utf-8'},
                    {'name': 'delimiter', 'type': 'select', 'label': 'Delimiter', 'options': [',', ';', '\t', '|'], 'default': ','}
                ]
            },
            {
                'id': 'import_text',
                'name': 'Import Text File',
                'description': 'Import data from a text file',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'source_path', 'type': 'text', 'label': 'Source File Path', 'required': True},
                    {'name': 'delimiter', 'type': 'select', 'label': 'Delimiter', 'options': ['\t', ',', '|', ' '], 'default': '\t'},
                    {'name': 'has_header', 'type': 'boolean', 'label': 'First Row is Header', 'default': True}
                ]
            },
            {
                'id': 'export_csv',
                'name': 'Export to CSV',
                'description': 'Export dataset to CSV format',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'output_name', 'type': 'text', 'label': 'Output Filename', 'required': True},
                    {'name': 'delimiter', 'type': 'select', 'label': 'Delimiter', 'options': [',', ';', '\t', '|'], 'default': ','},
                    {'name': 'include_header', 'type': 'boolean', 'label': 'Include Header Row', 'default': True}
                ]
            },
            {
                'id': 'export_pdf',
                'name': 'Export to PDF',
                'description': 'Export dataset to PDF format',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'output_name', 'type': 'text', 'label': 'Output Filename', 'required': True},
                    {'name': 'orientation', 'type': 'select', 'label': 'Page Orientation', 'options': ['portrait', 'landscape'], 'default': 'portrait'},
                    {'name': 'page_size', 'type': 'select', 'label': 'Page Size', 'options': ['A4', 'Letter', 'Legal', 'A3'], 'default': 'A4'},
                    {'name': 'include_header', 'type': 'boolean', 'label': 'Include Header Row', 'default': True}
                ]
            },
            {
                'id': 'combine_files',
                'name': 'Combine Files',
                'description': 'Merge multiple datasets into one',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'dataset_ids', 'type': 'text', 'label': 'Dataset IDs (comma-separated)', 'required': True},
                    {'name': 'merge_type', 'type': 'select', 'label': 'Merge Type', 'options': ['append_rows', 'join_columns', 'vlookup_join'], 'default': 'append_rows'},
                    {'name': 'join_column', 'type': 'column', 'label': 'Join Column (for vlookup)', 'required': False}
                ]
            },
            {
                'id': 'split_file',
                'name': 'Split File',
                'description': 'Split dataset into multiple files',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'split_by', 'type': 'select', 'label': 'Split By', 'options': ['row_count', 'column_value'], 'default': 'row_count'},
                    {'name': 'rows_per_file', 'type': 'number', 'label': 'Rows Per File', 'default': 100},
                    {'name': 'split_column', 'type': 'column', 'label': 'Split by Column Value', 'required': False}
                ]
            },
            {
                'id': 'batch_rename',
                'name': 'Batch Rename Columns',
                'description': 'Rename multiple columns at once',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'add_prefix', 'type': 'text', 'label': 'Add Prefix to All Columns', 'default': ''},
                    {'name': 'add_suffix', 'type': 'text', 'label': 'Add Suffix to All Columns', 'default': ''},
                    {'name': 'replace_spaces', 'type': 'boolean', 'label': 'Replace Spaces with Underscores', 'default': False},
                    {'name': 'to_lowercase', 'type': 'boolean', 'label': 'Convert to Lowercase', 'default': False}
                ]
            },
            {
                'id': 'auto_save',
                'name': 'Auto Save Checkpoint',
                'description': 'Create automatic save checkpoint',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'checkpoint_name', 'type': 'text', 'label': 'Checkpoint Name', 'default': 'auto_checkpoint'},
                    {'name': 'include_timestamp', 'type': 'boolean', 'label': 'Include Timestamp', 'default': True}
                ]
            },
            {
                'id': 'create_backup',
                'name': 'Create Backup',
                'description': 'Create a backup copy with timestamp',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'backup_name', 'type': 'text', 'label': 'Backup Name', 'default': 'backup'},
                    {'name': 'include_date', 'type': 'boolean', 'label': 'Include Date in Filename', 'default': True},
                    {'name': 'include_time', 'type': 'boolean', 'label': 'Include Time in Filename', 'default': False}
                ]
            },
            {
                'id': 'print_setup',
                'name': 'Print Setup',
                'description': 'Configure print layout settings',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'orientation', 'type': 'select', 'label': 'Orientation', 'options': ['portrait', 'landscape'], 'default': 'portrait'},
                    {'name': 'margins', 'type': 'select', 'label': 'Margins', 'options': ['normal', 'narrow', 'wide'], 'default': 'normal'},
                    {'name': 'fit_to_page', 'type': 'boolean', 'label': 'Fit to Page Width', 'default': True},
                    {'name': 'repeat_header', 'type': 'boolean', 'label': 'Repeat Header on Each Page', 'default': True}
                ]
            },
            {
                'id': 'save_template',
                'name': 'Save as Template',
                'description': 'Save current structure as reusable template',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'template_name', 'type': 'text', 'label': 'Template Name', 'required': True},
                    {'name': 'include_sample_data', 'type': 'boolean', 'label': 'Include Sample Data (first 5 rows)', 'default': False},
                    {'name': 'description', 'type': 'text', 'label': 'Template Description', 'default': ''}
                ]
            },
            {
                'id': 'batch_export',
                'name': 'Batch Export',
                'description': 'Export to multiple formats at once',
                'category': 'file_operations',
                'requiresColumn': False,
                'params': [
                    {'name': 'export_csv', 'type': 'boolean', 'label': 'Export as CSV', 'default': True},
                    {'name': 'export_excel', 'type': 'boolean', 'label': 'Export as Excel', 'default': True},
                    {'name': 'export_pdf', 'type': 'boolean', 'label': 'Export as PDF', 'default': False},
                    {'name': 'output_prefix', 'type': 'text', 'label': 'Output Filename Prefix', 'default': 'export'}
                ]
            }
        ]
        
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
        
        # ============================================
        # CATEGORY 10: CHART & REPORT OPERATIONS (12)
        # ============================================
        chart_report_operations = [
            {
                'id': 'create_bar_chart',
                'name': 'Create Bar Chart',
                'description': 'Create a bar chart from selected data columns',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'chart_title', 'type': 'text', 'label': 'Chart Title', 'required': True, 'default': 'Bar Chart'},
                    {'name': 'x_axis_column', 'type': 'column', 'label': 'X-Axis Column (Categories)', 'required': True},
                    {'name': 'y_axis_columns', 'type': 'text', 'label': 'Y-Axis Columns (comma-separated)', 'required': True, 'placeholder': 'Sales, Revenue, Profit'},
                    {'name': 'chart_style', 'type': 'select', 'label': 'Chart Style', 'options': ['clustered', 'stacked', 'stacked_100'], 'default': 'clustered'},
                    {'name': 'show_legend', 'type': 'boolean', 'label': 'Show Legend', 'default': True},
                    {'name': 'show_data_labels', 'type': 'boolean', 'label': 'Show Data Labels', 'default': False}
                ]
            },
            {
                'id': 'create_pie_chart',
                'name': 'Create Pie Chart',
                'description': 'Create a pie chart showing category distribution',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'chart_title', 'type': 'text', 'label': 'Chart Title', 'required': True, 'default': 'Pie Chart'},
                    {'name': 'labels_column', 'type': 'column', 'label': 'Labels Column', 'required': True},
                    {'name': 'values_column', 'type': 'column', 'label': 'Values Column', 'required': True},
                    {'name': 'chart_type', 'type': 'select', 'label': 'Pie Type', 'options': ['pie', 'doughnut', 'exploded'], 'default': 'pie'},
                    {'name': 'show_percentages', 'type': 'boolean', 'label': 'Show Percentages', 'default': True},
                    {'name': 'show_values', 'type': 'boolean', 'label': 'Show Values', 'default': False}
                ]
            },
            {
                'id': 'create_line_chart',
                'name': 'Create Line Chart',
                'description': 'Create a line chart showing trends over time',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'chart_title', 'type': 'text', 'label': 'Chart Title', 'required': True, 'default': 'Line Chart'},
                    {'name': 'x_axis_column', 'type': 'column', 'label': 'X-Axis Column (Time/Categories)', 'required': True},
                    {'name': 'y_axis_columns', 'type': 'text', 'label': 'Y-Axis Columns (comma-separated)', 'required': True},
                    {'name': 'line_style', 'type': 'select', 'label': 'Line Style', 'options': ['straight', 'smooth', 'stepped'], 'default': 'straight'},
                    {'name': 'show_markers', 'type': 'boolean', 'label': 'Show Data Markers', 'default': True},
                    {'name': 'show_gridlines', 'type': 'boolean', 'label': 'Show Gridlines', 'default': True}
                ]
            },
            {
                'id': 'update_chart_data',
                'name': 'Update Chart Data',
                'description': 'Update data source for existing chart',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'chart_name', 'type': 'text', 'label': 'Chart Name/ID to Update', 'required': True},
                    {'name': 'new_data_range', 'type': 'text', 'label': 'New Data Range (e.g., A1:D20)', 'required': True, 'default': 'A1:D20'},
                    {'name': 'refresh_title', 'type': 'boolean', 'label': 'Update Title with Timestamp', 'default': False},
                    {'name': 'preserve_formatting', 'type': 'boolean', 'label': 'Preserve Chart Formatting', 'default': True}
                ]
            },
            {
                'id': 'format_chart',
                'name': 'Format Chart',
                'description': 'Apply formatting and styling to charts',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'chart_name', 'type': 'text', 'label': 'Chart Name/ID', 'required': True},
                    {'name': 'color_scheme', 'type': 'select', 'label': 'Color Scheme', 'options': ['default', 'monochrome', 'colorful', 'pastel', 'corporate'], 'default': 'default'},
                    {'name': 'title_font_size', 'type': 'number', 'label': 'Title Font Size', 'default': 14},
                    {'name': 'show_border', 'type': 'boolean', 'label': 'Show Chart Border', 'default': True},
                    {'name': 'background_color', 'type': 'text', 'label': 'Background Color (hex)', 'default': '#FFFFFF'},
                    {'name': 'legend_position', 'type': 'select', 'label': 'Legend Position', 'options': ['bottom', 'top', 'left', 'right', 'none'], 'default': 'bottom'}
                ]
            },
            {
                'id': 'create_dashboard',
                'name': 'Create Dashboard',
                'description': 'Create a dashboard with multiple charts and KPIs',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'dashboard_title', 'type': 'text', 'label': 'Dashboard Title', 'required': True, 'default': 'Dashboard'},
                    {'name': 'kpi_columns', 'type': 'text', 'label': 'KPI Columns (comma-separated)', 'required': False, 'placeholder': 'Revenue, Orders, Customers'},
                    {'name': 'chart_types', 'type': 'text', 'label': 'Charts to Include (comma-separated)', 'default': 'bar, pie, line'},
                    {'name': 'layout', 'type': 'select', 'label': 'Dashboard Layout', 'options': ['grid_2x2', 'grid_3x2', 'vertical', 'horizontal'], 'default': 'grid_2x2'},
                    {'name': 'include_summary_table', 'type': 'boolean', 'label': 'Include Summary Table', 'default': True},
                    {'name': 'auto_refresh', 'type': 'boolean', 'label': 'Enable Auto-Refresh', 'default': False}
                ]
            },
            {
                'id': 'auto_refresh_charts',
                'name': 'Auto Refresh Charts',
                'description': 'Configure automatic chart refresh when data changes',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'chart_names', 'type': 'text', 'label': 'Chart Names (comma-separated, or "all")', 'default': 'all'},
                    {'name': 'refresh_mode', 'type': 'select', 'label': 'Refresh Mode', 'options': ['on_data_change', 'on_save', 'manual', 'scheduled'], 'default': 'on_data_change'},
                    {'name': 'refresh_interval_minutes', 'type': 'number', 'label': 'Refresh Interval (minutes, for scheduled)', 'default': 30},
                    {'name': 'show_refresh_timestamp', 'type': 'boolean', 'label': 'Show Last Refresh Time', 'default': True}
                ]
            },
            {
                'id': 'export_chart_image',
                'name': 'Export Chart as Image',
                'description': 'Export chart as PNG or JPG image file',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'chart_name', 'type': 'text', 'label': 'Chart Name/ID', 'required': True},
                    {'name': 'output_filename', 'type': 'text', 'label': 'Output Filename', 'required': True, 'default': 'chart_export'},
                    {'name': 'image_format', 'type': 'select', 'label': 'Image Format', 'options': ['png', 'jpg', 'svg', 'pdf'], 'default': 'png'},
                    {'name': 'width', 'type': 'number', 'label': 'Width (pixels)', 'default': 800},
                    {'name': 'height', 'type': 'number', 'label': 'Height (pixels)', 'default': 600},
                    {'name': 'dpi', 'type': 'number', 'label': 'Resolution (DPI)', 'default': 150}
                ]
            },
            {
                'id': 'create_pivot_table',
                'name': 'Create Pivot Table',
                'description': 'Create a pivot table to summarize data',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'row_fields', 'type': 'text', 'label': 'Row Fields (comma-separated)', 'required': True, 'placeholder': 'Region, Category'},
                    {'name': 'column_fields', 'type': 'text', 'label': 'Column Fields (comma-separated)', 'required': False, 'placeholder': 'Month, Quarter'},
                    {'name': 'value_field', 'type': 'column', 'label': 'Value Field', 'required': True},
                    {'name': 'aggregation', 'type': 'select', 'label': 'Aggregation Function', 'options': ['sum', 'count', 'average', 'min', 'max', 'std'], 'default': 'sum'},
                    {'name': 'output_sheet', 'type': 'text', 'label': 'Output Sheet Name', 'default': 'Pivot_Table'},
                    {'name': 'show_grand_totals', 'type': 'boolean', 'label': 'Show Grand Totals', 'default': True},
                    {'name': 'show_subtotals', 'type': 'boolean', 'label': 'Show Subtotals', 'default': True}
                ]
            },
            {
                'id': 'create_summary_report',
                'name': 'Create Summary Report',
                'description': 'Generate a summary report with key metrics',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'report_title', 'type': 'text', 'label': 'Report Title', 'required': True, 'default': 'Summary Report'},
                    {'name': 'metrics_columns', 'type': 'text', 'label': 'Metrics Columns (comma-separated)', 'required': True, 'placeholder': 'Revenue, Orders, Customers'},
                    {'name': 'group_by_column', 'type': 'column', 'label': 'Group By Column', 'required': False},
                    {'name': 'calculations', 'type': 'text', 'label': 'Calculations (sum, avg, min, max)', 'default': 'sum, avg'},
                    {'name': 'include_charts', 'type': 'boolean', 'label': 'Include Charts in Report', 'default': True},
                    {'name': 'output_format', 'type': 'select', 'label': 'Output Format', 'options': ['new_sheet', 'pdf', 'both'], 'default': 'new_sheet'}
                ]
            },
            {
                'id': 'generate_invoice',
                'name': 'Generate Invoice',
                'description': 'Auto-generate invoice from transaction data',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'invoice_number_prefix', 'type': 'text', 'label': 'Invoice Number Prefix', 'default': 'INV'},
                    {'name': 'customer_name_column', 'type': 'column', 'label': 'Customer Name Column', 'required': True},
                    {'name': 'item_description_column', 'type': 'column', 'label': 'Item Description Column', 'required': True},
                    {'name': 'quantity_column', 'type': 'column', 'label': 'Quantity Column', 'required': True},
                    {'name': 'unit_price_column', 'type': 'column', 'label': 'Unit Price Column', 'required': True},
                    {'name': 'tax_rate', 'type': 'number', 'label': 'Tax Rate (%)', 'default': 18},
                    {'name': 'include_gst', 'type': 'boolean', 'label': 'Include GST Breakdown (CGST/SGST)', 'default': True},
                    {'name': 'company_name', 'type': 'text', 'label': 'Your Company Name', 'default': 'Your Company'},
                    {'name': 'output_sheet', 'type': 'text', 'label': 'Output Sheet Name', 'default': 'Invoice'}
                ]
            },
            {
                'id': 'create_mis_report',
                'name': 'Create MIS Report',
                'description': 'Generate Management Information System report',
                'category': 'chart_report_operations',
                'requires_column': False,
                'params': [
                    {'name': 'report_title', 'type': 'text', 'label': 'Report Title', 'required': True, 'default': 'MIS Report'},
                    {'name': 'report_period', 'type': 'select', 'label': 'Report Period', 'options': ['daily', 'weekly', 'monthly', 'quarterly', 'yearly'], 'default': 'monthly'},
                    {'name': 'metrics_columns', 'type': 'text', 'label': 'Metrics to Include (comma-separated)', 'required': True, 'placeholder': 'Revenue, Expenses, Profit, Orders'},
                    {'name': 'comparison_type', 'type': 'select', 'label': 'Comparison', 'options': ['vs_previous_period', 'vs_target', 'vs_last_year', 'none'], 'default': 'vs_previous_period'},
                    {'name': 'target_column', 'type': 'column', 'label': 'Target Column (if comparing with target)', 'required': False},
                    {'name': 'include_variance', 'type': 'boolean', 'label': 'Include Variance Analysis', 'default': True},
                    {'name': 'include_charts', 'type': 'boolean', 'label': 'Include Charts', 'default': True},
                    {'name': 'include_recommendations', 'type': 'boolean', 'label': 'Include AI Recommendations', 'default': False},
                    {'name': 'output_sheet', 'type': 'text', 'label': 'Output Sheet Name', 'default': 'MIS_Report'}
                ]
            }
        ]
        
        # ============================================
        # CATEGORY 11: EMAIL OPERATIONS (8)
        # ============================================
        email_operations = [
            {
                'id': 'send_email',
                'name': 'Send Email',
                'description': 'Prepare and configure single email to be sent',
                'category': 'email_operations',
                'requires_column': False,
                'params': [
                    {'name': 'to_email_column', 'type': 'column', 'label': 'Recipient Email Column', 'required': True},
                    {'name': 'subject', 'type': 'text', 'label': 'Email Subject', 'required': True, 'default': 'Message from SmartSheet Pro'},
                    {'name': 'body_template', 'type': 'text', 'label': 'Email Body (use {column_name} for variables)', 'required': True, 'default': 'Hello {First_Name},\n\nThis is a message from SmartSheet Pro.\n\nRegards'},
                    {'name': 'from_name', 'type': 'text', 'label': 'Sender Name', 'default': 'SmartSheet Pro'},
                    {'name': 'reply_to', 'type': 'text', 'label': 'Reply-To Email', 'required': False},
                    {'name': 'priority', 'type': 'select', 'label': 'Priority', 'options': ['normal', 'high', 'low'], 'default': 'normal'},
                    {'name': 'row_filter', 'type': 'text', 'label': 'Send only to rows where (e.g., Status=Active)', 'required': False}
                ]
            },
            {
                'id': 'bulk_email',
                'name': 'Bulk Email',
                'description': 'Prepare bulk email campaign to multiple recipients from list',
                'category': 'email_operations',
                'requires_column': False,
                'params': [
                    {'name': 'email_column', 'type': 'column', 'label': 'Email Address Column', 'required': True},
                    {'name': 'name_column', 'type': 'column', 'label': 'Recipient Name Column', 'required': False},
                    {'name': 'subject', 'type': 'text', 'label': 'Email Subject', 'required': True},
                    {'name': 'body_template', 'type': 'text', 'label': 'Email Body Template', 'required': True},
                    {'name': 'batch_size', 'type': 'number', 'label': 'Batch Size (emails per batch)', 'default': 50},
                    {'name': 'delay_between_batches', 'type': 'number', 'label': 'Delay Between Batches (seconds)', 'default': 5},
                    {'name': 'filter_column', 'type': 'column', 'label': 'Filter Column (optional)', 'required': False},
                    {'name': 'filter_value', 'type': 'text', 'label': 'Filter Value (e.g., Active)', 'required': False},
                    {'name': 'exclude_unsubscribed', 'type': 'boolean', 'label': 'Exclude Unsubscribed', 'default': True}
                ]
            },
            {
                'id': 'email_with_attachment',
                'name': 'Email with Attachment',
                'description': 'Prepare email with file attachment',
                'category': 'email_operations',
                'requires_column': False,
                'params': [
                    {'name': 'to_email_column', 'type': 'column', 'label': 'Recipient Email Column', 'required': True},
                    {'name': 'subject', 'type': 'text', 'label': 'Email Subject', 'required': True},
                    {'name': 'body_template', 'type': 'text', 'label': 'Email Body', 'required': True},
                    {'name': 'attachment_type', 'type': 'select', 'label': 'Attachment Type', 'options': ['current_dataset', 'specific_file', 'generated_report'], 'default': 'current_dataset'},
                    {'name': 'attachment_path', 'type': 'text', 'label': 'Attachment File Path (if specific file)', 'required': False},
                    {'name': 'attachment_name', 'type': 'text', 'label': 'Attachment Display Name', 'default': 'Report.xlsx'},
                    {'name': 'export_format', 'type': 'select', 'label': 'Export Format (for dataset)', 'options': ['xlsx', 'csv', 'pdf'], 'default': 'xlsx'}
                ]
            },
            {
                'id': 'mail_merge',
                'name': 'Mail Merge',
                'description': 'Create personalized emails using template with data fields',
                'category': 'email_operations',
                'requires_column': False,
                'params': [
                    {'name': 'email_column', 'type': 'column', 'label': 'Email Address Column', 'required': True},
                    {'name': 'subject_template', 'type': 'text', 'label': 'Subject Template (use {Column_Name})', 'required': True, 'default': 'Hello {First_Name}, your invoice #{Invoice_No}'},
                    {'name': 'body_template', 'type': 'text', 'label': 'Body Template (use {Column_Name})', 'required': True, 'default': 'Dear {Salutation} {Last_Name},\n\nYour invoice #{Invoice_No} for {Amount} is due on {Due_Date}.\n\n{Personalized_Message}\n\nRegards,\n{Company}'},
                    {'name': 'preview_count', 'type': 'number', 'label': 'Generate Preview for First N Rows', 'default': 3},
                    {'name': 'validate_placeholders', 'type': 'boolean', 'label': 'Validate All Placeholders Exist', 'default': True},
                    {'name': 'missing_value_replacement', 'type': 'text', 'label': 'Replace Missing Values With', 'default': '[N/A]'}
                ]
            },
            {
                'id': 'schedule_email',
                'name': 'Schedule Email',
                'description': 'Schedule email to be sent at a specific date and time',
                'category': 'email_operations',
                'requires_column': False,
                'params': [
                    {'name': 'email_column', 'type': 'column', 'label': 'Email Address Column', 'required': True},
                    {'name': 'subject', 'type': 'text', 'label': 'Email Subject', 'required': True},
                    {'name': 'body_template', 'type': 'text', 'label': 'Email Body', 'required': True},
                    {'name': 'scheduled_date', 'type': 'text', 'label': 'Scheduled Date (YYYY-MM-DD)', 'required': True},
                    {'name': 'scheduled_time', 'type': 'text', 'label': 'Scheduled Time (HH:MM)', 'required': True, 'default': '09:00'},
                    {'name': 'timezone', 'type': 'select', 'label': 'Timezone', 'options': ['IST', 'UTC', 'EST', 'PST', 'GMT'], 'default': 'IST'},
                    {'name': 'repeat', 'type': 'select', 'label': 'Repeat', 'options': ['none', 'daily', 'weekly', 'monthly'], 'default': 'none'},
                    {'name': 'end_repeat_date', 'type': 'text', 'label': 'End Repeat Date (if repeating)', 'required': False}
                ]
            },
            {
                'id': 'email_report',
                'name': 'Email Report',
                'description': 'Configure automatic report emailing',
                'category': 'email_operations',
                'requires_column': False,
                'params': [
                    {'name': 'report_name', 'type': 'text', 'label': 'Report Name', 'required': True, 'default': 'Daily Report'},
                    {'name': 'recipients', 'type': 'text', 'label': 'Recipients (comma-separated emails)', 'required': True},
                    {'name': 'subject_template', 'type': 'text', 'label': 'Subject Template', 'default': '{Report_Name} - {Date}'},
                    {'name': 'include_summary', 'type': 'boolean', 'label': 'Include Summary in Email Body', 'default': True},
                    {'name': 'summary_columns', 'type': 'text', 'label': 'Summary Columns (comma-separated)', 'required': False},
                    {'name': 'attachment_format', 'type': 'select', 'label': 'Attachment Format', 'options': ['xlsx', 'csv', 'pdf', 'none'], 'default': 'xlsx'},
                    {'name': 'frequency', 'type': 'select', 'label': 'Frequency', 'options': ['immediate', 'daily', 'weekly', 'monthly'], 'default': 'immediate'},
                    {'name': 'send_time', 'type': 'text', 'label': 'Send Time (for scheduled)', 'default': '09:00'}
                ]
            },
            {
                'id': 'create_email_list',
                'name': 'Create Email List',
                'description': 'Extract and validate email addresses from data',
                'category': 'email_operations',
                'requires_column': True,
                'params': [
                    {'name': 'list_name', 'type': 'text', 'label': 'Email List Name', 'required': True, 'default': 'My Email List'},
                    {'name': 'include_name_column', 'type': 'column', 'label': 'Include Name Column', 'required': False},
                    {'name': 'include_company_column', 'type': 'column', 'label': 'Include Company Column', 'required': False},
                    {'name': 'validate_emails', 'type': 'boolean', 'label': 'Validate Email Format', 'default': True},
                    {'name': 'remove_duplicates', 'type': 'boolean', 'label': 'Remove Duplicate Emails', 'default': True},
                    {'name': 'filter_column', 'type': 'column', 'label': 'Filter by Column', 'required': False},
                    {'name': 'filter_value', 'type': 'text', 'label': 'Filter Value', 'required': False},
                    {'name': 'output_format', 'type': 'select', 'label': 'Output Format', 'options': ['new_column', 'new_sheet', 'csv_export'], 'default': 'new_column'},
                    {'name': 'segment_by', 'type': 'column', 'label': 'Segment List By Column', 'required': False}
                ]
            },
            {
                'id': 'track_responses',
                'name': 'Track Responses',
                'description': 'Analyze and track email response data',
                'category': 'email_operations',
                'requires_column': False,
                'params': [
                    {'name': 'email_column', 'type': 'column', 'label': 'Email Address Column', 'required': True},
                    {'name': 'sent_column', 'type': 'column', 'label': 'Sent Status Column', 'required': False},
                    {'name': 'delivered_column', 'type': 'column', 'label': 'Delivered Status Column', 'required': False},
                    {'name': 'opened_column', 'type': 'column', 'label': 'Opened Status Column', 'required': False},
                    {'name': 'clicked_column', 'type': 'column', 'label': 'Clicked Status Column', 'required': False},
                    {'name': 'replied_column', 'type': 'column', 'label': 'Replied Status Column', 'required': False},
                    {'name': 'calculate_metrics', 'type': 'boolean', 'label': 'Calculate Open/Click Rates', 'default': True},
                    {'name': 'identify_non_responders', 'type': 'boolean', 'label': 'Identify Non-Responders', 'default': True},
                    {'name': 'create_summary_sheet', 'type': 'boolean', 'label': 'Create Summary Sheet', 'default': True},
                    {'name': 'group_by_column', 'type': 'column', 'label': 'Group Results By', 'required': False}
                ]
            }
        ]
        
        # ============================================
        # CATEGORY 12: AUTOMATION OPERATIONS (12)
        # ============================================
        automation_operations = [
            {
                'id': 'run_on_file_open',
                'name': 'Run on File Open',
                'description': 'Configure workflow to run automatically when file is opened',
                'category': 'automation_operations',
                'requires_column': False,
                'params': [
                    {'name': 'trigger_name', 'type': 'text', 'label': 'Trigger Name', 'required': True, 'default': 'On File Open Trigger'},
                    {'name': 'file_pattern', 'type': 'text', 'label': 'File Pattern (e.g., *.xlsx, sales_*.csv)', 'default': '*'},
                    {'name': 'show_message', 'type': 'boolean', 'label': 'Show Welcome Message', 'default': True},
                    {'name': 'message_text', 'type': 'text', 'label': 'Welcome Message', 'default': 'Welcome! Workflow is running...'},
                    {'name': 'run_once_per_session', 'type': 'boolean', 'label': 'Run Only Once Per Session', 'default': True},
                    {'name': 'enabled', 'type': 'boolean', 'label': 'Enabled', 'default': True}
                ]
            },
            {
                'id': 'run_on_file_close',
                'name': 'Run on File Close',
                'description': 'Configure workflow to run automatically when file is closed',
                'category': 'automation_operations',
                'requires_column': False,
                'params': [
                    {'name': 'trigger_name', 'type': 'text', 'label': 'Trigger Name', 'required': True, 'default': 'On File Close Trigger'},
                    {'name': 'action_type', 'type': 'select', 'label': 'Action Type', 'options': ['auto_save', 'create_backup', 'run_cleanup', 'export_report', 'custom'], 'default': 'auto_save'},
                    {'name': 'backup_path', 'type': 'text', 'label': 'Backup/Export Path', 'default': './backups/'},
                    {'name': 'include_timestamp', 'type': 'boolean', 'label': 'Include Timestamp in Filename', 'default': True},
                    {'name': 'confirm_before_close', 'type': 'boolean', 'label': 'Confirm Before Closing', 'default': False},
                    {'name': 'enabled', 'type': 'boolean', 'label': 'Enabled', 'default': True}
                ]
            },
            {
                'id': 'run_on_cell_change',
                'name': 'Run on Cell Change',
                'description': 'Configure workflow to run when specific cell or column value changes',
                'category': 'automation_operations',
                'requires_column': True,
                'params': [
                    {'name': 'trigger_name', 'type': 'text', 'label': 'Trigger Name', 'required': True, 'default': 'On Cell Change Trigger'},
                    {'name': 'watch_type', 'type': 'select', 'label': 'Watch Type', 'options': ['specific_column', 'any_column', 'specific_cells', 'formula_results'], 'default': 'specific_column'},
                    {'name': 'action_on_change', 'type': 'select', 'label': 'Action on Change', 'options': ['recalculate', 'validate', 'update_related', 'trigger_workflow', 'log_change'], 'default': 'recalculate'},
                    {'name': 'target_workflow_id', 'type': 'text', 'label': 'Target Workflow ID (if trigger_workflow)', 'required': False},
                    {'name': 'debounce_seconds', 'type': 'number', 'label': 'Debounce Time (seconds)', 'default': 2},
                    {'name': 'enabled', 'type': 'boolean', 'label': 'Enabled', 'default': True}
                ]
            },
            {
                'id': 'scheduled_run',
                'name': 'Scheduled Run',
                'description': 'Schedule workflow to run at specific times',
                'category': 'automation_operations',
                'requires_column': False,
                'params': [
                    {'name': 'schedule_name', 'type': 'text', 'label': 'Schedule Name', 'required': True, 'default': 'Scheduled Task'},
                    {'name': 'schedule_type', 'type': 'select', 'label': 'Schedule Type', 'options': ['once', 'daily', 'weekly', 'monthly', 'hourly', 'custom_cron'], 'default': 'daily'},
                    {'name': 'run_time', 'type': 'text', 'label': 'Run Time (HH:MM)', 'required': True, 'default': '09:00'},
                    {'name': 'run_days', 'type': 'text', 'label': 'Run Days (for weekly: Mon,Tue,Wed...)', 'default': 'Mon-Fri'},
                    {'name': 'day_of_month', 'type': 'text', 'label': 'Day of Month (for monthly: 1-31 or Last)', 'default': '1'},
                    {'name': 'timezone', 'type': 'select', 'label': 'Timezone', 'options': ['IST', 'UTC', 'EST', 'PST', 'GMT', 'CET'], 'default': 'IST'},
                    {'name': 'end_date', 'type': 'text', 'label': 'End Date (YYYY-MM-DD, empty for no end)', 'required': False},
                    {'name': 'skip_holidays', 'type': 'boolean', 'label': 'Skip Holidays', 'default': False},
                    {'name': 'enabled', 'type': 'boolean', 'label': 'Enabled', 'default': True}
                ]
            },
            {
                'id': 'button_trigger',
                'name': 'Button Trigger',
                'description': 'Create a custom button to trigger workflow execution',
                'category': 'automation_operations',
                'requires_column': False,
                'params': [
                    {'name': 'button_id', 'type': 'text', 'label': 'Button ID', 'required': True, 'default': 'btn_run_workflow'},
                    {'name': 'button_label', 'type': 'text', 'label': 'Button Label', 'required': True, 'default': 'Run Workflow'},
                    {'name': 'button_style', 'type': 'select', 'label': 'Button Style', 'options': ['primary', 'secondary', 'success', 'danger', 'warning', 'info'], 'default': 'primary'},
                    {'name': 'button_icon', 'type': 'select', 'label': 'Button Icon', 'options': ['play', 'refresh', 'download', 'upload', 'check', 'cog', 'none'], 'default': 'play'},
                    {'name': 'confirm_before_run', 'type': 'boolean', 'label': 'Confirm Before Running', 'default': True},
                    {'name': 'confirmation_message', 'type': 'text', 'label': 'Confirmation Message', 'default': 'Are you sure you want to run this workflow?'},
                    {'name': 'show_result', 'type': 'boolean', 'label': 'Show Result After Run', 'default': True},
                    {'name': 'position', 'type': 'select', 'label': 'Button Position', 'options': ['toolbar', 'sidebar', 'floating', 'cell'], 'default': 'toolbar'}
                ]
            },
            {
                'id': 'keyboard_shortcut',
                'name': 'Keyboard Shortcut',
                'description': 'Assign keyboard shortcut to trigger workflow',
                'category': 'automation_operations',
                'requires_column': False,
                'params': [
                    {'name': 'shortcut_name', 'type': 'text', 'label': 'Shortcut Name', 'required': True, 'default': 'Quick Run Shortcut'},
                    {'name': 'key_combination', 'type': 'text', 'label': 'Key Combination', 'required': True, 'default': 'Ctrl+Shift+R', 'placeholder': 'Ctrl+Shift+R, Alt+F5, etc.'},
                    {'name': 'modifier_keys', 'type': 'select', 'label': 'Required Modifier', 'options': ['Ctrl', 'Shift', 'Alt', 'Ctrl+Shift', 'Ctrl+Alt', 'Alt+Shift'], 'default': 'Ctrl+Shift'},
                    {'name': 'action_key', 'type': 'text', 'label': 'Action Key (A-Z, 0-9, F1-F12)', 'required': True, 'default': 'R'},
                    {'name': 'show_tooltip', 'type': 'boolean', 'label': 'Show Shortcut in Tooltip', 'default': True},
                    {'name': 'global_shortcut', 'type': 'boolean', 'label': 'Global Shortcut (works when app not focused)', 'default': False},
                    {'name': 'enabled', 'type': 'boolean', 'label': 'Enabled', 'default': True}
                ]
            },
            {
                'id': 'loop_repeat',
                'name': 'Loop/Repeat',
                'description': 'Configure workflow to run repeatedly on multiple items',
                'category': 'automation_operations',
                'requires_column': False,
                'params': [
                    {'name': 'loop_name', 'type': 'text', 'label': 'Loop Name', 'required': True, 'default': 'Batch Process Loop'},
                    {'name': 'loop_type', 'type': 'select', 'label': 'Loop Type', 'options': ['files_in_folder', 'sheets_in_workbook', 'rows_in_sheet', 'items_in_list', 'date_range'], 'default': 'files_in_folder'},
                    {'name': 'source_path', 'type': 'text', 'label': 'Source Path/Pattern', 'default': './data/*.xlsx'},
                    {'name': 'file_pattern', 'type': 'text', 'label': 'File Pattern (for folder loop)', 'default': '*.xlsx'},
                    {'name': 'batch_size', 'type': 'number', 'label': 'Batch Size', 'default': 10},
                    {'name': 'parallel_execution', 'type': 'boolean', 'label': 'Parallel Execution', 'default': False},
                    {'name': 'max_iterations', 'type': 'number', 'label': 'Max Iterations (0 for unlimited)', 'default': 0},
                    {'name': 'continue_on_error', 'type': 'boolean', 'label': 'Continue on Error', 'default': True},
                    {'name': 'output_folder', 'type': 'text', 'label': 'Output Folder', 'default': './processed/'},
                    {'name': 'delay_between_iterations', 'type': 'number', 'label': 'Delay Between Iterations (seconds)', 'default': 1}
                ]
            },
            {
                'id': 'error_handling',
                'name': 'Error Handling',
                'description': 'Configure error handling behavior for workflow',
                'category': 'automation_operations',
                'requires_column': False,
                'params': [
                    {'name': 'handler_name', 'type': 'text', 'label': 'Handler Name', 'required': True, 'default': 'Error Handler'},
                    {'name': 'on_error_action', 'type': 'select', 'label': 'On Error Action', 'options': ['abort', 'continue', 'retry', 'skip_row', 'run_fallback', 'log_and_continue'], 'default': 'log_and_continue'},
                    {'name': 'retry_count', 'type': 'number', 'label': 'Retry Count', 'default': 3},
                    {'name': 'retry_delay_seconds', 'type': 'number', 'label': 'Retry Delay (seconds)', 'default': 5},
                    {'name': 'fallback_workflow_id', 'type': 'text', 'label': 'Fallback Workflow ID', 'required': False},
                    {'name': 'log_errors', 'type': 'boolean', 'label': 'Log Errors', 'default': True},
                    {'name': 'log_level', 'type': 'select', 'label': 'Log Level', 'options': ['debug', 'info', 'warning', 'error', 'critical'], 'default': 'error'},
                    {'name': 'notify_on_error', 'type': 'boolean', 'label': 'Send Notification on Error', 'default': True},
                    {'name': 'notification_email', 'type': 'text', 'label': 'Notification Email', 'required': False},
                    {'name': 'error_message_template', 'type': 'text', 'label': 'Custom Error Message', 'default': 'Workflow encountered an error: {error_message}'}
                ]
            },
            {
                'id': 'user_input',
                'name': 'User Input',
                'description': 'Prompt user for input before workflow execution',
                'category': 'automation_operations',
                'requires_column': False,
                'params': [
                    {'name': 'input_name', 'type': 'text', 'label': 'Input Name', 'required': True, 'default': 'User Input Prompt'},
                    {'name': 'input_type', 'type': 'select', 'label': 'Input Type', 'options': ['text', 'number', 'date', 'date_range', 'dropdown', 'multi_select', 'file_picker', 'folder_picker', 'confirm'], 'default': 'text'},
                    {'name': 'prompt_text', 'type': 'text', 'label': 'Prompt Text', 'required': True, 'default': 'Please enter value:'},
                    {'name': 'default_value', 'type': 'text', 'label': 'Default Value', 'required': False},
                    {'name': 'options', 'type': 'text', 'label': 'Options (for dropdown, comma-separated)', 'required': False, 'placeholder': 'Option1,Option2,Option3'},
                    {'name': 'required', 'type': 'boolean', 'label': 'Required', 'default': True},
                    {'name': 'validation_pattern', 'type': 'text', 'label': 'Validation Pattern (regex)', 'required': False},
                    {'name': 'min_value', 'type': 'text', 'label': 'Min Value (for number/date)', 'required': False},
                    {'name': 'max_value', 'type': 'text', 'label': 'Max Value (for number/date)', 'required': False},
                    {'name': 'store_as_variable', 'type': 'text', 'label': 'Store as Variable Name', 'default': 'user_input_value'}
                ]
            },
            {
                'id': 'progress_bar',
                'name': 'Progress Bar',
                'description': 'Show progress indicator during workflow execution',
                'category': 'automation_operations',
                'requires_column': False,
                'params': [
                    {'name': 'progress_name', 'type': 'text', 'label': 'Progress Name', 'required': True, 'default': 'Workflow Progress'},
                    {'name': 'display_type', 'type': 'select', 'label': 'Display Type', 'options': ['bar', 'percentage', 'spinner', 'steps', 'detailed'], 'default': 'bar'},
                    {'name': 'show_percentage', 'type': 'boolean', 'label': 'Show Percentage', 'default': True},
                    {'name': 'show_time_remaining', 'type': 'boolean', 'label': 'Show Estimated Time', 'default': True},
                    {'name': 'show_current_step', 'type': 'boolean', 'label': 'Show Current Step Name', 'default': True},
                    {'name': 'show_rows_processed', 'type': 'boolean', 'label': 'Show Rows Processed', 'default': True},
                    {'name': 'allow_cancel', 'type': 'boolean', 'label': 'Allow Cancel', 'default': True},
                    {'name': 'auto_close', 'type': 'boolean', 'label': 'Auto Close on Complete', 'default': True},
                    {'name': 'auto_close_delay', 'type': 'number', 'label': 'Auto Close Delay (seconds)', 'default': 3},
                    {'name': 'position', 'type': 'select', 'label': 'Position', 'options': ['center', 'top', 'bottom', 'corner'], 'default': 'center'}
                ]
            },
            {
                'id': 'log_activity',
                'name': 'Log Activity',
                'description': 'Record workflow execution activity and audit trail',
                'category': 'automation_operations',
                'requires_column': False,
                'params': [
                    {'name': 'log_name', 'type': 'text', 'label': 'Log Name', 'required': True, 'default': 'Workflow Activity Log'},
                    {'name': 'log_destination', 'type': 'select', 'label': 'Log Destination', 'options': ['file', 'database', 'console', 'sheet', 'all'], 'default': 'file'},
                    {'name': 'log_file_path', 'type': 'text', 'label': 'Log File Path', 'default': './logs/workflow_activity.log'},
                    {'name': 'log_level', 'type': 'select', 'label': 'Log Level', 'options': ['debug', 'info', 'warning', 'error'], 'default': 'info'},
                    {'name': 'include_timestamp', 'type': 'boolean', 'label': 'Include Timestamp', 'default': True},
                    {'name': 'include_user', 'type': 'boolean', 'label': 'Include User Info', 'default': True},
                    {'name': 'include_duration', 'type': 'boolean', 'label': 'Include Duration', 'default': True},
                    {'name': 'include_row_count', 'type': 'boolean', 'label': 'Include Rows Processed', 'default': True},
                    {'name': 'log_format', 'type': 'select', 'label': 'Log Format', 'options': ['text', 'json', 'csv'], 'default': 'json'},
                    {'name': 'max_log_size_mb', 'type': 'number', 'label': 'Max Log Size (MB)', 'default': 10},
                    {'name': 'rotate_logs', 'type': 'boolean', 'label': 'Rotate Logs', 'default': True}
                ]
            },
            {
                'id': 'chain_macros',
                'name': 'Chain Macros',
                'description': 'Chain multiple workflows to run in sequence',
                'category': 'automation_operations',
                'requires_column': False,
                'params': [
                    {'name': 'chain_name', 'type': 'text', 'label': 'Chain Name', 'required': True, 'default': 'Workflow Chain'},
                    {'name': 'workflow_sequence', 'type': 'text', 'label': 'Workflow IDs (comma-separated)', 'required': True, 'placeholder': 'WF001,WF002,WF003'},
                    {'name': 'pass_data_between', 'type': 'boolean', 'label': 'Pass Data Between Workflows', 'default': True},
                    {'name': 'on_step_failure', 'type': 'select', 'label': 'On Step Failure', 'options': ['abort_chain', 'skip_step', 'retry_step', 'run_error_handler'], 'default': 'abort_chain'},
                    {'name': 'delay_between_steps', 'type': 'number', 'label': 'Delay Between Steps (seconds)', 'default': 2},
                    {'name': 'timeout_per_step_minutes', 'type': 'number', 'label': 'Timeout Per Step (minutes)', 'default': 30},
                    {'name': 'notify_on_complete', 'type': 'boolean', 'label': 'Notify on Chain Complete', 'default': True},
                    {'name': 'save_intermediate_results', 'type': 'boolean', 'label': 'Save Intermediate Results', 'default': False},
                    {'name': 'rollback_on_failure', 'type': 'boolean', 'label': 'Rollback All on Failure', 'default': False},
                    {'name': 'summary_report', 'type': 'boolean', 'label': 'Generate Summary Report', 'default': True}
                ]
            }
        ]
        
        # Combine all operations
        all_operations.extend(cleanup_operations)
        all_operations.extend(validation_operations)
        all_operations.extend(text_operations)
        all_operations.extend(number_operations)
        all_operations.extend(date_operations)
        all_operations.extend(row_column_operations)
        all_operations.extend(formatting_operations)
        all_operations.extend(file_operations)
        all_operations.extend(sheet_operations)
        all_operations.extend(chart_report_operations)
        all_operations.extend(email_operations)
        all_operations.extend(automation_operations)
        
        return all_operations
    
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
            
            # Send completion notification
            try:
                from notifications.tasks import send_workflow_completion_email
                send_workflow_completion_email.delay(str(run.id))
            except ImportError:
                logger.warning("Notifications app not available")
            
        except Exception as e:
            logger.error(f"Workflow execution failed: {str(e)}")
            run.status = 'failed'
            run.completed_at = timezone.now()
            run.error_message = str(e)
            run.results = make_json_serializable(results)
            run.save()
            
            # Send failure notification
            try:
                from notifications.tasks import send_workflow_failure_email
                send_workflow_failure_email.delay(str(run.id), str(e))
            except ImportError:
                logger.warning("Notifications app not available")
        
        return run
    
    def _apply_operation(self, df, operation_name, column, params):
        """Apply a single cleanup operation to the DataFrame"""
        
        rows_before = len(df)
        
        # ============================================
        # CLEANUP OPERATIONS
        # ============================================
        
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
        
        elif operation_name == 'fill_empty' or operation_name == 'fill_empty_with_value':
            if column and column in df.columns:
                fill_value = params.get('fill_value', params.get('value', ''))
                filled = df[column].isna().sum()
                df[column] = df[column].fillna(fill_value)
                return df, f"Filled {filled} empty cells with '{fill_value}'"
            return df, "Column not found for fill value operation"
        
        elif operation_name == 'standardize_case':
            if column and column in df.columns:
                case_type = params.get('case_type', 'lower')
                if case_type == 'upper':
                    df[column] = df[column].astype(str).str.upper()
                elif case_type == 'lower':
                    df[column] = df[column].astype(str).str.lower()
                elif case_type == 'title':
                    df[column] = df[column].astype(str).str.title()
                return df, f"Converted '{column}' to {case_type} case"
            return df, "Column not found for standardize case operation"
        
        # ============================================
        # TEXT TRANSFORMATION OPERATIONS
        # ============================================
        
        elif operation_name == 'uppercase' or operation_name == 'convert_uppercase':
            if column and column in df.columns:
                df[column] = df[column].astype(str).str.upper()
                return df, f"Converted '{column}' to uppercase"
            return df, "Column not found for uppercase operation"
        
        elif operation_name == 'lowercase' or operation_name == 'convert_lowercase':
            if column and column in df.columns:
                df[column] = df[column].astype(str).str.lower()
                return df, f"Converted '{column}' to lowercase"
            return df, "Column not found for lowercase operation"
        
        elif operation_name == 'titlecase' or operation_name == 'convert_titlecase':
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
        
        elif operation_name == 'find_and_replace' or operation_name == 'find_replace':
            if column and column in df.columns:
                find_val = params.get('find_value', params.get('find', ''))
                replace_val = params.get('replace_value', params.get('replace', ''))
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
                start = int(params.get('start', 0))
                length = int(params.get('length', 10))
                df[column] = df[column].astype(str).str[start:start+length]
                return df, f"Extracted text from '{column}' (start: {start}, length: {length})"
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
            if isinstance(columns_to_merge, str):
                columns_to_merge = [c.strip() for c in columns_to_merge.split(',')]
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
        # NUMBER OPERATIONS
        # ============================================
        
        elif operation_name == 'round_numbers' or operation_name == 'round_number':
            if column and column in df.columns:
                decimal_places = int(params.get('decimal_places', params.get('decimals', 2)))
                df[column] = pd.to_numeric(df[column], errors='coerce').round(decimal_places)
                return df, f"Rounded '{column}' to {decimal_places} decimal places"
            return df, "Column not found for round numbers operation"
        
        elif operation_name == 'format_currency':
            if column and column in df.columns:
                symbol = params.get('currency_symbol', params.get('symbol', '₹'))
                decimals = int(params.get('decimal_places', 2))
                numeric_col = pd.to_numeric(df[column], errors='coerce')
                df[column] = numeric_col.apply(
                    lambda x: f"{symbol}{x:,.{decimals}f}" if pd.notna(x) else ''
                )
                return df, f"Formatted '{column}' as currency with {symbol}"
            return df, "Column not found for format currency operation"
        
        elif operation_name == 'format_percentage' or operation_name == 'calculate_percentage':
            if column and column in df.columns:
                decimals = int(params.get('decimal_places', 2))
                numeric_col = pd.to_numeric(df[column], errors='coerce')
                df[column] = numeric_col.apply(
                    lambda x: f"{(x * 100):.{decimals}f}%" if pd.notna(x) and abs(x) <= 1 
                    else f"{x:.{decimals}f}%" if pd.notna(x) else ''
                )
                return df, f"Formatted '{column}' as percentage"
            return df, "Column not found for format percentage operation"
        
        elif operation_name == 'add_subtract_value':
            if column and column in df.columns:
                math_op = params.get('operation', 'add')
                value = float(params.get('value', 0))
                numeric_col = pd.to_numeric(df[column], errors='coerce')
                if math_op == 'add':
                    df[column] = numeric_col + value
                    return df, f"Added {value} to '{column}'"
                else:
                    df[column] = numeric_col - value
                    return df, f"Subtracted {value} from '{column}'"
            return df, "Column not found for add/subtract operation"
        
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
                else:
                    df[f'{column}_sum'] = total
                    return df, f"Added sum ({total}) as new column"
            return df, "Column not found for calculate sum operation"
        
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
                else:
                    df[f'{column}_avg'] = avg
                    return df, f"Added average ({avg}) as new column"
            return df, "Column not found for calculate average operation"
        
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
                else:
                    df[f'{column}_{math_op}'] = result
                    return df, f"Added {label.lower()} ({result}) as new column"
            return df, "Column not found for find min/max operation"
        
        elif operation_name == 'remove_decimals':
            if column and column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').apply(
                    lambda x: int(x) if pd.notna(x) else x
                )
                return df, f"Removed decimals from '{column}'"
            return df, "Column not found for remove decimals operation"
        
        elif operation_name == 'negative_to_positive':
            if column and column in df.columns:
                df[column] = pd.to_numeric(df[column], errors='coerce').abs()
                return df, f"Converted negative values to positive in '{column}'"
            return df, "Column not found for negative to positive operation"
        
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
        
        elif operation_name == 'generate_sequence':
            start = int(params.get('start_value', 1))
            step = int(params.get('step', 1))
            col_name = params.get('column_name', 'sequence')
            df[col_name] = range(start, start + len(df) * step, step)
            return df, f"Generated sequence in '{col_name}' starting from {start} with step {step}"
        
        # ============================================
        # DATE/TIME OPERATIONS
        # ============================================
        
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
        
        elif operation_name == 'extract_year':
            if column and column in df.columns:
                output_col = params.get('output_column', f'{column}_year')
                if not output_col:
                    output_col = f'{column}_year'
                
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
        
        elif operation_name == 'extract_month':
            if column and column in df.columns:
                output_type = params.get('output_type', 'name')
                output_col = params.get('output_column', f'{column}_month')
                if not output_col:
                    output_col = f'{column}_month'
                
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
        
        elif operation_name == 'extract_day':
            if column and column in df.columns:
                output_col = params.get('output_column', f'{column}_day')
                if not output_col:
                    output_col = f'{column}_day'
                
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
        
        elif operation_name == 'find_day_of_week':
            if column and column in df.columns:
                output_col = params.get('output_column', f'{column}_weekday')
                if not output_col:
                    output_col = f'{column}_weekday'
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
        
        elif operation_name == 'quarter_calculation':
            if column and column in df.columns:
                output_col = params.get('output_column', f'{column}_quarter')
                if not output_col:
                    output_col = f'{column}_quarter'
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
        
        # ============================================
        # ROW/COLUMN OPERATIONS
        # ============================================

        elif operation_name == 'insert_rows':
            position = params.get('position', 'at_end')
            interval = int(params.get('interval', 5))
            count = int(params.get('count', 1))
            
            if position == 'after_every_n':
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
            position = params.get('position', 'at_end')
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
            condition = params.get('condition', 'blank_rows')
            original_count = len(df)
            
            if condition == 'blank_rows':
                df = df.dropna(how='all').reset_index(drop=True)
                df = df[~(df.astype(str).apply(lambda x: x.str.strip() == '').all(axis=1))].reset_index(drop=True)
            
            elif condition == 'duplicate_rows':
                subset_cols = params.get('columns', None)
                keep = params.get('keep', 'first')
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
            condition = params.get('condition', 'by_name')
            
            if condition == 'empty_columns':
                empty_cols = [col for col in df.columns if df[col].isna().all() or (df[col].astype(str).str.strip() == '').all()]
                df = df.drop(columns=empty_cols)
                return df, f"Deleted {len(empty_cols)} empty columns: {empty_cols}"
            
            elif condition == 'by_name':
                columns_to_delete = params.get('columns', [])
                if isinstance(columns_to_delete, str):
                    columns_to_delete = [c.strip() for c in columns_to_delete.split(',')]
                
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
            target = params.get('target', 'all')
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
            
            df = df.sort_values(by=group_by).reset_index(drop=True)
            
            df['_group'] = df[group_by]
            df['_group_level'] = 1
            
            if add_subtotals and subtotal_columns:
                if isinstance(subtotal_columns, str):
                    subtotal_columns = [c.strip() for c in subtotal_columns.split(',')]
                
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
                
                for idx, row in sorted(subtotals, key=lambda x: x[0], reverse=True):
                    df = pd.concat([df.iloc[:int(idx)+1], pd.DataFrame([row]), df.iloc[int(idx)+1:]]).reset_index(drop=True)
            
            return df, f"Grouped by '{group_by}'" + (f" with subtotals on {subtotal_columns}" if add_subtotals else "")

        elif operation_name == 'freeze_panes':
            freeze_rows = int(params.get('freeze_rows', 1))
            freeze_columns = int(params.get('freeze_columns', 0))
            
            df['_freeze_panes'] = f"rows:{freeze_rows},cols:{freeze_columns}"
            
            return df, f"Set freeze panes: {freeze_rows} row(s), {freeze_columns} column(s) (applied on Excel export)"
        
        # ============================================
        # VALIDATION OPERATIONS
        # ============================================
        
        elif operation_name == 'validate_email':
            col = params.get('column') or params.get('emailColumn') or params.get('email_column') or column
            
            if not col:
                raise ValueError(f"No column specified for validate_email. Params: {params}, Column arg: {column}")
            
            if col not in df.columns:
                cols_lower = {c.lower(): c for c in df.columns}
                if col.lower() in cols_lower:
                    col = cols_lower[col.lower()]
                else:
                    raise ValueError(f"Column '{col}' not found. Available: {list(df.columns)}")
            
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
        
        elif operation_name == 'validate_phone':
            column = params.get('column') or column
            country_code = params.get('country_code', 'IN')
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.validate_phone(df, column, country_code, add_col)
            return df, f"Phone validation completed: {validation_results['valid_phones']} valid, {validation_results['invalid_phones']} invalid"
            
        elif operation_name == 'validate_date':
            column = params.get('column') or column
            date_format = params.get('date_format', 'auto')
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.validate_date(df, column, date_format, add_col)
            return df, f"Date validation completed: {validation_results['valid_dates']} valid, {validation_results['invalid_dates']} invalid"
        
        elif operation_name == 'validate_number':
            if column and column in df.columns:
                status_column = f"{column}_validation"
                df[status_column] = df[column].apply(
                    lambda x: 'VALID' if pd.notna(x) and str(x).replace('.', '', 1).replace('-', '', 1).isdigit()
                    else ('BLANK' if pd.isna(x) or str(x).strip() == '' else 'INVALID')
                )
                valid_count = (df[status_column] == 'VALID').sum()
                invalid_count = (df[status_column] == 'INVALID').sum()
                return df, f"Number validation completed: {valid_count} valid, {invalid_count} invalid"
            return df, "Column not found for validate number operation"
            
        elif operation_name == 'check_for_blanks':
            column = params.get('column') or column
            action = params.get('action', 'flag')
            fill_value = params.get('fill_value')
            df, validation_results = DataValidationOperations.check_for_blanks(df, column, action, fill_value)
            return df, f"Blank check completed: {validation_results['blank_cells']} blank cells found"
            
        elif operation_name == 'check_data_type':
            column = params.get('column') or column
            expected_type = params.get('expected_type', 'number')
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.check_data_type(df, column, expected_type, add_col)
            return df, f"Data type check completed: {validation_results['valid_type']} valid, {validation_results['invalid_type']} invalid"
            
        elif operation_name == 'validate_range':
            column = params.get('column') or column
            min_value = params.get('min_value')
            max_value = params.get('max_value')
            if min_value is not None:
                min_value = float(min_value)
            if max_value is not None:
                max_value = float(max_value)
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.validate_range(df, column, min_value, max_value, add_col)
            return df, f"Range validation completed: {validation_results['valid_in_range']} in range, {validation_results['below_minimum']} below min, {validation_results['above_maximum']} above max"
            
        elif operation_name == 'check_duplicates':
            column = params.get('column') or column
            action = params.get('action', 'flag')
            keep = params.get('keep', 'first')
            df, validation_results = DataValidationOperations.check_duplicates(df, column, action, keep)
            return df, f"Duplicate check completed: {validation_results['duplicate_rows']} duplicates found"
            
        elif operation_name == 'validate_length':
            column = params.get('column') or column
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
            return df, f"Length validation completed: {validation_results['valid_length']} valid, {validation_results['too_short']} too short, {validation_results['too_long']} too long"
            
        elif operation_name == 'check_required_fields':
            columns = params.get('columns', [])
            if isinstance(columns, str):
                columns = [c.strip() for c in columns.split(',')]
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.check_required_fields(df, columns, add_col)
            return df, f"Required fields check completed: {validation_results['complete_rows']} complete rows, {validation_results['incomplete_rows']} incomplete"
            
        elif operation_name == 'validate_pan_aadhaar':
            column = params.get('column') or column
            id_type = params.get('id_type', 'PAN')
            add_col = params.get('add_validation_column', True)
            df, validation_results = DataValidationOperations.validate_pan_aadhaar(df, column, id_type, add_col)
            return df, f"{id_type} validation completed: {validation_results['valid_ids']} valid, {validation_results['invalid_ids']} invalid"
            
        elif operation_name == 'highlight_errors':
            column = params.get('column') or column
            error_type = params.get('error_type', 'any')
            create_col = params.get('create_error_column', True)
            df, validation_results = DataValidationOperations.highlight_errors(df, column, error_type, create_col)
            return df, f"Error highlighting completed: {validation_results['cells_with_errors']} errors found"
            
        elif operation_name == 'create_error_report':
            columns = params.get('columns')
            if isinstance(columns, str):
                columns = [c.strip() for c in columns.split(',')]
            output_column = params.get('output_column', 'error_summary')
            df, validation_results = DataValidationOperations.create_error_report(df, columns, output_column)
            return df, f"Error report created: {validation_results['rows_with_errors']} rows with errors"
        
        # ============================================
        # FORMATTING OPERATIONS
        # ============================================

        elif operation_name == 'autofit_column_width':
            self._formatting_ops.append({
                'type': 'autofit_column_width',
                'column': column or 'all'
            })
            self._requires_excel_output = True
            return df, f"Auto-fit column width scheduled for '{column or 'all columns'}'"

        elif operation_name == 'autofit_row_height':
            row_range = params.get('row_range', 'all')
            self._formatting_ops.append({
                'type': 'autofit_row_height',
                'row_range': row_range
            })
            self._requires_excel_output = True
            return df, f"Auto-fit row height scheduled for '{row_range}'"

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

        elif operation_name == 'add_borders':
            border_style = params.get('border_style', 'thin')
            self._formatting_ops.append({
                'type': 'add_borders',
                'column': column or 'all',
                'border_style': border_style
            })
            self._requires_excel_output = True
            return df, f"Borders '{border_style}' scheduled for '{column or 'all columns'}'"

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

        elif operation_name == 'clear_formatting':
            self._formatting_ops.append({
                'type': 'clear_formatting',
                'column': column or 'all'
            })
            self._requires_excel_output = True
            return df, f"Clear formatting scheduled for '{column or 'all columns'}'"

        elif operation_name == 'highlight_cells':
            if column and column in df.columns:
                color = params.get('color', 'yellow')
                self._formatting_ops.append({
                    'type': 'apply_cell_color',
                    'column': column,
                    'color': color,
                    'row_condition': 'all'
                })
                self._requires_excel_output = True
                return df, f"Highlight cells with '{color}' scheduled for '{column}'"
            return df, "Column not found for highlight cells operation"
        
        # ============================================
        # FILE OPERATIONS
        # ============================================
        
        elif operation_name == 'import_csv':
            source_path = params.get('source_path', '')
            encoding = params.get('encoding', 'utf-8')
            delimiter = params.get('delimiter', ',')
            
            if source_path and os.path.exists(source_path):
                imported_df = pd.read_csv(source_path, encoding=encoding, delimiter=delimiter)
                df = pd.concat([df, imported_df], ignore_index=True)
                message = f"Imported {len(imported_df)} rows from CSV file"
            else:
                message = "Source file not found - operation skipped"
            return df, message
        
        elif operation_name == 'import_text':
            source_path = params.get('source_path', '')
            delimiter = params.get('delimiter', '\t')
            has_header = params.get('has_header', True)
            
            if source_path and os.path.exists(source_path):
                header = 0 if has_header else None
                imported_df = pd.read_csv(source_path, delimiter=delimiter, header=header)
                df = pd.concat([df, imported_df], ignore_index=True)
                message = f"Imported {len(imported_df)} rows from text file"
            else:
                message = "Source file not found - operation skipped"
            return df, message
        
        elif operation_name == 'export_csv':
            output_name = params.get('output_name', 'exported_data')
            delimiter = params.get('delimiter', ',')
            include_header = params.get('include_header', True)
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            df.attrs['export_csv'] = {
                'name': output_name,
                'delimiter': delimiter,
                'header': include_header
            }
            return df, f"Marked for CSV export as '{output_name}.csv'"
        
        elif operation_name == 'export_pdf':
            output_name = params.get('output_name', 'exported_data')
            orientation = params.get('orientation', 'portrait')
            page_size = params.get('page_size', 'A4')
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            df.attrs['export_pdf'] = {
                'name': output_name,
                'orientation': orientation,
                'page_size': page_size
            }
            return df, f"Marked for PDF export as '{output_name}.pdf'"
        
        elif operation_name == 'combine_files':
            dataset_ids = params.get('dataset_ids', '')
            merge_type = params.get('merge_type', 'append_rows')
            join_column = column if column else params.get('join_column', '')
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            df.attrs['combine_pending'] = {
                'dataset_ids': dataset_ids.split(',') if dataset_ids else [],
                'merge_type': merge_type,
                'join_column': join_column
            }
            return df, f"Combine files instruction stored (merge_type: {merge_type})"
        
        elif operation_name == 'split_file':
            split_by = params.get('split_by', 'row_count')
            rows_per_file = int(params.get('rows_per_file', 100))
            split_column = column if column else params.get('split_column', '')
            
            if split_by == 'row_count':
                num_splits = (len(df) + rows_per_file - 1) // rows_per_file
                message = f"Dataset will be split into {num_splits} files ({rows_per_file} rows each)"
            elif split_by == 'column_value' and split_column and split_column in df.columns:
                unique_values = df[split_column].nunique()
                message = f"Dataset will be split into {unique_values} files by '{split_column}' values"
            else:
                message = "Split configuration saved"
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            df.attrs['split_config'] = {
                'split_by': split_by,
                'rows_per_file': rows_per_file,
                'split_column': split_column
            }
            return df, message
        
        elif operation_name == 'batch_rename':
            add_prefix = params.get('add_prefix', '')
            add_suffix = params.get('add_suffix', '')
            replace_spaces = params.get('replace_spaces', False)
            to_lowercase = params.get('to_lowercase', False)
            
            new_columns = {}
            for col in df.columns:
                new_name = col
                if replace_spaces:
                    new_name = new_name.replace(' ', '_')
                if to_lowercase:
                    new_name = new_name.lower()
                new_name = f"{add_prefix}{new_name}{add_suffix}"
                new_columns[col] = new_name
            
            df = df.rename(columns=new_columns)
            return df, f"Renamed {len(new_columns)} columns"
        
        elif operation_name == 'auto_save':
            checkpoint_name = params.get('checkpoint_name', 'auto_checkpoint')
            include_timestamp = params.get('include_timestamp', True)
            
            from datetime import datetime
            if include_timestamp:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                checkpoint_name = f"{checkpoint_name}_{timestamp}"
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            df.attrs['checkpoint_name'] = checkpoint_name
            return df, f"Checkpoint created: {checkpoint_name}"
        
        elif operation_name == 'create_backup':
            backup_name = params.get('backup_name', 'backup')
            include_date = params.get('include_date', True)
            include_time = params.get('include_time', False)
            
            from datetime import datetime
            suffix = ''
            if include_date:
                suffix += datetime.now().strftime('_%Y%m%d')
            if include_time:
                suffix += datetime.now().strftime('_%H%M%S')
            
            backup_filename = f"{backup_name}{suffix}"
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            df.attrs['backup_name'] = backup_filename
            return df, f"Backup will be created: {backup_filename}"
        
        elif operation_name == 'print_setup':
            orientation = params.get('orientation', 'portrait')
            margins = params.get('margins', 'normal')
            fit_to_page = params.get('fit_to_page', True)
            repeat_header = params.get('repeat_header', True)
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            df.attrs['print_setup'] = {
                'orientation': orientation,
                'margins': margins,
                'fit_to_page': fit_to_page,
                'repeat_header': repeat_header
            }
            return df, f"Print setup configured: {orientation}, {margins} margins"
        
        elif operation_name == 'save_template':
            template_name = params.get('template_name', 'template')
            include_sample = params.get('include_sample_data', False)
            description = params.get('description', '')
            
            template_info = {
                'name': template_name,
                'columns': list(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'description': description
            }
            if include_sample:
                template_info['sample_data'] = df.head(5).to_dict('records')
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            df.attrs['template'] = template_info
            return df, f"Template '{template_name}' configuration saved"
        
        elif operation_name == 'batch_export':
            export_csv = params.get('export_csv', True)
            export_excel = params.get('export_excel', True)
            export_pdf = params.get('export_pdf', False)
            output_prefix = params.get('output_prefix', 'export')
            
            exports = []
            if export_csv:
                exports.append('CSV')
            if export_excel:
                exports.append('Excel')
            if export_pdf:
                exports.append('PDF')
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            df.attrs['batch_export'] = {
                'prefix': output_prefix,
                'formats': exports
            }
            return df, f"Batch export configured: {', '.join(exports)}"
        
        # ============================================
        # SHEET OPERATIONS
        # ============================================
        
        elif operation_name == 'add_sheet':
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
            return df, f"New sheet '{sheet_name}' will be created at {position}"
        
        elif operation_name == 'delete_sheet':
            sheet_name = params.get('sheet_name', '')
            confirm_delete = params.get('confirm_delete', True)
            
            if not sheet_name:
                return df, "No sheet name specified"
            elif not confirm_delete:
                return df, "Deletion not confirmed - operation skipped"
            else:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['delete_sheet'] = {'name': sheet_name}
                return df, f"Sheet '{sheet_name}' marked for deletion"
        
        elif operation_name == 'rename_sheet':
            old_name = params.get('old_name', '')
            new_name = params.get('new_name', '')
            
            if old_name and new_name:
                if not hasattr(df, 'attrs'):
                    df.attrs = {}
                df.attrs['rename_sheet'] = {'old_name': old_name, 'new_name': new_name}
                return df, f"Sheet '{old_name}' will be renamed to '{new_name}'"
            else:
                return df, "Both old and new names are required"
        
        elif operation_name == 'copy_sheet':
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
                return df, f"Sheet '{source_sheet}' will be copied as '{new_name}'"
            else:
                return df, "Source sheet name is required"
        
        elif operation_name == 'move_sheet':
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
                return df, f"Sheet '{sheet_name}' will be moved to {new_position}"
            else:
                return df, "Sheet name is required"
        
        elif operation_name == 'hide_sheet':
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
                return df, f"Sheet '{sheet_name}' will be {action}"
            else:
                return df, "Sheet name is required"
        
        elif operation_name == 'protect_sheet':
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
                return df, f"Sheet '{sheet_name}' will be {action}"
            else:
                return df, "Sheet name is required"
        
        elif operation_name == 'compare_sheets':
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
                return df, f"Comparison between '{sheet1_name}' and '{sheet2_name}' configured (results to '{output_sheet}')"
            else:
                return df, "Both sheet names are required for comparison"
        
        elif operation_name == 'merge_sheets':
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
                return df, f"Will merge {len(sheet_names)} sheets into '{target_sheet}' ({merge_type})"
            else:
                return df, "At least one sheet name is required"
        
        elif operation_name == 'create_index':
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
            return df, f"Index sheet '{index_sheet_name}' will be created at {position}"
        
        elif operation_name == 'link_sheets':
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
                return df, f"Link created: {source_sheet}!{source_range} → {target_sheet}!{target_start_cell} ({link_type})"
            else:
                return df, "Source and target sheet names are required"
        
        elif operation_name == 'copy_to_file':
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
                return df, f"Sheet '{sheet_name}' will be copied to dataset {target_dataset_id} as '{new_sheet_name}'"
            else:
                return df, "Sheet name and target dataset ID are required"
        
        # ============================================
        # CHART & REPORT OPERATIONS
        # ============================================
        
        elif operation_name == 'create_bar_chart':
            chart_title = params.get('chart_title', 'Bar Chart')
            x_axis_column = params.get('x_axis_column', '')
            y_axis_columns_str = params.get('y_axis_columns', '')
            chart_style = params.get('chart_style', 'clustered')
            show_legend = params.get('show_legend', True)
            show_data_labels = params.get('show_data_labels', False)
            
            y_columns = [col.strip() for col in y_axis_columns_str.split(',') if col.strip()]
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['create_bar_chart'] = {
                'title': chart_title,
                'x_axis': x_axis_column,
                'y_axis': y_columns,
                'style': chart_style,
                'legend': show_legend,
                'data_labels': show_data_labels,
                'data_preview': df[[x_axis_column] + [c for c in y_columns if c in df.columns]].head(10).to_dict() if x_axis_column in df.columns else {}
            }
            return df, f"Bar chart '{chart_title}' configuration saved with {len(y_columns)} data series"
        
        elif operation_name == 'create_pie_chart':
            chart_title = params.get('chart_title', 'Pie Chart')
            labels_column = params.get('labels_column', '')
            values_column = params.get('values_column', '')
            chart_type = params.get('chart_type', 'pie')
            show_percentages = params.get('show_percentages', True)
            show_values = params.get('show_values', False)
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            # Calculate percentages if data exists
            pie_data = {}
            if labels_column in df.columns and values_column in df.columns:
                total = df[values_column].sum()
                pie_data = {
                    'labels': df[labels_column].tolist(),
                    'values': df[values_column].tolist(),
                    'percentages': [(v/total*100) for v in df[values_column]] if total > 0 else []
                }
            
            df.attrs['create_pie_chart'] = {
                'title': chart_title,
                'labels_column': labels_column,
                'values_column': values_column,
                'chart_type': chart_type,
                'show_percentages': show_percentages,
                'show_values': show_values,
                'data': pie_data
            }
            return df, f"Pie chart '{chart_title}' configuration saved ({chart_type} type)"
        
        elif operation_name == 'create_line_chart':
            chart_title = params.get('chart_title', 'Line Chart')
            x_axis_column = params.get('x_axis_column', '')
            y_axis_columns_str = params.get('y_axis_columns', '')
            line_style = params.get('line_style', 'straight')
            show_markers = params.get('show_markers', True)
            show_gridlines = params.get('show_gridlines', True)
            
            y_columns = [col.strip() for col in y_axis_columns_str.split(',') if col.strip()]
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['create_line_chart'] = {
                'title': chart_title,
                'x_axis': x_axis_column,
                'y_axis': y_columns,
                'line_style': line_style,
                'markers': show_markers,
                'gridlines': show_gridlines
            }
            return df, f"Line chart '{chart_title}' configuration saved with {len(y_columns)} trend lines"
        
        elif operation_name == 'update_chart_data':
            chart_name = params.get('chart_name', '')
            new_data_range = params.get('new_data_range', 'A1:D20')
            refresh_title = params.get('refresh_title', False)
            preserve_formatting = params.get('preserve_formatting', True)
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
            
            df.attrs['update_chart_data'] = {
                'chart_name': chart_name,
                'new_range': new_data_range,
                'refresh_title': refresh_title,
                'preserve_formatting': preserve_formatting,
                'updated_at': timestamp
            }
            return df, f"Chart '{chart_name}' data update configured (range: {new_data_range})"
        
        elif operation_name == 'format_chart':
            chart_name = params.get('chart_name', '')
            color_scheme = params.get('color_scheme', 'default')
            title_font_size = int(params.get('title_font_size', 14))
            show_border = params.get('show_border', True)
            background_color = params.get('background_color', '#FFFFFF')
            legend_position = params.get('legend_position', 'bottom')
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['format_chart'] = {
                'chart_name': chart_name,
                'color_scheme': color_scheme,
                'title_font_size': title_font_size,
                'show_border': show_border,
                'background_color': background_color,
                'legend_position': legend_position
            }
            return df, f"Chart '{chart_name}' formatting configured ({color_scheme} theme, legend at {legend_position})"
        
        elif operation_name == 'create_dashboard':
            dashboard_title = params.get('dashboard_title', 'Dashboard')
            kpi_columns_str = params.get('kpi_columns', '')
            chart_types_str = params.get('chart_types', 'bar, pie, line')
            layout = params.get('layout', 'grid_2x2')
            include_summary = params.get('include_summary_table', True)
            auto_refresh = params.get('auto_refresh', False)
            
            kpi_columns = [col.strip() for col in kpi_columns_str.split(',') if col.strip()]
            chart_types = [ct.strip() for ct in chart_types_str.split(',') if ct.strip()]
            
            # Generate KPI summaries if columns exist
            kpi_data = {}
            for kpi in kpi_columns:
                if kpi in df.columns:
                    try:
                        kpi_data[kpi] = {
                            'current': float(df[kpi].iloc[-1]) if len(df) > 0 else 0,
                            'total': float(df[kpi].sum()),
                            'average': float(df[kpi].mean()),
                            'change': float(((df[kpi].iloc[-1] - df[kpi].iloc[0]) / df[kpi].iloc[0] * 100)) if len(df) > 1 and df[kpi].iloc[0] != 0 else 0
                        }
                    except:
                        kpi_data[kpi] = {'error': 'Could not calculate'}
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['create_dashboard'] = {
                'title': dashboard_title,
                'kpis': kpi_columns,
                'kpi_data': kpi_data,
                'charts': chart_types,
                'layout': layout,
                'include_summary': include_summary,
                'auto_refresh': auto_refresh
            }
            return df, f"Dashboard '{dashboard_title}' configuration saved ({layout} layout, {len(chart_types)} charts)"
        
        elif operation_name == 'auto_refresh_charts':
            chart_names_str = params.get('chart_names', 'all')
            refresh_mode = params.get('refresh_mode', 'on_data_change')
            refresh_interval = int(params.get('refresh_interval_minutes', 30))
            show_timestamp = params.get('show_refresh_timestamp', True)
            
            chart_names = [cn.strip() for cn in chart_names_str.split(',') if cn.strip()]
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['auto_refresh_charts'] = {
                'charts': chart_names if chart_names_str.lower() != 'all' else ['all'],
                'mode': refresh_mode,
                'interval_minutes': refresh_interval,
                'show_timestamp': show_timestamp
            }
            return df, f"Auto-refresh configured for {len(chart_names) if chart_names_str.lower() != 'all' else 'all'} charts ({refresh_mode})"
        
        elif operation_name == 'export_chart_image':
            chart_name = params.get('chart_name', '')
            output_filename = params.get('output_filename', 'chart_export')
            image_format = params.get('image_format', 'png')
            width = int(params.get('width', 800))
            height = int(params.get('height', 600))
            dpi = int(params.get('dpi', 150))
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['export_chart_image'] = {
                'chart_name': chart_name,
                'filename': f"{output_filename}.{image_format}",
                'format': image_format,
                'width': width,
                'height': height,
                'dpi': dpi
            }
            return df, f"Chart '{chart_name}' will be exported as {output_filename}.{image_format} ({width}x{height}px)"
        
        elif operation_name == 'create_pivot_table':
            row_fields_str = params.get('row_fields', '')
            column_fields_str = params.get('column_fields', '')
            value_field = params.get('value_field', '') or column
            aggregation = params.get('aggregation', 'sum')
            output_sheet = params.get('output_sheet', 'Pivot_Table')
            show_grand_totals = params.get('show_grand_totals', True)
            show_subtotals = params.get('show_subtotals', True)
            
            row_fields = [rf.strip() for rf in row_fields_str.split(',') if rf.strip()]
            column_fields = [cf.strip() for cf in column_fields_str.split(',') if cf.strip()] if column_fields_str else []
            
            # Create actual pivot table if possible
            pivot_result = None
            if row_fields and value_field and value_field in df.columns:
                valid_row_fields = [rf for rf in row_fields if rf in df.columns]
                if valid_row_fields:
                    try:
                        if aggregation == 'sum':
                            pivot_result = df.groupby(valid_row_fields)[value_field].sum().reset_index()
                        elif aggregation == 'count':
                            pivot_result = df.groupby(valid_row_fields)[value_field].count().reset_index()
                        elif aggregation == 'average':
                            pivot_result = df.groupby(valid_row_fields)[value_field].mean().reset_index()
                        elif aggregation == 'min':
                            pivot_result = df.groupby(valid_row_fields)[value_field].min().reset_index()
                        elif aggregation == 'max':
                            pivot_result = df.groupby(valid_row_fields)[value_field].max().reset_index()
                        else:
                            pivot_result = df.groupby(valid_row_fields)[value_field].sum().reset_index()
                    except Exception as e:
                        pivot_result = None
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['create_pivot_table'] = {
                'row_fields': row_fields,
                'column_fields': column_fields,
                'value_field': value_field,
                'aggregation': aggregation,
                'output_sheet': output_sheet,
                'grand_totals': show_grand_totals,
                'subtotals': show_subtotals,
                'preview': pivot_result.head(10).to_dict() if pivot_result is not None else {}
            }
            return df, f"Pivot table configuration saved (grouping by {', '.join(row_fields)}, {aggregation} of {value_field})"
        
        elif operation_name == 'create_summary_report':
            report_title = params.get('report_title', 'Summary Report')
            metrics_columns_str = params.get('metrics_columns', '')
            group_by_column = params.get('group_by_column', '') or column
            calculations_str = params.get('calculations', 'sum, avg')
            include_charts = params.get('include_charts', True)
            output_format = params.get('output_format', 'new_sheet')
            
            metrics_columns = [mc.strip() for mc in metrics_columns_str.split(',') if mc.strip()]
            calculations = [calc.strip().lower() for calc in calculations_str.split(',') if calc.strip()]
            
            # Calculate summary statistics
            summary_data = {}
            for metric in metrics_columns:
                if metric in df.columns:
                    summary_data[metric] = {}
                    try:
                        if 'sum' in calculations:
                            summary_data[metric]['sum'] = float(df[metric].sum())
                        if 'avg' in calculations or 'average' in calculations:
                            summary_data[metric]['average'] = float(df[metric].mean())
                        if 'min' in calculations:
                            summary_data[metric]['min'] = float(df[metric].min())
                        if 'max' in calculations:
                            summary_data[metric]['max'] = float(df[metric].max())
                        if 'count' in calculations:
                            summary_data[metric]['count'] = int(df[metric].count())
                    except:
                        summary_data[metric] = {'error': 'Could not calculate'}
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['create_summary_report'] = {
                'title': report_title,
                'metrics': metrics_columns,
                'group_by': group_by_column,
                'calculations': calculations,
                'include_charts': include_charts,
                'output_format': output_format,
                'summary_data': summary_data
            }
            return df, f"Summary report '{report_title}' configured with {len(metrics_columns)} metrics"
        
        elif operation_name == 'generate_invoice':
            invoice_prefix = params.get('invoice_number_prefix', 'INV')
            customer_col = params.get('customer_name_column', '') or column
            item_desc_col = params.get('item_description_column', '')
            quantity_col = params.get('quantity_column', '')
            unit_price_col = params.get('unit_price_column', '')
            tax_rate = float(params.get('tax_rate', 18))
            include_gst = params.get('include_gst', True)
            company_name = params.get('company_name', 'Your Company')
            output_sheet = params.get('output_sheet', 'Invoice')
            
            from datetime import datetime
            invoice_number = f"{invoice_prefix}-{datetime.now().strftime('%Y%m%d')}-001"
            
            # Calculate invoice totals if possible
            invoice_calc = {}
            try:
                if quantity_col in df.columns and unit_price_col in df.columns:
                    df['_line_total'] = df[quantity_col] * df[unit_price_col]
                    subtotal = float(df['_line_total'].sum())
                    tax_amount = subtotal * (tax_rate / 100)
                    grand_total = subtotal + tax_amount
                    
                    invoice_calc = {
                        'subtotal': subtotal,
                        'tax_rate': tax_rate,
                        'tax_amount': tax_amount,
                        'cgst': tax_amount / 2 if include_gst else 0,
                        'sgst': tax_amount / 2 if include_gst else 0,
                        'grand_total': grand_total
                    }
                    df = df.drop('_line_total', axis=1)
            except:
                invoice_calc = {'error': 'Could not calculate invoice totals'}
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['generate_invoice'] = {
                'invoice_number': invoice_number,
                'company_name': company_name,
                'customer_column': customer_col,
                'item_column': item_desc_col,
                'quantity_column': quantity_col,
                'price_column': unit_price_col,
                'tax_rate': tax_rate,
                'include_gst': include_gst,
                'output_sheet': output_sheet,
                'calculations': invoice_calc
            }
            return df, f"Invoice '{invoice_number}' configuration saved (Tax: {tax_rate}%, GST breakdown: {include_gst})"
        
        elif operation_name == 'create_mis_report':
            report_title = params.get('report_title', 'MIS Report')
            report_period = params.get('report_period', 'monthly')
            metrics_columns_str = params.get('metrics_columns', '')
            comparison_type = params.get('comparison_type', 'vs_previous_period')
            target_column = params.get('target_column', '') or column
            include_variance = params.get('include_variance', True)
            include_charts = params.get('include_charts', True)
            include_recommendations = params.get('include_recommendations', False)
            output_sheet = params.get('output_sheet', 'MIS_Report')
            
            metrics_columns = [mc.strip() for mc in metrics_columns_str.split(',') if mc.strip()]
            
            # Calculate MIS metrics
            mis_data = {}
            for metric in metrics_columns:
                if metric in df.columns:
                    try:
                        current_value = float(df[metric].iloc[-1]) if len(df) > 0 else 0
                        previous_value = float(df[metric].iloc[-2]) if len(df) > 1 else current_value
                        
                        variance = current_value - previous_value
                        variance_pct = (variance / previous_value * 100) if previous_value != 0 else 0
                        
                        mis_data[metric] = {
                            'current': current_value,
                            'previous': previous_value,
                            'variance': variance,
                            'variance_percent': variance_pct,
                            'trend': 'up' if variance > 0 else ('down' if variance < 0 else 'stable')
                        }
                        
                        # Add target comparison if available
                        if target_column and target_column in df.columns and comparison_type == 'vs_target':
                            target_value = float(df[target_column].iloc[-1]) if len(df) > 0 else 0
                            achievement = (current_value / target_value * 100) if target_value != 0 else 0
                            mis_data[metric]['target'] = target_value
                            mis_data[metric]['achievement_percent'] = achievement
                    except:
                        mis_data[metric] = {'error': 'Could not calculate'}
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['create_mis_report'] = {
                'title': report_title,
                'period': report_period,
                'metrics': metrics_columns,
                'comparison_type': comparison_type,
                'target_column': target_column,
                'include_variance': include_variance,
                'include_charts': include_charts,
                'include_recommendations': include_recommendations,
                'output_sheet': output_sheet,
                'mis_data': mis_data
            }
            return df, f"MIS Report '{report_title}' configured ({report_period} period, {len(metrics_columns)} metrics)"
        
        # ============================================
        # EMAIL OPERATIONS
        # ============================================
        
        elif operation_name == 'send_email':
            to_email_column = params.get('to_email_column', '') or column
            subject = params.get('subject', 'Message from SmartSheet Pro')
            body_template = params.get('body_template', '')
            from_name = params.get('from_name', 'SmartSheet Pro')
            reply_to = params.get('reply_to', '')
            priority = params.get('priority', 'normal')
            row_filter = params.get('row_filter', '')
            
            email_count = 0
            valid_emails = []
            
            if to_email_column and to_email_column in df.columns:
                import re
                email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                
                for idx, row in df.iterrows():
                    email = str(row.get(to_email_column, ''))
                    if re.match(email_pattern, email):
                        # Apply row filter if specified
                        if row_filter:
                            try:
                                filter_parts = row_filter.split('=')
                                if len(filter_parts) == 2:
                                    filter_col, filter_val = filter_parts[0].strip(), filter_parts[1].strip()
                                    if filter_col in df.columns and str(row.get(filter_col, '')) != filter_val:
                                        continue
                            except:
                                pass
                        
                        valid_emails.append({
                            'email': email,
                            'row_index': idx,
                            'row_data': row.to_dict()
                        })
                        email_count += 1
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['send_email'] = {
                'to_column': to_email_column,
                'subject': subject,
                'body_template': body_template,
                'from_name': from_name,
                'reply_to': reply_to,
                'priority': priority,
                'total_recipients': email_count,
                'valid_emails': valid_emails[:5],  # Preview first 5
                'status': 'configured'
            }
            return df, f"Email configured for {email_count} valid recipients (Subject: {subject[:30]}...)"
        
        elif operation_name == 'bulk_email':
            email_column = params.get('email_column', '') or column
            name_column = params.get('name_column', '')
            subject = params.get('subject', '')
            body_template = params.get('body_template', '')
            batch_size = int(params.get('batch_size', 50))
            delay = int(params.get('delay_between_batches', 5))
            filter_column = params.get('filter_column', '')
            filter_value = params.get('filter_value', '')
            exclude_unsubscribed = params.get('exclude_unsubscribed', True)
            
            import re
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            
            valid_count = 0
            invalid_count = 0
            filtered_out = 0
            
            if email_column and email_column in df.columns:
                for idx, row in df.iterrows():
                    email = str(row.get(email_column, ''))
                    
                    # Check filter
                    if filter_column and filter_column in df.columns and filter_value:
                        if str(row.get(filter_column, '')) != filter_value:
                            filtered_out += 1
                            continue
                    
                    # Check unsubscribed
                    if exclude_unsubscribed:
                        unsub_cols = ['Unsubscribed', 'unsubscribed', 'Unsubscribe', 'opt_out']
                        for uc in unsub_cols:
                            if uc in df.columns and str(row.get(uc, '')).lower() in ['yes', 'true', '1']:
                                filtered_out += 1
                                continue
                    
                    if re.match(email_pattern, email):
                        valid_count += 1
                    else:
                        invalid_count += 1
            
            num_batches = (valid_count + batch_size - 1) // batch_size if valid_count > 0 else 0
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['bulk_email'] = {
                'email_column': email_column,
                'name_column': name_column,
                'subject': subject,
                'body_template': body_template[:100] + '...' if len(body_template) > 100 else body_template,
                'batch_size': batch_size,
                'delay_seconds': delay,
                'total_valid': valid_count,
                'total_invalid': invalid_count,
                'filtered_out': filtered_out,
                'num_batches': num_batches,
                'status': 'configured'
            }
            return df, f"Bulk email configured: {valid_count} valid recipients in {num_batches} batches ({invalid_count} invalid, {filtered_out} filtered out)"
        
        elif operation_name == 'email_with_attachment':
            to_email_column = params.get('to_email_column', '') or column
            subject = params.get('subject', '')
            body_template = params.get('body_template', '')
            attachment_type = params.get('attachment_type', 'current_dataset')
            attachment_path = params.get('attachment_path', '')
            attachment_name = params.get('attachment_name', 'Report.xlsx')
            export_format = params.get('export_format', 'xlsx')
            
            import re
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            valid_count = 0
            
            if to_email_column and to_email_column in df.columns:
                valid_count = df[to_email_column].apply(lambda x: bool(re.match(email_pattern, str(x)))).sum()
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['email_with_attachment'] = {
                'to_column': to_email_column,
                'subject': subject,
                'body_template': body_template[:100] + '...' if len(body_template) > 100 else body_template,
                'attachment_type': attachment_type,
                'attachment_path': attachment_path,
                'attachment_name': attachment_name,
                'export_format': export_format,
                'valid_recipients': int(valid_count),
                'dataset_rows': len(df),
                'dataset_columns': len(df.columns),
                'status': 'configured'
            }
            return df, f"Email with attachment configured for {int(valid_count)} recipients (Attachment: {attachment_name})"
        
        elif operation_name == 'mail_merge':
            email_column = params.get('email_column', '') or column
            subject_template = params.get('subject_template', '')
            body_template = params.get('body_template', '')
            preview_count = int(params.get('preview_count', 3))
            validate_placeholders = params.get('validate_placeholders', True)
            missing_replacement = params.get('missing_value_replacement', '[N/A]')
            
            import re
            
            # Extract placeholders from templates
            subject_placeholders = re.findall(r'\{(\w+)\}', subject_template)
            body_placeholders = re.findall(r'\{(\w+)\}', body_template)
            all_placeholders = list(set(subject_placeholders + body_placeholders))
            
            # Check which placeholders exist as columns
            missing_placeholders = [p for p in all_placeholders if p not in df.columns]
            found_placeholders = [p for p in all_placeholders if p in df.columns]
            
            # Generate previews
            previews = []
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            
            for idx, row in df.head(preview_count).iterrows():
                if email_column in df.columns:
                    email = str(row.get(email_column, ''))
                    if not re.match(email_pattern, email):
                        continue
                        
                    # Replace placeholders in subject
                    merged_subject = subject_template
                    merged_body = body_template
                    
                    for placeholder in all_placeholders:
                        value = str(row.get(placeholder, missing_replacement))
                        if pd.isna(row.get(placeholder)):
                            value = missing_replacement
                        merged_subject = merged_subject.replace(f'{{{placeholder}}}', value)
                        merged_body = merged_body.replace(f'{{{placeholder}}}', value)
                    
                    previews.append({
                        'email': email,
                        'subject': merged_subject,
                        'body_preview': merged_body[:200] + '...' if len(merged_body) > 200 else merged_body
                    })
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['mail_merge'] = {
                'email_column': email_column,
                'subject_template': subject_template,
                'body_template': body_template[:100] + '...' if len(body_template) > 100 else body_template,
                'placeholders_found': found_placeholders,
                'placeholders_missing': missing_placeholders,
                'total_rows': len(df),
                'previews': previews,
                'status': 'configured' if not missing_placeholders or not validate_placeholders else 'warning'
            }
            
            warning = f" (Warning: Missing columns: {missing_placeholders})" if missing_placeholders and validate_placeholders else ""
            return df, f"Mail merge configured with {len(found_placeholders)} placeholders for {len(df)} rows{warning}"
        
        elif operation_name == 'schedule_email':
            email_column = params.get('email_column', '') or column
            subject = params.get('subject', '')
            body_template = params.get('body_template', '')
            scheduled_date = params.get('scheduled_date', '')
            scheduled_time = params.get('scheduled_time', '09:00')
            timezone = params.get('timezone', 'IST')
            repeat = params.get('repeat', 'none')
            end_repeat_date = params.get('end_repeat_date', '')
            
            import re
            from datetime import datetime
            
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            valid_count = 0
            
            if email_column and email_column in df.columns:
                valid_count = df[email_column].apply(lambda x: bool(re.match(email_pattern, str(x)))).sum()
            
            # Validate date format
            date_valid = False
            try:
                datetime.strptime(scheduled_date, '%Y-%m-%d')
                date_valid = True
            except:
                pass
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['schedule_email'] = {
                'email_column': email_column,
                'subject': subject,
                'body_template': body_template[:100] + '...' if len(body_template) > 100 else body_template,
                'scheduled_date': scheduled_date,
                'scheduled_time': scheduled_time,
                'timezone': timezone,
                'repeat': repeat,
                'end_repeat_date': end_repeat_date,
                'valid_recipients': int(valid_count),
                'date_valid': date_valid,
                'status': 'scheduled' if date_valid else 'invalid_date'
            }
            return df, f"Email scheduled for {scheduled_date} at {scheduled_time} ({timezone}) to {int(valid_count)} recipients (Repeat: {repeat})"
        
        elif operation_name == 'email_report':
            report_name = params.get('report_name', 'Daily Report')
            recipients = params.get('recipients', '')
            subject_template = params.get('subject_template', '{Report_Name} - {Date}')
            include_summary = params.get('include_summary', True)
            summary_columns_str = params.get('summary_columns', '')
            attachment_format = params.get('attachment_format', 'xlsx')
            frequency = params.get('frequency', 'immediate')
            send_time = params.get('send_time', '09:00')
            
            from datetime import datetime
            import re
            
            # Parse recipients
            recipient_list = [r.strip() for r in recipients.split(',') if r.strip()]
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            valid_recipients = [r for r in recipient_list if re.match(email_pattern, r)]
            
            # Parse summary columns
            summary_columns = [c.strip() for c in summary_columns_str.split(',') if c.strip()]
            valid_summary_columns = [c for c in summary_columns if c in df.columns]
            
            # Generate summary if requested
            summary_data = {}
            if include_summary and valid_summary_columns:
                for col in valid_summary_columns:
                    try:
                        if df[col].dtype in ['int64', 'float64']:
                            summary_data[col] = {
                                'sum': float(df[col].sum()),
                                'avg': float(df[col].mean()),
                                'min': float(df[col].min()),
                                'max': float(df[col].max())
                            }
                        else:
                            summary_data[col] = {
                                'unique_count': int(df[col].nunique()),
                                'total_count': int(df[col].count())
                            }
                    except:
                        pass
            
            # Generate subject with placeholders
            current_date = datetime.now().strftime('%Y-%m-%d')
            final_subject = subject_template.replace('{Report_Name}', report_name).replace('{Date}', current_date)
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['email_report'] = {
                'report_name': report_name,
                'recipients': valid_recipients,
                'subject': final_subject,
                'include_summary': include_summary,
                'summary_data': summary_data,
                'attachment_format': attachment_format,
                'frequency': frequency,
                'send_time': send_time,
                'data_rows': len(df),
                'data_columns': len(df.columns),
                'status': 'configured'
            }
            return df, f"Report '{report_name}' configured to send to {len(valid_recipients)} recipients ({frequency}, format: {attachment_format})"
        
        elif operation_name == 'create_email_list':
            email_column = column if column else params.get('email_column', '')
            list_name = params.get('list_name', 'My Email List')
            name_column = params.get('include_name_column', '')
            company_column = params.get('include_company_column', '')
            validate_emails = params.get('validate_emails', True)
            remove_duplicates = params.get('remove_duplicates', True)
            filter_column = params.get('filter_column', '')
            filter_value = params.get('filter_value', '')
            output_format = params.get('output_format', 'new_column')
            segment_by = params.get('segment_by', '')
            
            import re
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            
            email_list = []
            invalid_emails = []
            
            if email_column and email_column in df.columns:
                for idx, row in df.iterrows():
                    email = str(row.get(email_column, '')).strip()
                    
                    # Apply filter
                    if filter_column and filter_column in df.columns and filter_value:
                        if str(row.get(filter_column, '')) != filter_value:
                            continue
                    
                    # Validate email
                    if validate_emails:
                        if not re.match(email_pattern, email):
                            invalid_emails.append(email)
                            continue
                    
                    entry = {'email': email}
                    if name_column and name_column in df.columns:
                        entry['name'] = str(row.get(name_column, ''))
                    if company_column and company_column in df.columns:
                        entry['company'] = str(row.get(company_column, ''))
                    if segment_by and segment_by in df.columns:
                        entry['segment'] = str(row.get(segment_by, ''))
                    
                    email_list.append(entry)
            
            # Remove duplicates
            if remove_duplicates:
                seen = set()
                unique_list = []
                for entry in email_list:
                    if entry['email'] not in seen:
                        seen.add(entry['email'])
                        unique_list.append(entry)
                duplicates_removed = len(email_list) - len(unique_list)
                email_list = unique_list
            else:
                duplicates_removed = 0
            
            # Create segments if requested
            segments = {}
            if segment_by:
                for entry in email_list:
                    seg = entry.get('segment', 'Other')
                    if seg not in segments:
                        segments[seg] = []
                    segments[seg].append(entry['email'])
            
            # Add validation column if output is new_column
            if output_format == 'new_column' and validate_emails:
                df['Email_Valid'] = df[email_column].apply(lambda x: 'Valid' if re.match(email_pattern, str(x)) else 'Invalid')
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['create_email_list'] = {
                'list_name': list_name,
                'total_valid': len(email_list),
                'total_invalid': len(invalid_emails),
                'duplicates_removed': duplicates_removed,
                'segments': segments if segment_by else {},
                'email_list_preview': email_list[:10],
                'invalid_preview': invalid_emails[:5],
                'output_format': output_format,
                'status': 'created'
            }
            return df, f"Email list '{list_name}' created: {len(email_list)} valid emails ({len(invalid_emails)} invalid, {duplicates_removed} duplicates removed)"
        
        elif operation_name == 'track_responses':
            email_column = params.get('email_column', '') or column
            sent_column = params.get('sent_column', '')
            delivered_column = params.get('delivered_column', '')
            opened_column = params.get('opened_column', '')
            clicked_column = params.get('clicked_column', '')
            replied_column = params.get('replied_column', '')
            calculate_metrics = params.get('calculate_metrics', True)
            identify_non_responders = params.get('identify_non_responders', True)
            create_summary = params.get('create_summary_sheet', True)
            group_by_column = params.get('group_by_column', '')
            
            metrics = {
                'total_emails': len(df),
                'sent': 0,
                'delivered': 0,
                'opened': 0,
                'clicked': 0,
                'replied': 0,
                'bounced': 0
            }
            
            non_responders = []
            
            # Count metrics
            for col, key in [(sent_column, 'sent'), (delivered_column, 'delivered'), 
                             (opened_column, 'opened'), (clicked_column, 'clicked'), 
                             (replied_column, 'replied')]:
                if col and col in df.columns:
                    metrics[key] = df[col].apply(lambda x: str(x).lower() in ['yes', 'true', '1', 'delivered', 'sent', 'opened', 'clicked', 'replied']).sum()
            
            # Calculate rates
            rates = {}
            if calculate_metrics and metrics['sent'] > 0:
                rates['delivery_rate'] = round(metrics['delivered'] / metrics['sent'] * 100, 2) if metrics['delivered'] else 0
                rates['open_rate'] = round(metrics['opened'] / metrics['delivered'] * 100, 2) if metrics['delivered'] and metrics['opened'] else 0
                rates['click_rate'] = round(metrics['clicked'] / metrics['opened'] * 100, 2) if metrics['opened'] and metrics['clicked'] else 0
                rates['reply_rate'] = round(metrics['replied'] / metrics['sent'] * 100, 2) if metrics['replied'] else 0
            
            # Identify non-responders
            if identify_non_responders and email_column in df.columns:
                for idx, row in df.iterrows():
                    is_non_responder = True
                    for col in [opened_column, clicked_column, replied_column]:
                        if col and col in df.columns:
                            if str(row.get(col, '')).lower() in ['yes', 'true', '1']:
                                is_non_responder = False
                                break
                    if is_non_responder:
                        non_responders.append(str(row.get(email_column, '')))
            
            # Group by analysis
            grouped_metrics = {}
            if group_by_column and group_by_column in df.columns and opened_column in df.columns:
                for group in df[group_by_column].unique():
                    group_df = df[df[group_by_column] == group]
                    group_opened = group_df[opened_column].apply(lambda x: str(x).lower() in ['yes', 'true', '1']).sum()
                    grouped_metrics[str(group)] = {
                        'total': len(group_df),
                        'opened': int(group_opened),
                        'open_rate': round(group_opened / len(group_df) * 100, 2) if len(group_df) > 0 else 0
                    }
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['track_responses'] = {
                'metrics': metrics,
                'rates': rates,
                'non_responders_count': len(non_responders),
                'non_responders_preview': non_responders[:10],
                'grouped_metrics': grouped_metrics,
                'status': 'analyzed'
            }
            
            return df, f"Response tracking complete: {metrics['sent']} sent, {metrics['opened']} opened ({rates.get('open_rate', 0)}%), {metrics['replied']} replied ({rates.get('reply_rate', 0)}%)"
        
        # ============================================
        # AUTOMATION OPERATIONS
        # ============================================
        
        elif operation_name == 'run_on_file_open':
            trigger_name = params.get('trigger_name', 'On File Open Trigger')
            file_pattern = params.get('file_pattern', '*')
            show_message = params.get('show_message', True)
            message_text = params.get('message_text', 'Welcome! Workflow is running...')
            run_once = params.get('run_once_per_session', True)
            enabled = params.get('enabled', True)
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['run_on_file_open'] = {
                'trigger_name': trigger_name,
                'trigger_type': 'on_file_open',
                'file_pattern': file_pattern,
                'show_message': show_message,
                'message_text': message_text,
                'run_once_per_session': run_once,
                'enabled': enabled,
                'status': 'configured'
            }
            return df, f"File open trigger '{trigger_name}' configured (Pattern: {file_pattern}, Enabled: {enabled})"
        
        elif operation_name == 'run_on_file_close':
            trigger_name = params.get('trigger_name', 'On File Close Trigger')
            action_type = params.get('action_type', 'auto_save')
            backup_path = params.get('backup_path', './backups/')
            include_timestamp = params.get('include_timestamp', True)
            confirm_before = params.get('confirm_before_close', False)
            enabled = params.get('enabled', True)
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['run_on_file_close'] = {
                'trigger_name': trigger_name,
                'trigger_type': 'on_file_close',
                'action_type': action_type,
                'backup_path': backup_path,
                'include_timestamp': include_timestamp,
                'confirm_before_close': confirm_before,
                'enabled': enabled,
                'status': 'configured'
            }
            return df, f"File close trigger '{trigger_name}' configured (Action: {action_type}, Path: {backup_path})"
        
        elif operation_name == 'run_on_cell_change':
            trigger_name = params.get('trigger_name', 'On Cell Change Trigger')
            watch_column = column if column else params.get('watch_column', '')
            watch_type = params.get('watch_type', 'specific_column')
            action = params.get('action_on_change', 'recalculate')
            target_workflow = params.get('target_workflow_id', '')
            debounce = int(params.get('debounce_seconds', 2))
            enabled = params.get('enabled', True)
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['run_on_cell_change'] = {
                'trigger_name': trigger_name,
                'trigger_type': 'on_cell_change',
                'watch_column': watch_column,
                'watch_type': watch_type,
                'action_on_change': action,
                'target_workflow_id': target_workflow,
                'debounce_seconds': debounce,
                'enabled': enabled,
                'status': 'configured'
            }
            return df, f"Cell change trigger '{trigger_name}' configured (Column: {watch_column}, Action: {action})"
        
        elif operation_name == 'scheduled_run':
            schedule_name = params.get('schedule_name', 'Scheduled Task')
            schedule_type = params.get('schedule_type', 'daily')
            run_time = params.get('run_time', '09:00')
            run_days = params.get('run_days', 'Mon-Fri')
            day_of_month = params.get('day_of_month', '1')
            timezone = params.get('timezone', 'IST')
            end_date = params.get('end_date', '')
            skip_holidays = params.get('skip_holidays', False)
            enabled = params.get('enabled', True)
            
            # Calculate next run time
            from datetime import datetime, timedelta
            now = datetime.now()
            next_run = now.replace(hour=int(run_time.split(':')[0]), minute=int(run_time.split(':')[1]), second=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['scheduled_run'] = {
                'schedule_name': schedule_name,
                'trigger_type': 'scheduled',
                'schedule_type': schedule_type,
                'run_time': run_time,
                'run_days': run_days,
                'day_of_month': day_of_month,
                'timezone': timezone,
                'end_date': end_date,
                'skip_holidays': skip_holidays,
                'enabled': enabled,
                'next_run': next_run.strftime('%Y-%m-%d %H:%M'),
                'status': 'scheduled'
            }
            return df, f"Schedule '{schedule_name}' configured ({schedule_type} at {run_time} {timezone}, Next: {next_run.strftime('%Y-%m-%d %H:%M')})"
        
        elif operation_name == 'button_trigger':
            button_id = params.get('button_id', 'btn_run_workflow')
            button_label = params.get('button_label', 'Run Workflow')
            button_style = params.get('button_style', 'primary')
            button_icon = params.get('button_icon', 'play')
            confirm = params.get('confirm_before_run', True)
            confirm_msg = params.get('confirmation_message', 'Are you sure?')
            show_result = params.get('show_result', True)
            position = params.get('position', 'toolbar')
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['button_trigger'] = {
                'button_id': button_id,
                'button_label': button_label,
                'button_style': button_style,
                'button_icon': button_icon,
                'confirm_before_run': confirm,
                'confirmation_message': confirm_msg,
                'show_result': show_result,
                'position': position,
                'trigger_type': 'button',
                'status': 'configured'
            }
            return df, f"Button trigger '{button_label}' configured (ID: {button_id}, Style: {button_style}, Position: {position})"
        
        elif operation_name == 'keyboard_shortcut':
            shortcut_name = params.get('shortcut_name', 'Quick Run Shortcut')
            key_combo = params.get('key_combination', 'Ctrl+Shift+R')
            modifier = params.get('modifier_keys', 'Ctrl+Shift')
            action_key = params.get('action_key', 'R')
            show_tooltip = params.get('show_tooltip', True)
            global_shortcut = params.get('global_shortcut', False)
            enabled = params.get('enabled', True)
            
            # Normalize key combination
            full_shortcut = f"{modifier}+{action_key}" if action_key else key_combo
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['keyboard_shortcut'] = {
                'shortcut_name': shortcut_name,
                'key_combination': full_shortcut,
                'modifier_keys': modifier,
                'action_key': action_key,
                'show_tooltip': show_tooltip,
                'global_shortcut': global_shortcut,
                'enabled': enabled,
                'trigger_type': 'keyboard',
                'status': 'configured'
            }
            return df, f"Keyboard shortcut '{shortcut_name}' configured ({full_shortcut}, Global: {global_shortcut})"
        
        elif operation_name == 'loop_repeat':
            loop_name = params.get('loop_name', 'Batch Process Loop')
            loop_type = params.get('loop_type', 'files_in_folder')
            source_path = params.get('source_path', './data/')
            file_pattern = params.get('file_pattern', '*.xlsx')
            batch_size = int(params.get('batch_size', 10))
            parallel = params.get('parallel_execution', False)
            max_iter = int(params.get('max_iterations', 0))
            continue_on_error = params.get('continue_on_error', True)
            output_folder = params.get('output_folder', './processed/')
            delay = int(params.get('delay_between_iterations', 1))
            
            # Estimate iterations based on loop type
            estimated_iterations = 0
            if loop_type == 'rows_in_sheet':
                estimated_iterations = len(df)
            elif loop_type == 'sheets_in_workbook':
                estimated_iterations = 1  # Current sheet
            else:
                estimated_iterations = max_iter if max_iter > 0 else 'Unknown'
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['loop_repeat'] = {
                'loop_name': loop_name,
                'loop_type': loop_type,
                'source_path': source_path,
                'file_pattern': file_pattern,
                'batch_size': batch_size,
                'parallel_execution': parallel,
                'max_iterations': max_iter,
                'continue_on_error': continue_on_error,
                'output_folder': output_folder,
                'delay_between_iterations': delay,
                'estimated_iterations': estimated_iterations,
                'status': 'configured'
            }
            return df, f"Loop '{loop_name}' configured ({loop_type}, Batch: {batch_size}, Parallel: {parallel})"
        
        elif operation_name == 'error_handling':
            handler_name = params.get('handler_name', 'Error Handler')
            on_error = params.get('on_error_action', 'log_and_continue')
            retry_count = int(params.get('retry_count', 3))
            retry_delay = int(params.get('retry_delay_seconds', 5))
            fallback_wf = params.get('fallback_workflow_id', '')
            log_errors = params.get('log_errors', True)
            log_level = params.get('log_level', 'error')
            notify = params.get('notify_on_error', True)
            notify_email = params.get('notification_email', '')
            error_template = params.get('error_message_template', 'Error: {error_message}')
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['error_handling'] = {
                'handler_name': handler_name,
                'on_error_action': on_error,
                'retry_count': retry_count,
                'retry_delay_seconds': retry_delay,
                'fallback_workflow_id': fallback_wf,
                'log_errors': log_errors,
                'log_level': log_level,
                'notify_on_error': notify,
                'notification_email': notify_email,
                'error_message_template': error_template,
                'status': 'configured'
            }
            return df, f"Error handler '{handler_name}' configured (Action: {on_error}, Retries: {retry_count})"
        
        elif operation_name == 'user_input':
            input_name = params.get('input_name', 'User Input Prompt')
            input_type = params.get('input_type', 'text')
            prompt_text = params.get('prompt_text', 'Please enter value:')
            default_value = params.get('default_value', '')
            options = params.get('options', '')
            required = params.get('required', True)
            validation = params.get('validation_pattern', '')
            min_val = params.get('min_value', '')
            max_val = params.get('max_value', '')
            variable_name = params.get('store_as_variable', 'user_input_value')
            
            options_list = [o.strip() for o in options.split(',') if o.strip()] if options else []
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['user_input'] = {
                'input_name': input_name,
                'input_type': input_type,
                'prompt_text': prompt_text,
                'default_value': default_value,
                'options': options_list,
                'required': required,
                'validation_pattern': validation,
                'min_value': min_val,
                'max_value': max_val,
                'variable_name': variable_name,
                'status': 'configured'
            }
            return df, f"User input '{input_name}' configured (Type: {input_type}, Required: {required})"
        
        elif operation_name == 'progress_bar':
            progress_name = params.get('progress_name', 'Workflow Progress')
            display_type = params.get('display_type', 'bar')
            show_percentage = params.get('show_percentage', True)
            show_time = params.get('show_time_remaining', True)
            show_step = params.get('show_current_step', True)
            show_rows = params.get('show_rows_processed', True)
            allow_cancel = params.get('allow_cancel', True)
            auto_close = params.get('auto_close', True)
            auto_close_delay = int(params.get('auto_close_delay', 3))
            position = params.get('position', 'center')
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['progress_bar'] = {
                'progress_name': progress_name,
                'display_type': display_type,
                'show_percentage': show_percentage,
                'show_time_remaining': show_time,
                'show_current_step': show_step,
                'show_rows_processed': show_rows,
                'allow_cancel': allow_cancel,
                'auto_close': auto_close,
                'auto_close_delay': auto_close_delay,
                'position': position,
                'total_rows': len(df),
                'status': 'configured'
            }
            return df, f"Progress bar '{progress_name}' configured ({display_type}, Position: {position}, Cancel: {allow_cancel})"
        
        elif operation_name == 'log_activity':
            log_name = params.get('log_name', 'Workflow Activity Log')
            destination = params.get('log_destination', 'file')
            file_path = params.get('log_file_path', './logs/workflow_activity.log')
            log_level = params.get('log_level', 'info')
            include_timestamp = params.get('include_timestamp', True)
            include_user = params.get('include_user', True)
            include_duration = params.get('include_duration', True)
            include_rows = params.get('include_row_count', True)
            log_format = params.get('log_format', 'json')
            max_size = int(params.get('max_log_size_mb', 10))
            rotate = params.get('rotate_logs', True)
            
            from datetime import datetime
            
            # Create a sample log entry
            sample_entry = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S') if include_timestamp else None,
                'user': 'current_user' if include_user else None,
                'workflow': 'current_workflow',
                'action': 'started',
                'rows_processed': len(df) if include_rows else None,
                'duration_seconds': 0 if include_duration else None,
                'status': 'success'
            }
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['log_activity'] = {
                'log_name': log_name,
                'destination': destination,
                'file_path': file_path,
                'log_level': log_level,
                'include_timestamp': include_timestamp,
                'include_user': include_user,
                'include_duration': include_duration,
                'include_row_count': include_rows,
                'log_format': log_format,
                'max_size_mb': max_size,
                'rotate_logs': rotate,
                'sample_entry': sample_entry,
                'status': 'configured'
            }
            return df, f"Activity logging '{log_name}' configured (Destination: {destination}, Format: {log_format})"
        
        elif operation_name == 'chain_macros':
            chain_name = params.get('chain_name', 'Workflow Chain')
            workflow_sequence_str = params.get('workflow_sequence', '')
            pass_data = params.get('pass_data_between', True)
            on_failure = params.get('on_step_failure', 'abort_chain')
            delay = int(params.get('delay_between_steps', 2))
            timeout = int(params.get('timeout_per_step_minutes', 30))
            notify = params.get('notify_on_complete', True)
            save_intermediate = params.get('save_intermediate_results', False)
            rollback = params.get('rollback_on_failure', False)
            summary = params.get('summary_report', True)
            
            workflow_sequence = [wf.strip() for wf in workflow_sequence_str.split(',') if wf.strip()]
            
            if not hasattr(df, 'attrs'):
                df.attrs = {}
            
            df.attrs['chain_macros'] = {
                'chain_name': chain_name,
                'workflow_sequence': workflow_sequence,
                'total_steps': len(workflow_sequence),
                'pass_data_between': pass_data,
                'on_step_failure': on_failure,
                'delay_between_steps': delay,
                'timeout_per_step_minutes': timeout,
                'notify_on_complete': notify,
                'save_intermediate_results': save_intermediate,
                'rollback_on_failure': rollback,
                'generate_summary': summary,
                'status': 'configured'
            }
            return df, f"Workflow chain '{chain_name}' configured ({len(workflow_sequence)} steps: {' → '.join(workflow_sequence)})"
        
        else:
            return df, f"Operation '{operation_name}' not implemented yet"
    
    def _save_cleaned_dataset(self, df, original_dataset, workflow, user):
        """Save processed DataFrame as new dataset with optional formatting"""
        
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


# Alias for backward compatibility
WorkflowService = WorkflowExecutionService