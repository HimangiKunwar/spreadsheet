"""
Async Tasks for Dataset Processing
"""
import os
from celery import shared_task
from django.conf import settings

from tasks.base import ProgressTask
from .models import Dataset, DatasetVersion
from .services.file_parser import FileParser
from .services.type_detector import TypeDetector
from .services.data_cleaner import DataCleaner

# from .services import FileParser, TypeDetector, DataCleaner


@shared_task(bind=True, base=ProgressTask)
def process_file_upload(self, file_path, user_id, dataset_name, original_filename):
    """
    Process uploaded file asynchronously
    """
    from authentication.models import User
    
    try:
        user = User.objects.get(id=user_id)
        
        self.update_progress(5, 100, 'Starting file processing...')
        
        # Parse file
        self.update_progress(15, 100, 'Parsing file...')
        parser = FileParser(file_path)
        df, file_type = parser.parse()
        
        # Convert to records
        self.update_progress(35, 100, 'Converting data...')
        data = FileParser.dataframe_to_records(df)
        
        # Detect types
        self.update_progress(55, 100, 'Detecting column types...')
        detector = TypeDetector(data)
        schema = detector.detect_all()
        
        # Create dataset
        self.update_progress(80, 100, 'Saving to database...')
        file_size = os.path.getsize(file_path)
        
        dataset = Dataset.objects.create(
            user=user,
            name=dataset_name,
            original_filename=original_filename,
            file_path=file_path,
            file_type=file_type,
            file_size=file_size,
            schema=schema,
            data=data,
            row_count=len(data),
            column_count=len(df.columns),
            version=1,
        )
        
        # Create initial version
        dataset.save_version("Initial upload")
        
        self.update_progress(100, 100, 'Complete!')
        
        return {
            'status': 'success',
            'dataset_id': str(dataset.id),
            'name': dataset.name,
            'row_count': len(data),
            'column_count': len(df.columns),
        }
        
    except Exception as e:
        # Cleanup on error
        if os.path.exists(file_path):
            os.remove(file_path)
        raise


@shared_task(bind=True, base=ProgressTask)
def cleanup_dataset_async(self, dataset_id, operation, columns=None, options=None):
    """
    Apply cleanup operation asynchronously
    """
    try:
        dataset = Dataset.objects.get(id=dataset_id)
        
        self.update_progress(10, 100, 'Starting cleanup...')
        
        # Save version before cleanup
        dataset.save_version(f"Before cleanup: {operation}")
        
        # Apply cleanup
        self.update_progress(30, 100, f'Applying {operation}...')
        cleaner = DataCleaner(dataset.data)
        cleaned_data = cleaner.apply(operation, columns, options or {})
        
        # Re-detect types
        self.update_progress(70, 100, 'Re-detecting column types...')
        detector = TypeDetector(cleaned_data)
        new_schema = detector.detect_all()
        
        # Save
        self.update_progress(90, 100, 'Saving changes...')
        dataset.data = cleaned_data
        dataset.schema = new_schema
        dataset.row_count = len(cleaned_data)
        dataset.version += 1
        dataset.save()
        
        self.update_progress(100, 100, 'Complete!')
        
        return {
            'status': 'success',
            'rows_affected': len(cleaned_data),
            'new_version': dataset.version,
        }
        
    except Exception as e:
        raise