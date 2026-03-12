"""
Async Tasks for Reconciliation
"""
from celery import shared_task
from django.utils import timezone

from tasks.base import ProgressTask, ChunkedTask
from .models import ReconciliationJob
from .services.comparator import DatasetComparator
# from .services import DatasetComparator


@shared_task(bind=True, base=ProgressTask)
def run_reconciliation_async(self, job_id):
    """
    Run reconciliation asynchronously
    """
    try:
        job = ReconciliationJob.objects.get(id=job_id)
        job.status = 'running'
        job.save()
        
        self.update_progress(10, 100, 'Loading datasets...')
        
        source_data = job.source_dataset.data
        target_data = job.target_dataset.data
        
        self.update_progress(25, 100, 'Preparing comparison...')
        
        results = DatasetComparator.compare_datasets(
            source_data=source_data,
            target_data=target_data,
            source_keys=job.source_key_columns,
            target_keys=job.target_key_columns,
            compare_columns=job.compare_columns,
            fuzzy_match=job.fuzzy_match,
            fuzzy_threshold=job.fuzzy_threshold
        )
        
        self.update_progress(90, 100, 'Saving results...')
        
        job.results = results
        job.summary = results['summary']
        job.status = 'completed'
        job.completed_at = timezone.now()
        job.save()
        
        self.update_progress(100, 100, 'Complete!')
        
        return {
            'status': 'success',
            'job_id': str(job_id),
            'summary': results['summary'],
        }
        
    except Exception as e:
        try:
            job = ReconciliationJob.objects.get(id=job_id)
            job.status = 'failed'
            job.error_message = str(e)
            job.save()
        except:
            pass
        raise