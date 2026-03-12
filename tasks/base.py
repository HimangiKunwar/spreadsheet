"""
Base Task Classes with Progress Tracking
"""
from celery import Task
from django.core.cache import cache


class ProgressTask(Task):
    """
    Task with progress tracking
    """
    
    def update_progress(self, current, total, message=''):
        """Update task progress in cache and task state"""
        percent = int((current / total) * 100) if total > 0 else 0
        
        progress = {
            'current': current,
            'total': total,
            'percent': percent,
            'message': message,
            'state': 'PROGRESS',
        }
        
        # Store in cache for fast retrieval
        cache.set(f'task_progress:{self.request.id}', progress, timeout=3600)
        
        # Update Celery task state
        self.update_state(state='PROGRESS', meta=progress)
        
        return progress
    
    def on_success(self, retval, task_id, args, kwargs):
        """Clear progress on success"""
        cache.delete(f'task_progress:{task_id}')
        super().on_success(retval, task_id, args, kwargs)
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Clear progress on failure"""
        cache.delete(f'task_progress:{task_id}')
        super().on_failure(exc, task_id, args, kwargs, einfo)


class ChunkedTask(ProgressTask):
    """
    Task for processing large datasets in chunks
    """
    
    chunk_size = 1000
    
    def process_chunks(self, data, process_func):
        """
        Process data in chunks with progress updates
        """
        total = len(data)
        results = []
        
        for i in range(0, total, self.chunk_size):
            chunk = data[i:i + self.chunk_size]
            result = process_func(chunk)
            
            if isinstance(result, list):
                results.extend(result)
            else:
                results.append(result)
            
            processed = min(i + self.chunk_size, total)
            self.update_progress(processed, total, f'Processed {processed:,} of {total:,}')
        
        return results