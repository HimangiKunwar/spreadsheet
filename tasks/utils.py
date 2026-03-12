"""
Task Utility Functions
"""
from django.core.cache import cache
from celery.result import AsyncResult


def get_task_progress(task_id):
    """
    Get task progress
    """
    # Try cache first (faster)
    progress = cache.get(f'task_progress:{task_id}')
    if progress:
        return progress
    
    # Fallback to Celery result
    result = AsyncResult(task_id)
    
    response = {
        'state': result.state,
        'current': 0,
        'total': 100,
        'percent': 0,
        'message': '',
    }
    
    if result.state == 'PENDING':
        response['message'] = 'Task pending...'
    elif result.state == 'PROGRESS':
        if isinstance(result.info, dict):
            response.update(result.info)
    elif result.state == 'SUCCESS':
        response.update({
            'current': 100,
            'total': 100,
            'percent': 100,
            'message': 'Complete',
            'result': result.result,
        })
    elif result.state == 'FAILURE':
        response.update({
            'message': 'Task failed',
            'error': str(result.info),
        })
    
    return response


def cancel_task(task_id):
    """Cancel a running task"""
    result = AsyncResult(task_id)
    result.revoke(terminate=True)
    cache.delete(f'task_progress:{task_id}')
    return True