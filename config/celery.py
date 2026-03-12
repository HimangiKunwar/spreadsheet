"""
Celery Configuration for SmartSheet Pro
"""
import os
from celery import Celery

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

# Create Celery app
app = Celery('smartsheet_pro')

# Load config from Django settings with CELERY_ prefix
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks in all apps
app.autodiscover_tasks()

# Task routing to different queues
app.conf.task_routes = {
    'datasets.tasks.*': {'queue': 'datasets'},
    'reconciliation.tasks.*': {'queue': 'reconciliation'},
    'bulk_operations.tasks.*': {'queue': 'bulk'},
    'reports.tasks.*': {'queue': 'reports'},
    'notifications.tasks.*': {'queue': 'notifications'},
}

# Default task settings
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,      # 1 hour hard limit
    task_soft_time_limit=3000,  # 50 min soft limit
    worker_prefetch_multiplier=1,
    task_acks_late=True,
)