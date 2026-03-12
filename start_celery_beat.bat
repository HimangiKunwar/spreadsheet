@echo off
echo Starting Celery Beat Scheduler for SmartSheet Pro...
cd /d D:\himangi\smartsheet-pro\backend
call venv\Scripts\activate
celery -A config beat -l info --scheduler django_celery_beat.schedulers:DatabaseScheduler
pause