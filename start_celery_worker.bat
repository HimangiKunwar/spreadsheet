@echo off
echo Starting Celery Worker for SmartSheet Pro...
cd /d D:\himangi\smartsheet-pro\backend
call venv\Scripts\activate
celery -A config worker -l info -P solo --concurrency=1
pause