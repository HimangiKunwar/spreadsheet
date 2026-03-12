#!/bin/bash

echo "=== SmartSheet Pro - Celery & Redis Setup ==="

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Run Django migrations
echo "Running Django migrations..."
python manage.py makemigrations django_celery_results
python manage.py makemigrations django_celery_beat
python manage.py migrate

# Create superuser if needed
echo "Creating superuser (optional)..."
echo "Run: python manage.py createsuperuser --email admin@example.com --first_name Admin --last_name User"

echo ""
echo "=== Setup Complete! ==="
echo ""
echo "To run the system:"
echo "1. Start Redis: docker-compose up redis"
echo "2. Start Django: python manage.py runserver"
echo "3. Start Celery Worker: celery -A config worker -l info"
echo "4. (Optional) Start Flower: celery -A config flower --port=5555"
echo ""
echo "Test async processing:"
echo "- Upload files > 5MB for async file processing"
echo "- Run reconciliation on datasets > 10K rows"
echo "- Execute bulk operations on datasets > 10K rows"
echo "- Generate reports with > 5 charts"