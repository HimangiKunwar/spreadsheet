#!/usr/bin/env python
"""
Setup script for Email Notifications with Celery
"""
import os
import sys
import django

# Add the backend directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from django.core.management import execute_from_command_line
from django.contrib.auth import get_user_model
from notifications.models import NotificationPreference

def setup_notifications():
    """Setup email notifications system"""
    print("🚀 Setting up Email Notifications with Celery")
    print("=" * 60)
    
    # 1. Create migrations
    print("\n📦 Creating database migrations...")
    try:
        execute_from_command_line(['manage.py', 'makemigrations', 'notifications'])
        print("✓ Notifications migrations created")
    except Exception as e:
        print(f"⚠️  Migration creation: {e}")
    
    # 2. Apply migrations
    print("\n🗄️  Applying database migrations...")
    try:
        execute_from_command_line(['manage.py', 'migrate'])
        print("✓ Database migrations applied")
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return
    
    # 3. Create notification preferences for existing users
    print("\n👥 Setting up notification preferences for existing users...")
    User = get_user_model()
    users_updated = 0
    
    for user in User.objects.all():
        prefs, created = NotificationPreference.objects.get_or_create(
            user=user,
            defaults={
                'email_workflow_complete': True,
                'email_workflow_failed': True,
                'email_report_ready': True,
                'email_daily_summary': False,
                'email_weekly_summary': True,
            }
        )
        if created:
            users_updated += 1
    
    print(f"✓ Updated notification preferences for {users_updated} users")
    
    # 4. Check environment variables
    print("\n🔧 Checking environment configuration...")
    
    email_user = os.environ.get('EMAIL_HOST_USER')
    email_pass = os.environ.get('EMAIL_HOST_PASSWORD')
    
    if email_user and email_pass:
        print("✓ Email credentials configured")
    else:
        print("⚠️  Email credentials not configured")
        print("   Set EMAIL_HOST_USER and EMAIL_HOST_PASSWORD environment variables")
    
    redis_url = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
    print(f"✓ Redis URL: {redis_url}")
    
    # 5. Test Redis connection
    print("\n🔴 Testing Redis connection...")
    try:
        import redis
        r = redis.from_url(redis_url)
        r.ping()
        print("✓ Redis connection successful")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        print("   Install Redis: https://redis.io/download")
        print("   Or use Docker: docker run -d -p 6379:6379 redis:alpine")
    
    # 6. Create periodic tasks
    print("\n⏰ Setting up periodic tasks...")
    try:
        from django_celery_beat.models import PeriodicTask, CrontabSchedule
        
        # Daily summary at 9 AM
        schedule, created = CrontabSchedule.objects.get_or_create(
            minute=0,
            hour=9,
            day_of_week='*',
            day_of_month='*',
            month_of_year='*',
        )
        
        task, created = PeriodicTask.objects.get_or_create(
            name='Send Daily Summary',
            defaults={
                'crontab': schedule,
                'task': 'notifications.tasks.send_daily_summary',
                'enabled': True,
            }
        )
        print(f"✓ Daily summary task {'created' if created else 'updated'}")
        
    except Exception as e:
        print(f"⚠️  Periodic task setup: {e}")
    
    print("\n" + "=" * 60)
    print("🎉 Email Notifications setup completed!")
    print("\nNext steps:")
    print("1. Start Redis server")
    print("2. Start Celery worker: start_celery_worker.bat")
    print("3. Start Celery beat: start_celery_beat.bat")
    print("4. Test notifications: python test_notifications.py")
    print("\nAPI Endpoints:")
    print("- GET /api/notifications/ - List notifications")
    print("- GET /api/notification-preferences/ - Get preferences")
    print("- PUT /api/notification-preferences/ - Update preferences")

if __name__ == '__main__':
    setup_notifications()