# Email Notifications with Celery - SmartSheet Pro

This document describes the email notification system implemented for SmartSheet Pro using Celery and Redis.

## 🚀 Features

- **Workflow Notifications**: Email alerts when workflows complete or fail
- **Report Notifications**: Email alerts when reports are generated
- **User Preferences**: Granular control over notification types
- **Scheduled Summaries**: Daily/weekly summary emails
- **Async Processing**: Non-blocking email sending using Celery
- **Template System**: HTML email templates with branding
- **Audit Trail**: Complete logging of email activities

## 📋 Requirements

- Redis server (message broker)
- SMTP email server (Gmail, Outlook, etc.)
- Python packages (already in requirements.txt):
  - celery[redis]>=5.3.0
  - redis>=5.0.0
  - django-celery-beat>=2.5.0
  - django-celery-results>=2.5.0

## 🛠️ Installation & Setup

### 1. Install Redis

**Windows (using Chocolatey):**
```bash
choco install redis-64
```

**Windows (using Docker):**
```bash
docker run -d -p 6379:6379 --name redis redis:alpine
```

**Linux/macOS:**
```bash
# Ubuntu/Debian
sudo apt-get install redis-server

# macOS
brew install redis
```

### 2. Configure Environment Variables

Create a `.env` file in the backend directory:

```env
# Email Configuration
EMAIL_HOST_USER=your-email@gmail.com
EMAIL_HOST_PASSWORD=your-app-password
FRONTEND_URL=http://localhost:3000

# Redis Configuration (optional)
REDIS_URL=redis://localhost:6379/0
```

**For Gmail:**
1. Enable 2-factor authentication
2. Generate an App Password: https://myaccount.google.com/apppasswords
3. Use the app password (not your regular password)

### 3. Run Setup Script

```bash
cd backend
python setup_notifications.py
```

This will:
- Create database migrations
- Apply migrations
- Set up notification preferences for existing users
- Create periodic tasks for daily summaries
- Test Redis connection

### 4. Start Services

**Terminal 1 - Django Server:**
```bash
python manage.py runserver
```

**Terminal 2 - Celery Worker:**
```bash
start_celery_worker.bat
# or manually:
celery -A config worker -l info -P solo
```

**Terminal 3 - Celery Beat (for scheduled tasks):**
```bash
start_celery_beat.bat
# or manually:
celery -A config beat -l info --scheduler django_celery_beat.schedulers:DatabaseScheduler
```

### 5. Test the System

```bash
python test_notifications.py
```

## 📧 Email Templates

Email templates are located in `notifications/templates/notifications/`:

- `email_template.html` - Main HTML template with SmartSheet Pro branding

The template includes:
- Responsive design
- Company branding
- Dynamic content based on notification type
- Links back to the application
- Preference management links

## 🔔 Notification Types

### 1. Workflow Complete
- **Trigger**: When a workflow finishes successfully
- **Content**: Workflow name, dataset, results summary
- **User Setting**: `email_workflow_complete`

### 2. Workflow Failed
- **Trigger**: When a workflow encounters an error
- **Content**: Workflow name, dataset, error message
- **User Setting**: `email_workflow_failed`

### 3. Report Ready
- **Trigger**: When a report is generated and ready for download
- **Content**: Report name, download link
- **User Setting**: `email_report_ready`

### 4. Daily Summary
- **Trigger**: Scheduled daily at 9 AM
- **Content**: Yesterday's workflow statistics
- **User Setting**: `email_daily_summary`

### 5. Weekly Summary
- **Trigger**: Scheduled weekly (configurable)
- **Content**: Week's activity summary
- **User Setting**: `email_weekly_summary`

## 🎛️ User Preferences

Users can control their notification preferences via API:

**Get Preferences:**
```http
GET /api/notification-preferences/
Authorization: Bearer <token>
```

**Update Preferences:**
```http
PUT /api/notification-preferences/
Authorization: Bearer <token>
Content-Type: application/json

{
  "email_workflow_complete": true,
  "email_workflow_failed": true,
  "email_report_ready": true,
  "email_daily_summary": false,
  "email_weekly_summary": true
}
```

## 📊 API Endpoints

### Notifications
- `GET /api/notifications/` - List user's notifications
- `GET /api/notifications/{id}/` - Get specific notification
- `POST /api/notifications/{id}/mark_read/` - Mark as read
- `POST /api/notifications/mark_all_read/` - Mark all as read
- `GET /api/notifications/unread_count/` - Get unread count

### Notification Preferences
- `GET /api/notification-preferences/` - Get user preferences
- `PUT /api/notification-preferences/` - Update preferences

## 🔧 Configuration

### Celery Settings (config/settings.py)

```python
# Celery Configuration
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'django-db'
CELERY_CACHE_BACKEND = 'django-cache'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'UTC'
CELERY_TASK_TRACK_STARTED = True
CELERY_RESULT_EXPIRES = 86400  # 24 hours

# Email Configuration
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_HOST = 'smtp.gmail.com'
EMAIL_PORT = 587
EMAIL_USE_TLS = True
EMAIL_HOST_USER = os.environ.get('EMAIL_HOST_USER', '')
EMAIL_HOST_PASSWORD = os.environ.get('EMAIL_HOST_PASSWORD', '')
DEFAULT_FROM_EMAIL = 'SmartSheet Pro <noreply@smartsheetpro.com>'
FRONTEND_URL = os.environ.get('FRONTEND_URL', 'http://localhost:3000')
```

### Task Routing

Tasks are routed to specific queues:

```python
app.conf.task_routes = {
    'notifications.tasks.*': {'queue': 'notifications'},
    'datasets.tasks.*': {'queue': 'datasets'},
    'reconciliation.tasks.*': {'queue': 'reconciliation'},
    # ... other routes
}
```

## 🐛 Troubleshooting

### Common Issues

**1. Celery worker not starting:**
```bash
# Check Redis connection
redis-cli ping

# Start worker with verbose logging
celery -A config worker -l debug
```

**2. Emails not sending:**
```bash
# Test email configuration
python manage.py shell
>>> from django.core.mail import send_mail
>>> send_mail('Test', 'Test message', 'from@example.com', ['to@example.com'])
```

**3. Tasks not executing:**
```bash
# Check Celery status
celery -A config inspect active
celery -A config inspect stats
```

**4. Redis connection issues:**
```bash
# Test Redis connection
redis-cli ping
# Should return: PONG
```

### Logs

Check logs in:
- Django logs: Console output
- Celery worker logs: Console output
- Email logs: Database table `email_logs`

## 🔒 Security Considerations

1. **Email Credentials**: Store in environment variables, never in code
2. **Rate Limiting**: Celery naturally provides rate limiting
3. **User Isolation**: All notifications are user-scoped
4. **Template Security**: HTML templates are safe from XSS
5. **Unsubscribe**: Users can disable notifications via preferences

## 📈 Monitoring

### Database Tables

- `notifications` - All notifications
- `notification_preferences` - User preferences
- `email_logs` - Email sending audit trail
- `django_celery_results_taskresult` - Celery task results
- `django_celery_beat_periodictask` - Scheduled tasks

### Celery Monitoring

```bash
# Monitor tasks
celery -A config events

# Web monitoring (install flower)
pip install flower
celery -A config flower
# Visit: http://localhost:5555
```

## 🚀 Production Deployment

### 1. Use Production Email Service
- AWS SES
- SendGrid
- Mailgun
- Office 365

### 2. Redis Configuration
```python
# Use Redis with authentication
CELERY_BROKER_URL = 'redis://:password@redis-host:6379/0'
```

### 3. Process Management
Use supervisor or systemd to manage Celery processes:

```ini
# /etc/supervisor/conf.d/celery.conf
[program:celery-worker]
command=/path/to/venv/bin/celery -A config worker -l info
directory=/path/to/backend
user=www-data
autostart=true
autorestart=true

[program:celery-beat]
command=/path/to/venv/bin/celery -A config beat -l info
directory=/path/to/backend
user=www-data
autostart=true
autorestart=true
```

### 4. Environment Variables
```bash
export EMAIL_HOST_USER=production@company.com
export EMAIL_HOST_PASSWORD=secure-password
export REDIS_URL=redis://production-redis:6379/0
export FRONTEND_URL=https://smartsheetpro.com
```

## 📝 Development

### Adding New Notification Types

1. **Add to models.py:**
```python
NOTIFICATION_TYPES = [
    # ... existing types
    ('new_type', 'New Type Description'),
]
```

2. **Create task in tasks.py:**
```python
@shared_task
def send_new_type_email(object_id):
    # Implementation
    pass
```

3. **Trigger from views:**
```python
from notifications.tasks import send_new_type_email
send_new_type_email.delay(str(object.id))
```

4. **Update preferences model:**
```python
email_new_type = models.BooleanField(default=True)
```

### Testing

```bash
# Run notification tests
python test_notifications.py

# Test specific task
python manage.py shell
>>> from notifications.tasks import send_notification_email
>>> send_notification_email('notification-id')
```

## 📞 Support

For issues or questions:
1. Check the troubleshooting section
2. Review Celery and Redis logs
3. Test individual components
4. Verify environment configuration

## 🎉 Success!

Your email notification system is now ready! Users will receive timely updates about their workflows and reports, improving the overall user experience of SmartSheet Pro.