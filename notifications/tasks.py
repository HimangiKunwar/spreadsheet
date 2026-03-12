from celery import shared_task
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from django.utils import timezone
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def send_notification_email(self, notification_id):
    """Send email for a specific notification"""
    from .models import Notification, EmailLog
    
    try:
        notification = Notification.objects.select_related('user').get(id=notification_id)
        user = notification.user
        
        # Check user preferences
        prefs = getattr(user, 'notification_preferences', None)
        if prefs:
            pref_map = {
                'workflow_complete': prefs.email_workflow_complete,
                'workflow_failed': prefs.email_workflow_failed,
                'report_ready': prefs.email_report_ready,
            }
            if not pref_map.get(notification.notification_type, True):
                logger.info(f"Email disabled for {notification.notification_type} - user {user.email}")
                return {'status': 'skipped', 'reason': 'user_preference'}
        
        # Create email content
        subject = f"SmartSheet Pro: {notification.title}"
        
        html_content = render_to_string('notifications/email_template.html', {
            'user': user,
            'notification': notification,
            'app_url': getattr(settings, 'FRONTEND_URL', 'http://localhost:3000')
        })
        text_content = strip_tags(html_content)
        
        # Send email
        email = EmailMultiAlternatives(
            subject=subject,
            body=text_content,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=[user.email]
        )
        email.attach_alternative(html_content, "text/html")
        email.send()
        
        # Log the email
        EmailLog.objects.create(
            notification=notification,
            recipient_email=user.email,
            subject=subject,
            body=text_content,
            status='sent',
            sent_at=timezone.now()
        )
        
        # Update notification
        notification.email_sent = True
        notification.email_sent_at = timezone.now()
        notification.status = 'sent'
        notification.save()
        
        logger.info(f"Email sent successfully to {user.email}")
        return {'status': 'sent', 'email': user.email}
        
    except Exception as exc:
        logger.error(f"Failed to send email: {exc}")
        # Log failure
        if 'notification' in locals():
            EmailLog.objects.create(
                recipient_email=notification.user.email,
                subject=f"Failed: {notification_id}",
                body='',
                status='failed',
                error_message=str(exc)
            )
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))


@shared_task
def send_workflow_completion_email(workflow_run_id):
    """Send email when workflow completes"""
    from workflows.models import WorkflowRun
    from .models import Notification
    
    try:
        run = WorkflowRun.objects.select_related('workflow', 'workflow__user', 'dataset').get(id=workflow_run_id)
        user = run.workflow.user
        
        # Create notification
        notification = Notification.objects.create(
            user=user,
            notification_type='workflow_complete',
            title=f"Workflow '{run.workflow.name}' Completed",
            message=f"Your workflow has finished processing dataset '{run.dataset.name}'. "
                    f"Status: {run.status}. "
                    f"Results: {run.results.get('summary', 'Processing complete')}",
            metadata={
                'workflow_id': str(run.workflow.id),
                'workflow_run_id': str(run.id),
                'dataset_id': str(run.dataset.id)
            }
        )
        
        # Queue email
        send_notification_email.delay(str(notification.id))
        
        return {'status': 'queued', 'notification_id': str(notification.id)}
        
    except Exception as e:
        logger.error(f"Error creating workflow completion notification: {e}")
        return {'status': 'error', 'message': str(e)}


@shared_task
def send_workflow_failure_email(workflow_run_id, error_message):
    """Send email when workflow fails"""
    from workflows.models import WorkflowRun
    from .models import Notification
    
    try:
        run = WorkflowRun.objects.select_related('workflow', 'workflow__user', 'dataset').get(id=workflow_run_id)
        user = run.workflow.user
        
        notification = Notification.objects.create(
            user=user,
            notification_type='workflow_failed',
            title=f"Workflow '{run.workflow.name}' Failed",
            message=f"Your workflow encountered an error while processing dataset '{run.dataset.name}'. "
                    f"Error: {error_message}",
            metadata={
                'workflow_id': str(run.workflow.id),
                'workflow_run_id': str(run.id),
                'dataset_id': str(run.dataset.id),
                'error': error_message
            }
        )
        
        send_notification_email.delay(str(notification.id))
        
        return {'status': 'queued', 'notification_id': str(notification.id)}
        
    except Exception as e:
        logger.error(f"Error creating workflow failure notification: {e}")
        return {'status': 'error', 'message': str(e)}


@shared_task
def send_report_ready_email(report_id):
    """Send email when report is generated"""
    from reports.models import Report
    from .models import Notification
    
    try:
        report = Report.objects.select_related('user').get(id=report_id)
        user = report.user
        
        notification = Notification.objects.create(
            user=user,
            notification_type='report_ready',
            title=f"Report '{report.name}' is Ready",
            message=f"Your report has been generated and is ready for download.",
            metadata={
                'report_id': str(report.id)
            }
        )
        
        send_notification_email.delay(str(notification.id))
        
        return {'status': 'queued', 'notification_id': str(notification.id)}
        
    except Exception as e:
        logger.error(f"Error creating report ready notification: {e}")
        return {'status': 'error', 'message': str(e)}


@shared_task
def send_daily_summary():
    """Send daily summary to users who opted in"""
    from django.contrib.auth import get_user_model
    from .models import NotificationPreference, Notification
    from workflows.models import WorkflowRun
    from datetime import timedelta
    
    User = get_user_model()
    yesterday = timezone.now() - timedelta(days=1)
    
    users_with_daily = NotificationPreference.objects.filter(
        email_daily_summary=True
    ).select_related('user')
    
    for pref in users_with_daily:
        user = pref.user
        
        # Get yesterday's stats
        runs = WorkflowRun.objects.filter(
            workflow__user=user,
            started_at__gte=yesterday
        )
        
        completed = runs.filter(status='completed').count()
        failed = runs.filter(status='failed').count()
        
        if completed > 0 or failed > 0:
            notification = Notification.objects.create(
                user=user,
                notification_type='system',
                title="Daily Summary",
                message=f"Yesterday's activity: {completed} workflows completed, {failed} failed.",
                metadata={'completed': completed, 'failed': failed}
            )
            send_notification_email.delay(str(notification.id))
    
    return {'status': 'completed'}