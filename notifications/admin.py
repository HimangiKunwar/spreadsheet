from django.contrib import admin
from .models import Notification, NotificationPreference, EmailLog


@admin.register(NotificationPreference)
class NotificationPreferenceAdmin(admin.ModelAdmin):
    list_display = ['user', 'email_workflow_complete', 'email_workflow_failed', 'email_report_ready', 'updated_at']
    list_filter = ['email_workflow_complete', 'email_workflow_failed', 'email_report_ready']
    search_fields = ['user__email']


@admin.register(Notification)
class NotificationAdmin(admin.ModelAdmin):
    list_display = ['title', 'user', 'notification_type', 'status', 'email_sent', 'created_at']
    list_filter = ['notification_type', 'status', 'email_sent']
    search_fields = ['user__email', 'title']
    readonly_fields = ['created_at']


@admin.register(EmailLog)
class EmailLogAdmin(admin.ModelAdmin):
    list_display = ['recipient_email', 'subject', 'status', 'sent_at', 'created_at']
    list_filter = ['status']
    search_fields = ['recipient_email', 'subject']
    readonly_fields = ['created_at']