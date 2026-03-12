from rest_framework import serializers
from .models import Notification, NotificationPreference


class NotificationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Notification
        fields = [
            'id', 'notification_type', 'title', 'message', 
            'status', 'email_sent', 'read_at', 'metadata', 'created_at'
        ]
        read_only_fields = ['id', 'notification_type', 'title', 'message', 'email_sent', 'created_at']


class NotificationPreferenceSerializer(serializers.ModelSerializer):
    class Meta:
        model = NotificationPreference
        fields = [
            'id', 'email_workflow_complete', 'email_workflow_failed',
            'email_report_ready', 'email_daily_summary', 'email_weekly_summary',
            'updated_at'
        ]
        read_only_fields = ['id', 'updated_at']