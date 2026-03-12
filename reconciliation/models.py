import uuid
from django.db import models
from django.conf import settings
from datasets.models import Dataset

class ReconciliationJob(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    name = models.CharField(max_length=255)
    source_dataset = models.ForeignKey(Dataset, on_delete=models.SET_NULL, null=True, related_name='source_reconciliations')
    target_dataset = models.ForeignKey(Dataset, on_delete=models.SET_NULL, null=True, related_name='target_reconciliations')
    source_key_columns = models.JSONField()
    target_key_columns = models.JSONField()
    compare_columns = models.JSONField()
    fuzzy_match = models.BooleanField(default=False)
    fuzzy_threshold = models.IntegerField(default=80)
    results = models.JSONField(default=dict)
    summary = models.JSONField(default=dict)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    task_id = models.CharField(max_length=255, blank=True, null=True)  # Celery task ID
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return self.name