import uuid
from django.db import models
from django.conf import settings
from datasets.models import Dataset

class CleanupWorkflow(models.Model):
    """Saved sequence of cleanup operations (like Excel Macro)"""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='workflows')
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    # List of operations: [{"operation": "trim_whitespace", "column": "Name"}, ...]
    operations = models.JSONField(default=list)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_run = models.DateTimeField(null=True, blank=True)
    run_count = models.IntegerField(default=0)

    class Meta:
        ordering = ['-updated_at']

    def __str__(self):
        return f"{self.name} ({len(self.operations)} operations)"


class WorkflowRun(models.Model):
    """History of workflow executions"""
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    workflow = models.ForeignKey(CleanupWorkflow, on_delete=models.CASCADE, related_name='runs')
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name='workflow_runs')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    # Results: {"operations_completed": 5, "rows_affected": 150, "details": [...]}
    results = models.JSONField(null=True, blank=True)
    error_message = models.TextField(blank=True)

    class Meta:
        ordering = ['-started_at']