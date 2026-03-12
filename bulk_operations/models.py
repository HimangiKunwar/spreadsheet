import uuid
from django.db import models
from django.conf import settings
from datasets.models import Dataset

class SavedRule(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, null=True, blank=True)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    configuration = models.JSONField()
    use_count = models.IntegerField(default=0)
    last_used_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

class BulkOperation(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    saved_rule = models.ForeignKey(SavedRule, on_delete=models.SET_NULL, null=True, blank=True)
    rule_config = models.JSONField()
    affected_rows = models.IntegerField()
    affected_indices = models.JSONField()
    undo_data = models.JSONField()
    dataset_version_before = models.IntegerField()
    dataset_version_after = models.IntegerField()
    is_undone = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Operation on {self.dataset.name}"