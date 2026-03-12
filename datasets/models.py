import uuid
from django.db import models
from django.conf import settings

class Dataset(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    name = models.CharField(max_length=255)
    original_filename = models.CharField(max_length=255)
    file_path = models.CharField(max_length=500)
    file_type = models.CharField(max_length=10, choices=[
        ('csv', 'CSV'), ('xlsx', 'Excel'), ('xls', 'Excel Legacy'),
        ('tsv', 'TSV'), ('json', 'JSON')
    ])
    file_size = models.BigIntegerField()
    schema = models.JSONField()
    data = models.JSONField()
    row_count = models.IntegerField()
    column_count = models.IntegerField()
    version = models.IntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

class DatasetVersion(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    version_number = models.IntegerField()
    schema_snapshot = models.JSONField()
    data_snapshot = models.JSONField()
    change_description = models.CharField(max_length=500)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ['dataset', 'version_number']