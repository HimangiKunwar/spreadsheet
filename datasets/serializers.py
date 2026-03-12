from rest_framework import serializers
from .models import Dataset, DatasetVersion
from .services.data_cleaner import DataCleaner

class DatasetSerializer(serializers.ModelSerializer):
    class Meta:
        model = Dataset
        fields = ['id', 'name', 'original_filename', 'file_type', 'file_size', 
                 'schema', 'row_count', 'column_count', 'version', 'created_at', 'updated_at']
        read_only_fields = ['id', 'created_at', 'updated_at']

class DatasetListSerializer(serializers.ModelSerializer):
    class Meta:
        model = Dataset
        fields = ['id', 'name', 'original_filename', 'file_type', 'file_size', 
                 'row_count', 'column_count', 'version', 'created_at']

class DatasetVersionSerializer(serializers.ModelSerializer):
    class Meta:
        model = DatasetVersion
        fields = ['id', 'version_number', 'change_description', 'created_at']

class CleanupOperationSerializer(serializers.Serializer):
    operation = serializers.ChoiceField(choices=list(DataCleaner.CLEANUP_OPERATIONS.keys()))
    columns = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        allow_empty=True
    )
    options = serializers.JSONField(required=False, default=dict)

class CleanupPreviewSerializer(serializers.Serializer):
    operation = serializers.ChoiceField(choices=list(DataCleaner.CLEANUP_OPERATIONS.keys()))
    columns = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        allow_empty=True
    )
    options = serializers.JSONField(required=False, default=dict)