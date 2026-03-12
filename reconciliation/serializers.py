from rest_framework import serializers
from .models import ReconciliationJob
from datasets.models import Dataset

class ReconciliationJobSerializer(serializers.ModelSerializer):
    source_dataset_name = serializers.CharField(source='source_dataset.name', read_only=True)
    target_dataset_name = serializers.CharField(source='target_dataset.name', read_only=True)

    class Meta:
        model = ReconciliationJob
        fields = ['id', 'name', 'source_dataset', 'target_dataset', 
                 'source_dataset_name', 'target_dataset_name',
                 'source_key_columns', 'target_key_columns', 'compare_columns',
                 'fuzzy_match', 'fuzzy_threshold', 'results', 'summary', 
                 'status', 'error_message', 'created_at', 'completed_at']
        read_only_fields = ['id', 'results', 'summary', 'status', 'error_message', 
                           'created_at', 'completed_at']

class ReconciliationJobCreateSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReconciliationJob
        fields = ['name', 'source_dataset', 'target_dataset', 
                 'source_key_columns', 'target_key_columns', 'compare_columns',
                 'fuzzy_match', 'fuzzy_threshold']

    def validate_source_key_columns(self, value):
        if not value or not isinstance(value, list):
            raise serializers.ValidationError("Source key columns must be a non-empty list")
        return value
    
    def validate_target_key_columns(self, value):
        if not value or not isinstance(value, list):
            raise serializers.ValidationError("Target key columns must be a non-empty list")
        return value
    
    def validate_compare_columns(self, value):
        if not isinstance(value, list):
            raise serializers.ValidationError("Compare columns must be a list")
        return value
    
    def validate_fuzzy_threshold(self, value):
        if value is not None and (value < 0 or value > 100):
            raise serializers.ValidationError("Fuzzy threshold must be between 0 and 100")
        return value

    def validate(self, attrs):
        # Validate datasets belong to user
        user = self.context['request'].user
        
        if not Dataset.objects.filter(id=attrs['source_dataset'].id, user=user).exists():
            raise serializers.ValidationError("Source dataset not found")
        
        if not Dataset.objects.filter(id=attrs['target_dataset'].id, user=user).exists():
            raise serializers.ValidationError("Target dataset not found")
        
        if attrs['source_dataset'].id == attrs['target_dataset'].id:
            raise serializers.ValidationError("Source and target datasets must be different")
        
        return attrs