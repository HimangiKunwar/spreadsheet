from rest_framework import serializers
from .models import SavedRule, BulkOperation

class SavedRuleSerializer(serializers.ModelSerializer):
    dataset_id = serializers.SerializerMethodField()
    dataset_name = serializers.SerializerMethodField()
    
    class Meta:
        model = SavedRule
        fields = ['id', 'name', 'description', 'configuration', 'dataset', 'dataset_id', 'dataset_name',
                 'use_count', 'last_used_at', 'created_at', 'updated_at']
        read_only_fields = ['id', 'dataset_id', 'dataset_name', 'use_count', 'last_used_at', 'created_at', 'updated_at']
    
    def get_dataset_id(self, obj):
        return str(obj.dataset.id) if obj.dataset else None
    
    def get_dataset_name(self, obj):
        return obj.dataset.name if obj.dataset else None

class BulkOperationSerializer(serializers.ModelSerializer):
    dataset_name = serializers.SerializerMethodField()
    rule_name = serializers.SerializerMethodField()

    class Meta:
        model = BulkOperation
        fields = ['id', 'dataset', 'dataset_name', 'saved_rule', 'rule_name',
                 'rule_config', 'affected_rows', 'affected_indices', 
                 'dataset_version_before', 'dataset_version_after', 
                 'is_undone', 'created_at']
        read_only_fields = ['id', 'created_at']
    
    def get_dataset_name(self, obj):
        return obj.dataset.name if obj.dataset else None
    
    def get_rule_name(self, obj):
        return obj.saved_rule.name if obj.saved_rule else None

class RulePreviewSerializer(serializers.Serializer):
    conditions = serializers.ListField()
    action = serializers.DictField()

class RuleExecuteSerializer(serializers.Serializer):
    conditions = serializers.ListField()
    action = serializers.DictField()
    saved_rule_id = serializers.UUIDField(required=False, allow_null=True)
    description = serializers.CharField(max_length=500, required=False)