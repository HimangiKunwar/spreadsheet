from rest_framework import serializers
from .models import CleanupWorkflow, WorkflowRun

class WorkflowOperationSerializer(serializers.Serializer):
    """Single operation in a workflow"""
    operation = serializers.CharField()
    column = serializers.CharField(required=False, allow_blank=True)
    params = serializers.DictField(required=False, default=dict)

class CleanupWorkflowSerializer(serializers.ModelSerializer):
    operations = WorkflowOperationSerializer(many=True)
    run_count = serializers.IntegerField(read_only=True)
    last_run = serializers.DateTimeField(read_only=True)

    class Meta:
        model = CleanupWorkflow
        fields = ['id', 'name', 'description', 'operations', 'is_active', 
                  'created_at', 'updated_at', 'last_run', 'run_count']
        read_only_fields = ['id', 'created_at', 'updated_at']

    def create(self, validated_data):
        validated_data['user'] = self.context['request'].user
        return super().create(validated_data)


class WorkflowRunSerializer(serializers.ModelSerializer):
    workflow_name = serializers.CharField(source='workflow.name', read_only=True)
    dataset_name = serializers.CharField(source='dataset.name', read_only=True)

    class Meta:
        model = WorkflowRun
        fields = ['id', 'workflow', 'workflow_name', 'dataset', 'dataset_name',
                  'status', 'started_at', 'completed_at', 'results', 'error_message']
        read_only_fields = ['id', 'started_at', 'completed_at', 'results', 'error_message']


class RunWorkflowSerializer(serializers.Serializer):
    """Input for running a workflow"""
    dataset_id = serializers.UUIDField()