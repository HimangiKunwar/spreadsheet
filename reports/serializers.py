from rest_framework import serializers
from .models import Report, ReportTemplate
from reconciliation.models import ReconciliationJob

class ReportSerializer(serializers.ModelSerializer):
    def to_internal_value(self, data):
        # Handle reconciliation_id before validation
        if 'reconciliation_id' in data and data['reconciliation_id']:
            data = data.copy()
            data['reconciliation'] = data.pop('reconciliation_id')
        return super().to_internal_value(data)
    
    dataset_name = serializers.SerializerMethodField()
    reconciliation_name = serializers.SerializerMethodField()
    source_dataset_name = serializers.SerializerMethodField()
    target_dataset_name = serializers.SerializerMethodField()

    class Meta:
        model = Report
        fields = ['id', 'dataset', 'dataset_name', 'reconciliation',
                 'reconciliation_name', 'source_dataset_name', 'target_dataset_name', 
                 'name', 'description', 'configuration', 'pdf_path', 'pptx_path', 'xlsx_path', 
                 'status', 'error_message', 'is_template', 
                 'created_at', 'updated_at', 'generated_at']
        read_only_fields = ['id', 'pdf_path', 'pptx_path', 'xlsx_path', 
                           'status', 'error_message', 'created_at', 
                           'updated_at', 'generated_at', 'user']
    
    def get_dataset_name(self, obj):
        if obj.dataset:
            return obj.dataset.name
        return None
    
    def get_reconciliation_name(self, obj):
        if obj.reconciliation:
            return obj.reconciliation.name
        return None
    
    def get_source_dataset_name(self, obj):
        if obj.reconciliation and obj.reconciliation.source_dataset:
            return obj.reconciliation.source_dataset.name
        return None
    
    def get_target_dataset_name(self, obj):
        if obj.reconciliation and obj.reconciliation.target_dataset:
            return obj.reconciliation.target_dataset.name
        return None
    
    def create(self, validated_data):
        # Handle reconciliation UUID string
        reconciliation = validated_data.get('reconciliation')
        if reconciliation and isinstance(reconciliation, str):
            try:
                reconciliation_obj = ReconciliationJob.objects.get(
                    pk=reconciliation,
                    user=self.context['request'].user
                )
                validated_data['reconciliation'] = reconciliation_obj
            except ReconciliationJob.DoesNotExist:
                validated_data['reconciliation'] = None
        
        return super().create(validated_data)
    
    def update(self, instance, validated_data):
        reconciliation_id = validated_data.pop('reconciliation_id', None)
        
        if reconciliation_id:
            try:
                reconciliation = ReconciliationJob.objects.get(
                    pk=reconciliation_id,
                    user=self.context['request'].user
                )
                validated_data['reconciliation'] = reconciliation
            except ReconciliationJob.DoesNotExist:
                pass
        
        return super().update(instance, validated_data)

class ReportTemplateSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReportTemplate
        fields = ['id', 'name', 'description', 'category', 'configuration', 
                 'required_columns', 'thumbnail', 'is_public', 'created_at']

class ChartPreviewSerializer(serializers.Serializer):
    chart_type = serializers.ChoiceField(choices=[
        'bar', 'horizontal_bar', 'line', 'pie', 'area', 'scatter'
    ])
    x_column = serializers.CharField()
    y_column = serializers.CharField()
    title = serializers.CharField(required=False, allow_blank=True)

class SummaryRequestSerializer(serializers.Serializer):
    columns = serializers.ListField(child=serializers.CharField(), required=False)
    metrics = serializers.ListField(child=serializers.CharField(), required=False)
    group_by = serializers.CharField(required=False, allow_blank=True)

class GenerateReportSerializer(serializers.Serializer):
    format = serializers.ChoiceField(choices=['pdf', 'xlsx'], default='pdf')