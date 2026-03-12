from django.utils import timezone
from django.conf import settings
from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from datasets.models import Dataset, DatasetVersion
from .models import SavedRule, BulkOperation
from .serializers import (
    SavedRuleSerializer, BulkOperationSerializer, 
    RulePreviewSerializer, RuleExecuteSerializer
)
from .services.rule_engine import RuleEngine
from .tasks import execute_bulk_async

class SavedRuleListCreateView(generics.ListCreateAPIView):
    serializer_class = SavedRuleSerializer

    def get_queryset(self):
        return SavedRule.objects.filter(user=self.request.user).select_related('dataset').order_by('-created_at')

    def perform_create(self, serializer):
        print(f"Received data: {self.request.data}")  # Debug log
        serializer.save(user=self.request.user)
        
    def create(self, request, *args, **kwargs):
        print(f"Raw request data: {request.data}")  # Debug log
        serializer = self.get_serializer(data=request.data)
        if not serializer.is_valid():
            print(f"Validation errors: {serializer.errors}")  # Debug log
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        self.perform_create(serializer)
        headers = self.get_success_headers(serializer.data)
        return Response(serializer.data, status=status.HTTP_201_CREATED, headers=headers)

class SavedRuleDetailView(generics.RetrieveUpdateDestroyAPIView):
    serializer_class = SavedRuleSerializer

    def get_queryset(self):
        return SavedRule.objects.filter(user=self.request.user).select_related('dataset')

class BulkOperationListView(generics.ListAPIView):
    serializer_class = BulkOperationSerializer

    def get_queryset(self):
        return BulkOperation.objects.filter(user=self.request.user).select_related('dataset', 'saved_rule').order_by('-created_at')

@api_view(['POST'])
def preview_rule(request, dataset_id):
    try:
        dataset = Dataset.objects.get(pk=dataset_id, user=request.user)
    except Dataset.DoesNotExist:
        return Response({'error': 'Dataset not found'}, status=status.HTTP_404_NOT_FOUND)
    
    # Handle both old and new API formats
    if 'conditions' in request.data and 'action' in request.data:
        # Direct format from frontend
        rule_config = {
            'conditions': request.data['conditions'],
            'action': request.data['action']
        }
    else:
        # Serializer format
        serializer = RulePreviewSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        rule_config = {
            'conditions': serializer.validated_data['conditions'],
            'action': serializer.validated_data['action']
        }

    try:
        result = RuleEngine.preview_affected_rows(dataset.data, rule_config)
        return Response({
            'affected_count': result['affected_count'],
            'total_rows': result['total_rows'],
            'preview_rows': result['preview_rows'],
            'affected_indices': result['affected_indices']
        })
    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

@api_view(['POST'])
def execute_rule(request, dataset_id):
    try:
        dataset = Dataset.objects.get(pk=dataset_id, user=request.user)
    except Dataset.DoesNotExist:
        return Response({'error': 'Dataset not found'}, status=status.HTTP_404_NOT_FOUND)
    
    # Handle both old and new API formats
    if 'conditions' in request.data and 'action' in request.data:
        # Direct format from frontend
        rule_config = {
            'conditions': request.data['conditions'],
            'action': request.data['action']
        }
        saved_rule_id = request.data.get('saved_rule_id')
        description = request.data.get('description', 'Bulk operation applied')
        save_as_rule = request.data.get('save_as_rule', False)
        rule_name = request.data.get('rule_name', '')
    else:
        # Serializer format
        serializer = RuleExecuteSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        rule_config = {
            'conditions': serializer.validated_data['conditions'],
            'action': serializer.validated_data['action']
        }
        saved_rule_id = serializer.validated_data.get('saved_rule_id')
        description = serializer.validated_data.get('description', 'Bulk operation applied')
        save_as_rule = serializer.validated_data.get('save_as_rule', False)
        rule_name = serializer.validated_data.get('rule_name', '')

    # Check if async processing needed
    if dataset.row_count > settings.ASYNC_ROW_COUNT_THRESHOLD:
        # Process asynchronously
        task = execute_bulk_async.delay(
            dataset_id=str(dataset.id),
            user_id=str(request.user.id),
            rule_config=rule_config,
            save_as_rule=save_as_rule,
            rule_name=rule_name,
            rule_description=description
        )
        
        return Response({
            'message': 'Bulk operation started. Processing in background.',
            'task_id': task.id,
            'status_url': f'/api/tasks/{task.id}/status/',
        }, status=status.HTTP_202_ACCEPTED)
    
    else:
        # Process synchronously (existing code)
        # Create version snapshot
        DatasetVersion.objects.create(
            dataset=dataset,
            version_number=dataset.version,
            schema_snapshot=dataset.schema,
            data_snapshot=dataset.data,
            change_description=description
        )

        try:
            # Execute rule
            result = RuleEngine.execute_rule(dataset.data.copy(), rule_config)
            
            # Update saved rule usage if provided
            saved_rule = None
            if saved_rule_id:
                try:
                    saved_rule = SavedRule.objects.get(pk=saved_rule_id, user=request.user)
                    saved_rule.use_count += 1
                    saved_rule.last_used_at = timezone.now()
                    saved_rule.save()
                except SavedRule.DoesNotExist:
                    pass

            # Create bulk operation record
            bulk_op = BulkOperation.objects.create(
                user=request.user,
                dataset=dataset,
                saved_rule=saved_rule,
                rule_config=rule_config,
                affected_rows=result['affected_count'],
                affected_indices=result['affected_indices'],
                undo_data=result['undo_data'],
                dataset_version_before=dataset.version,
                dataset_version_after=dataset.version + 1
            )

            # Update dataset
            dataset.data = result['modified_data']
            dataset.version += 1
            dataset.save()

            return Response({
                'operation_id': str(bulk_op.id),
                'affected_rows': int(result['affected_count']),
                'dataset_version': int(dataset.version)
            })

        except Exception as e:
            import traceback
            print(f"Error executing rule: {str(e)}")
            print(traceback.format_exc())
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
def undo_operation(request, operation_id):
    try:
        operation = BulkOperation.objects.get(pk=operation_id, user=request.user)
    except BulkOperation.DoesNotExist:
        return Response({'error': 'Operation not found'}, status=status.HTTP_404_NOT_FOUND)

    if operation.is_undone:
        return Response({'error': 'Operation already undone'}, status=status.HTTP_400_BAD_REQUEST)

    dataset = operation.dataset

    # Create version snapshot
    DatasetVersion.objects.create(
        dataset=dataset,
        version_number=dataset.version,
        schema_snapshot=dataset.schema,
        data_snapshot=dataset.data,
        change_description=f'Undo operation {operation.id}'
    )

    try:
        # Undo the operation
        undone_data = RuleEngine.undo_operation(dataset.data.copy(), operation.undo_data)
        
        # Update dataset
        dataset.data = undone_data
        dataset.version += 1
        dataset.save()

        # Mark operation as undone
        operation.is_undone = True
        operation.save()

        return Response({
            'message': 'Operation undone successfully',
            'dataset_version': dataset.version
        })

    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

@api_view(['POST'])
def apply_saved_rule(request, rule_id):
    """Execute a saved rule on a dataset"""
    try:
        rule = SavedRule.objects.get(pk=rule_id, user=request.user)
    except SavedRule.DoesNotExist:
        return Response({'error': 'Rule not found'}, status=status.HTTP_404_NOT_FOUND)
    
    dataset_id = request.data.get('dataset_id')
    if not dataset_id:
        return Response({'error': 'dataset_id is required'}, status=status.HTTP_400_BAD_REQUEST)
    
    try:
        dataset = Dataset.objects.get(pk=dataset_id, user=request.user)
    except Dataset.DoesNotExist:
        return Response({'error': 'Dataset not found'}, status=status.HTTP_404_NOT_FOUND)
    
    # Create version snapshot
    DatasetVersion.objects.create(
        dataset=dataset,
        version_number=dataset.version,
        schema_snapshot=dataset.schema,
        data_snapshot=dataset.data,
        change_description=f'Applied saved rule: {rule.name}'
    )
    
    try:
        # Execute the saved rule
        result = RuleEngine.execute_rule(dataset.data.copy(), rule.configuration)
        
        # Update rule usage
        rule.use_count += 1
        rule.last_used_at = timezone.now()
        rule.save()
        
        # Create bulk operation record
        bulk_op = BulkOperation.objects.create(
            user=request.user,
            dataset=dataset,
            saved_rule=rule,
            rule_config=rule.configuration,
            affected_rows=result['affected_count'],
            affected_indices=result['affected_indices'],
            undo_data=result['undo_data'],
            dataset_version_before=dataset.version,
            dataset_version_after=dataset.version + 1
        )
        
        # Update dataset
        dataset.data = result['modified_data']
        dataset.version += 1
        dataset.save()
        
        return Response({
            'success': True,
            'message': 'Rule applied successfully',
            'rows_affected': result['affected_count'],
            'total_rows': len(dataset.data),
            'operation_id': str(bulk_op.id)
        })
        
    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

@api_view(['GET'])
def dataset_history(request, dataset_id):
    try:
        dataset = Dataset.objects.get(pk=dataset_id, user=request.user)
    except Dataset.DoesNotExist:
        return Response({'error': 'Dataset not found'}, status=status.HTTP_404_NOT_FOUND)

    operations = BulkOperation.objects.filter(
        dataset=dataset, user=request.user
    ).order_by('-created_at')
    
    serializer = BulkOperationSerializer(operations, many=True)
    return Response(serializer.data)