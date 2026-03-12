from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.shortcuts import get_object_or_404
from datasets.models import Dataset
from .models import CleanupWorkflow, WorkflowRun
from .serializers import CleanupWorkflowSerializer, WorkflowRunSerializer, RunWorkflowSerializer
from .services import WorkflowExecutionService

class WorkflowViewSet(viewsets.ModelViewSet):
    serializer_class = CleanupWorkflowSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return CleanupWorkflow.objects.filter(user=self.request.user)

    @action(detail=False, methods=['get'])
    def operations(self, request):
        """Get list of available cleanup operations"""
        try:
            service = WorkflowExecutionService()
            operations = service.get_available_operations()
            return Response(operations)
        except Exception as e:
            import traceback
            print(f"ERROR in get_operations: {e}")
            traceback.print_exc()
            return Response({'error': str(e)}, status=500)

    @action(detail=True, methods=['post'])
    def run(self, request, pk=None):
        """Execute workflow on a dataset"""
        workflow = self.get_object()
        serializer = RunWorkflowSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        
        dataset = get_object_or_404(
            Dataset, 
            id=serializer.validated_data['dataset_id'],
            user=request.user
        )
        
        run = WorkflowExecutionService.run_workflow(workflow, dataset, request.user)
        return Response(WorkflowRunSerializer(run).data)

    @action(detail=True, methods=['get'])
    def history(self, request, pk=None):
        """Get run history for workflow"""
        workflow = self.get_object()
        runs = WorkflowRun.objects.filter(workflow=workflow).order_by('-started_at')[:20]
        return Response(WorkflowRunSerializer(runs, many=True).data)


class WorkflowRunViewSet(viewsets.ReadOnlyModelViewSet):
    serializer_class = WorkflowRunSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return WorkflowRun.objects.filter(workflow__user=self.request.user)