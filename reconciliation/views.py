from django.utils import timezone
from django.core.paginator import Paginator
from django.conf import settings
from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import ReconciliationJob
from .serializers import ReconciliationJobSerializer, ReconciliationJobCreateSerializer
from .services.comparator import DatasetComparator
from .tasks import run_reconciliation_async

class ReconciliationJobListCreateView(generics.ListCreateAPIView):
    def get_queryset(self):
        # Filter by status if requested (for dropdown)
        status_filter = self.request.query_params.get('status')
        queryset = ReconciliationJob.objects.filter(user=self.request.user).order_by('-created_at')
        if status_filter:
            queryset = queryset.filter(status=status_filter)
        return queryset
    
    def get_serializer_class(self):
        if self.request.method == 'POST':
            return ReconciliationJobCreateSerializer
        return ReconciliationJobSerializer
    
    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            self.perform_create(serializer)
            headers = self.get_success_headers(serializer.data)
            
            # Return the full object using the read serializer
            output_serializer = ReconciliationJobSerializer(serializer.instance)
            return Response(output_serializer.data, status=status.HTTP_201_CREATED, headers=headers)
        except Exception as e:
            import traceback
            traceback.print_exc()
            return Response(
                {'error': 'Failed to create reconciliation', 'detail': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def perform_create(self, serializer):
        try:
            job = serializer.save(user=self.request.user, status='pending')
            
            # Check if Celery is enabled
            use_celery = getattr(settings, 'USE_CELERY', True)
            
            if not use_celery:
                # Run synchronously without Celery
                self._run_reconciliation_sync(job)
            else:
                # Check if async processing needed
                max_rows = max(job.source_dataset.row_count, job.target_dataset.row_count)
                
                if max_rows > settings.ASYNC_ROW_COUNT_THRESHOLD:
                    # Run asynchronously
                    try:
                        task = run_reconciliation_async.delay(str(job.id))
                        job.task_id = task.id
                        job.save()
                    except Exception as e:
                        print(f"Celery unavailable ({e}), running synchronously...")
                        self._run_reconciliation_sync(job)
                else:
                    # Run synchronously even with Celery enabled for small datasets
                    self._run_reconciliation_sync(job)
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise
    
    def _run_reconciliation_sync(self, job):
        """Run reconciliation synchronously without Celery"""
        job.status = 'running'
        job.save()
        
        try:
            # Run comparison
            source_data = job.source_dataset.data
            target_data = job.target_dataset.data
            
            results = DatasetComparator.compare_datasets(
                source_data=source_data,
                target_data=target_data,
                source_keys=job.source_key_columns,
                target_keys=job.target_key_columns,
                compare_columns=job.compare_columns,
                fuzzy_match=job.fuzzy_match,
                fuzzy_threshold=job.fuzzy_threshold
            )
            
            job.results = results
            job.summary = results['summary']
            job.status = 'completed'
            job.completed_at = timezone.now()
            job.save()
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            job.status = 'failed'
            job.error_message = str(e)
            job.completed_at = timezone.now()
            job.save()

class ReconciliationJobDetailView(generics.RetrieveDestroyAPIView):
    serializer_class = ReconciliationJobSerializer
    
    def get_queryset(self):
        return ReconciliationJob.objects.filter(user=self.request.user)

@api_view(['GET'])
def reconciliation_results(request, pk):
    try:
        job = ReconciliationJob.objects.get(pk=pk, user=request.user)
    except ReconciliationJob.DoesNotExist:
        return Response({'error': 'Job not found'}, status=status.HTTP_404_NOT_FOUND)
    
    if job.status != 'completed':
        return Response({'error': 'Job not completed'}, status=status.HTTP_400_BAD_REQUEST)
    
    result_type = request.GET.get('type', 'all')  # matches, mismatches, source_only, target_only
    page = int(request.GET.get('page', 1))
    page_size = int(request.GET.get('page_size', 50))
    
    if result_type == 'all':
        data = job.results
    else:
        data = job.results.get(result_type, [])
    
    if isinstance(data, list):
        paginator = Paginator(data, page_size)
        page_obj = paginator.get_page(page)
        
        return Response({
            'data': page_obj.object_list,
            'total_pages': paginator.num_pages,
            'current_page': page,
            'total_items': len(data),
            'summary': job.summary
        })
    else:
        return Response({
            'data': data,
            'summary': job.summary
        })

@api_view(['GET'])
def export_results(request, pk):
    try:
        job = ReconciliationJob.objects.get(pk=pk, user=request.user)
    except ReconciliationJob.DoesNotExist:
        return Response({'error': 'Job not found'}, status=status.HTTP_404_NOT_FOUND)
    
    if job.status != 'completed':
        return Response({'error': 'Job not completed'}, status=status.HTTP_400_BAD_REQUEST)
    
    return Response({
        'job_name': job.name,
        'created_at': job.created_at,
        'completed_at': job.completed_at,
        'summary': job.summary,
        'results': job.results
    })