import os
import uuid
from django.conf import settings
from django.core.paginator import Paginator
from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from .models import Dataset, DatasetVersion
from .serializers import (
    DatasetSerializer, DatasetListSerializer, DatasetVersionSerializer,
    CleanupOperationSerializer, CleanupPreviewSerializer
)
from .services.file_parser import FileParser
from .services.type_detector import TypeDetector
from .services.data_cleaner import DataCleaner
from .tasks import process_file_upload, cleanup_dataset_async

class DatasetListCreateView(generics.ListCreateAPIView):
    serializer_class = DatasetListSerializer
    parser_classes = [MultiPartParser, FormParser]

    def get_queryset(self):
        return Dataset.objects.filter(user=self.request.user)

    def create(self, request, *args, **kwargs):
        file_obj = request.FILES.get('file')
        if not file_obj:
            return Response({'error': 'No file provided'}, status=status.HTTP_400_BAD_REQUEST)

        # Validate file size
        if file_obj.size > settings.MAX_UPLOAD_SIZE:
            return Response({'error': 'File too large'}, status=status.HTTP_400_BAD_REQUEST)

        # Validate file extension
        file_ext = os.path.splitext(file_obj.name)[1].lower()
        if file_ext not in settings.ALLOWED_FILE_EXTENSIONS:
            return Response({'error': 'File type not supported'}, status=status.HTTP_400_BAD_REQUEST)

        # Save file
        file_id = str(uuid.uuid4())
        file_path = os.path.join(settings.MEDIA_ROOT, 'datasets', f"{file_id}{file_ext}")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'wb') as f:
            for chunk in file_obj.chunks():
                f.write(chunk)

        # Check if async processing needed
        if file_obj.size > settings.ASYNC_FILE_SIZE_THRESHOLD:
            # Process asynchronously
            task = process_file_upload.delay(
                file_path=file_path,
                user_id=str(request.user.id),
                dataset_name=request.data.get('name', file_obj.name),
                original_filename=file_obj.name
            )
            
            return Response({
                'message': 'File upload started. Processing in background.',
                'task_id': task.id,
                'status_url': f'/api/tasks/{task.id}/status/',
            }, status=status.HTTP_202_ACCEPTED)
        
        else:
            # Process synchronously (existing code)
            try:
                # Parse file
                file_type = file_ext[1:]  # Remove dot
                data = FileParser.parse_file(file_path, file_type)
                schema = TypeDetector.detect_column_types(data)

                # Create dataset
                dataset = Dataset.objects.create(
                    user=request.user,
                    name=request.data.get('name', file_obj.name),
                    original_filename=file_obj.name,
                    file_path=file_path,
                    file_type=file_type,
                    file_size=file_obj.size,
                    schema=schema,
                    data=data,
                    row_count=len(data),
                    column_count=len(schema)
                )

                return Response(DatasetSerializer(dataset).data, status=status.HTTP_201_CREATED)

            except Exception as e:
                # Clean up file on error
                if os.path.exists(file_path):
                    os.remove(file_path)
                return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

class DatasetDetailView(generics.RetrieveDestroyAPIView):
    serializer_class = DatasetSerializer

    def get_queryset(self):
        return Dataset.objects.filter(user=self.request.user)

    def destroy(self, request, *args, **kwargs):
        dataset = self.get_object()
        # Delete file
        if os.path.exists(dataset.file_path):
            os.remove(dataset.file_path)
        return super().destroy(request, *args, **kwargs)

@api_view(['GET'])
def dataset_data(request, pk):
    try:
        dataset = Dataset.objects.get(pk=pk, user=request.user)
    except Dataset.DoesNotExist:
        return Response({'error': 'Dataset not found'}, status=status.HTTP_404_NOT_FOUND)

    page = int(request.GET.get('page', 1))
    page_size = int(request.GET.get('page_size', 50))
    
    paginator = Paginator(dataset.data, page_size)
    page_obj = paginator.get_page(page)
    
    return Response({
        'data': page_obj.object_list,
        'total_pages': paginator.num_pages,
        'current_page': page,
        'total_rows': len(dataset.data)
    })

@api_view(['GET'])
def cleanup_operations(request):
    """Get available cleanup operations"""
    operations = DataCleaner.get_operations()
    return Response(operations)

@api_view(['POST'])
def cleanup_preview(request, pk):
    """Preview cleanup changes without applying them"""
    try:
        dataset = Dataset.objects.get(pk=pk, user=request.user)
    except Dataset.DoesNotExist:
        return Response({'error': 'Dataset not found'}, status=status.HTTP_404_NOT_FOUND)

    serializer = CleanupPreviewSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    try:
        preview_result = DataCleaner.preview_cleanup(
            dataset.data,
            serializer.validated_data['operation'],
            serializer.validated_data.get('columns'),
            serializer.validated_data.get('options', {})
        )
        return Response(preview_result)
    except Exception as e:
        return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

@api_view(['POST'])
def cleanup_dataset(request, pk):
    try:
        dataset = Dataset.objects.get(pk=pk, user=request.user)
    except Dataset.DoesNotExist:
        return Response({'error': 'Dataset not found'}, status=status.HTTP_404_NOT_FOUND)

    serializer = CleanupOperationSerializer(data=request.data)
    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    # Check if async processing needed
    if dataset.row_count > settings.ASYNC_ROW_COUNT_THRESHOLD:
        # Process asynchronously
        task = cleanup_dataset_async.delay(
            dataset_id=str(dataset.id),
            operation=serializer.validated_data['operation'],
            columns=serializer.validated_data.get('columns'),
            options=serializer.validated_data.get('options', {})
        )
        
        return Response({
            'message': 'Cleanup started. Processing in background.',
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
            change_description=f"Applied {serializer.validated_data['operation']}"
        )

        # Apply cleanup
        try:
            cleaned_data, affected_count = DataCleaner.apply_cleanup(
                dataset.data.copy(),
                serializer.validated_data['operation'],
                serializer.validated_data.get('columns'),
                serializer.validated_data.get('options', {})
            )
            
            # Update dataset
            dataset.data = cleaned_data
            dataset.version += 1
            dataset.row_count = len(cleaned_data)
            dataset.save()

            return Response({
                'success': True,
                'message': f"Applied {serializer.validated_data['operation']} to {affected_count} rows",
                'affected_rows': affected_count,
                'new_version': dataset.version
            })

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

@api_view(['GET'])
def dataset_versions(request, pk):
    try:
        dataset = Dataset.objects.get(pk=pk, user=request.user)
    except Dataset.DoesNotExist:
        return Response({'error': 'Dataset not found'}, status=status.HTTP_404_NOT_FOUND)

    versions = DatasetVersion.objects.filter(dataset=dataset).order_by('-version_number')
    serializer = DatasetVersionSerializer(versions, many=True)
    return Response(serializer.data)

@api_view(['POST'])
def revert_dataset(request, pk, version):
    try:
        dataset = Dataset.objects.get(pk=pk, user=request.user)
        version_obj = DatasetVersion.objects.get(dataset=dataset, version_number=version)
    except (Dataset.DoesNotExist, DatasetVersion.DoesNotExist):
        return Response({'error': 'Dataset or version not found'}, status=status.HTTP_404_NOT_FOUND)

    # Create current version snapshot
    DatasetVersion.objects.create(
        dataset=dataset,
        version_number=dataset.version,
        schema_snapshot=dataset.schema,
        data_snapshot=dataset.data,
        change_description=f"Before reverting to version {version}"
    )

    # Revert to selected version
    dataset.schema = version_obj.schema_snapshot
    dataset.data = version_obj.data_snapshot
    dataset.version += 1
    dataset.row_count = len(dataset.data)
    dataset.save()

    return Response(DatasetSerializer(dataset).data)