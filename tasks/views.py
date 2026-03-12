"""
Task Status API
"""
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated

from .utils import get_task_progress, cancel_task


class TaskStatusView(APIView):
    """GET /api/tasks/{task_id}/status/"""
    permission_classes = [IsAuthenticated]
    
    def get(self, request, task_id):
        progress = get_task_progress(task_id)
        return Response(progress)


class TaskCancelView(APIView):
    """POST /api/tasks/{task_id}/cancel/"""
    permission_classes = [IsAuthenticated]
    
    def post(self, request, task_id):
        cancel_task(task_id)
        return Response({'message': 'Task cancelled', 'task_id': task_id})