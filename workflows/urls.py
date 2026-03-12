from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import WorkflowViewSet, WorkflowRunViewSet

router = DefaultRouter()
router.register(r'workflows', WorkflowViewSet, basename='workflow')
router.register(r'workflow-runs', WorkflowRunViewSet, basename='workflow-run')

urlpatterns = [
    path('', include(router.urls)),
]