from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import ReportViewSet

# Create router
router = DefaultRouter()

# Register the viewset
router.register(r'', ReportViewSet, basename='report')

# Get the viewset instance for the download action
report_download = ReportViewSet.as_view({'get': 'download', 'post': 'download'})

urlpatterns = [
    # Explicit download URL (add this BEFORE router.urls)
    path('<uuid:pk>/download/', report_download, name='report-download'),
    # Router URLs
    path('', include(router.urls)),
]