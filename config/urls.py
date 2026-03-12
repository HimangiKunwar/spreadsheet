from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from .admin_dashboard import smartsheet_admin
from . import admin as custom_admin  # Import to register models

urlpatterns = [
    path('admin/', admin.site.urls),
    path('smartsheet-admin/', smartsheet_admin.urls),  # Custom admin with dashboard
    path('api/auth/', include('authentication.urls')),
    path('api/datasets/', include('datasets.urls')),
    path('api/reconcile/', include('reconciliation.urls')),
    path('api/bulk/', include('bulk_operations.urls')),
    path('api/reports/', include('reports.urls')),
    path('api/tasks/', include('tasks.urls')),
    path('api/', include('workflows.urls')),
    path('api/', include('notifications.urls')),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)