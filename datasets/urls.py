from django.urls import path
from . import views

urlpatterns = [
    path('', views.DatasetListCreateView.as_view(), name='dataset-list-create'),
    path('<uuid:pk>/', views.DatasetDetailView.as_view(), name='dataset-detail'),
    path('<uuid:pk>/data/', views.dataset_data, name='dataset-data'),
    path('cleanup-operations/', views.cleanup_operations, name='cleanup-operations'),
    path('<uuid:pk>/cleanup/preview/', views.cleanup_preview, name='cleanup-preview'),
    path('<uuid:pk>/cleanup/', views.cleanup_dataset, name='cleanup-dataset'),
    path('<uuid:pk>/versions/', views.dataset_versions, name='dataset-versions'),
    path('<uuid:pk>/revert/<int:version>/', views.revert_dataset, name='revert-dataset'),
]