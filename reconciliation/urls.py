from django.urls import path
from . import views

urlpatterns = [
    path('', views.ReconciliationJobListCreateView.as_view(), name='reconciliation-list-create'),
    path('<uuid:pk>/', views.ReconciliationJobDetailView.as_view(), name='reconciliation-detail'),
    path('<uuid:pk>/results/', views.reconciliation_results, name='reconciliation-results'),
    path('<uuid:pk>/export/', views.export_results, name='export-results'),
]