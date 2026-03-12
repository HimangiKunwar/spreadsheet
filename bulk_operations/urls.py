from django.urls import path
from . import views

urlpatterns = [
    path('rules/', views.SavedRuleListCreateView.as_view(), name='saved-rules-list-create'),
    path('rules/<uuid:pk>/', views.SavedRuleDetailView.as_view(), name='saved-rule-detail'),
    path('rules/<uuid:rule_id>/apply/', views.apply_saved_rule, name='apply-saved-rule'),
    path('operations/', views.BulkOperationListView.as_view(), name='bulk-operations-list'),
    path('<uuid:dataset_id>/preview/', views.preview_rule, name='preview-rule'),
    path('<uuid:dataset_id>/execute/', views.execute_rule, name='execute-rule'),
    path('undo/<uuid:operation_id>/', views.undo_operation, name='undo-operation'),
    path('<uuid:dataset_id>/history/', views.dataset_history, name='dataset-history'),
]