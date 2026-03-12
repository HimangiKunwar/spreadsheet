from django.contrib import admin
from django.urls import path
from django.shortcuts import render
from django.contrib.auth import get_user_model
from django.db.models import Count, Sum
from django.utils import timezone
from datetime import timedelta

User = get_user_model()

class SmartSheetAdminSite(admin.AdminSite):
    site_header = 'SmartSheet Pro Administration'
    site_title = 'SmartSheet Pro Admin'
    index_title = 'Dashboard'
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('dashboard/', self.admin_view(self.dashboard_view), name='dashboard'),
        ]
        return custom_urls + urls
    
    def dashboard_view(self, request):
        from datasets.models import Dataset
        from reconciliation.models import ReconciliationJob
        from reports.models import Report
        from bulk_operations.models import BulkOperation
        
        # Calculate date ranges
        now = timezone.now()
        last_30_days = now - timedelta(days=30)
        last_7_days = now - timedelta(days=7)
        
        # Basic stats
        total_users = User.objects.count()
        active_users = User.objects.filter(last_login__gte=last_30_days).count()
        total_datasets = Dataset.objects.count()
        total_reconciliations = ReconciliationJob.objects.count()
        total_reports = Report.objects.count()
        total_operations = BulkOperation.objects.count()
        
        # Recent activity
        recent_datasets = Dataset.objects.select_related('user').order_by('-created_at')[:5]
        recent_users = User.objects.order_by('-created_at')[:5]
        recent_reconciliations = ReconciliationJob.objects.select_related('user', 'source_dataset', 'target_dataset').order_by('-created_at')[:5]
        
        # File type distribution
        file_type_stats = Dataset.objects.values('file_type').annotate(count=Count('id')).order_by('-count')
        
        # Status distribution for reconciliations
        reconciliation_status_stats = ReconciliationJob.objects.values('status').annotate(count=Count('id'))
        
        # Report status distribution
        report_status_stats = Report.objects.values('status').annotate(count=Count('id'))
        
        # Data size stats
        total_data_size = Dataset.objects.aggregate(total_size=Sum('file_size'))['total_size'] or 0
        
        context = {
            **self.each_context(request),
            'total_users': total_users,
            'active_users': active_users,
            'total_datasets': total_datasets,
            'total_reconciliations': total_reconciliations,
            'total_reports': total_reports,
            'total_operations': total_operations,
            'total_data_size': self.format_file_size(total_data_size),
            'recent_datasets': recent_datasets,
            'recent_users': recent_users,
            'recent_reconciliations': recent_reconciliations,
            'file_type_stats': file_type_stats,
            'reconciliation_status_stats': reconciliation_status_stats,
            'report_status_stats': report_status_stats,
        }
        return render(request, 'admin/dashboard.html', context)
    
    def format_file_size(self, size_bytes):
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"

# Create custom admin site instance
smartsheet_admin = SmartSheetAdminSite(name='smartsheet_admin')