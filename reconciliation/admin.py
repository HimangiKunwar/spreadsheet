from django.contrib import admin
from django.utils.html import format_html
from .models import ReconciliationJob

@admin.register(ReconciliationJob)
class ReconciliationJobAdmin(admin.ModelAdmin):
    list_display = ('name', 'user', 'source_dataset', 'target_dataset', 'status', 'fuzzy_match', 'created_at', 'completed_at')
    list_filter = ('status', 'fuzzy_match', 'created_at')
    search_fields = ('name', 'user__email', 'source_dataset__name', 'target_dataset__name')
    readonly_fields = ('id', 'task_id', 'results', 'summary', 'created_at', 'completed_at')
    ordering = ('-created_at',)
    date_hierarchy = 'created_at'
    
    fieldsets = (
        ('Basic Info', {
            'fields': ('id', 'user', 'name', 'status')
        }),
        ('Datasets', {
            'fields': ('source_dataset', 'target_dataset')
        }),
        ('Matching Configuration', {
            'fields': ('source_key_columns', 'target_key_columns', 'compare_columns', 'fuzzy_match', 'fuzzy_threshold'),
            'classes': ('collapse',)
        }),
        ('Task Info', {
            'fields': ('task_id', 'error_message'),
            'classes': ('collapse',)
        }),
        ('Results', {
            'fields': ('results', 'summary'),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'completed_at')
        }),
    )
    
    actions = ['restart_failed_jobs']
    
    @admin.action(description='Restart failed reconciliation jobs')
    def restart_failed_jobs(self, request, queryset):
        failed_jobs = queryset.filter(status='failed')
        count = failed_jobs.update(status='pending', error_message='', task_id='')
        self.message_user(request, f'{count} failed jobs restarted.')
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('user', 'source_dataset', 'target_dataset')
