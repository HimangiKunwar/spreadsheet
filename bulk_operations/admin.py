from django.contrib import admin
from django.utils.html import format_html
from .models import BulkOperation, SavedRule

@admin.register(BulkOperation)
class BulkOperationAdmin(admin.ModelAdmin):
    list_display = ('dataset', 'user', 'affected_rows', 'is_undone', 'version_info', 'created_at')
    list_filter = ('is_undone', 'created_at', 'user')
    search_fields = ('user__email', 'dataset__name')
    readonly_fields = ('id', 'affected_rows', 'affected_indices', 'undo_data', 'dataset_version_before', 'dataset_version_after', 'created_at')
    ordering = ('-created_at',)
    date_hierarchy = 'created_at'
    
    fieldsets = (
        ('Basic Info', {
            'fields': ('id', 'user', 'dataset', 'saved_rule')
        }),
        ('Operation Details', {
            'fields': ('rule_config', 'affected_rows', 'affected_indices'),
            'classes': ('collapse',)
        }),
        ('Version Control', {
            'fields': ('dataset_version_before', 'dataset_version_after', 'is_undone')
        }),
        ('Undo Data', {
            'fields': ('undo_data',),
            'classes': ('collapse',)
        }),
        ('Timestamp', {
            'fields': ('created_at',)
        }),
    )
    
    def version_info(self, obj):
        return f"v{obj.dataset_version_before} → v{obj.dataset_version_after}"
    version_info.short_description = 'Version Change'
    
    actions = ['undo_operations']
    
    @admin.action(description='Undo selected operations')
    def undo_operations(self, request, queryset):
        undoable = queryset.filter(is_undone=False)
        count = 0
        for operation in undoable:
            # Here you would implement the actual undo logic
            operation.is_undone = True
            operation.save()
            count += 1
        self.message_user(request, f'{count} operations marked as undone.')
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('user', 'dataset', 'saved_rule')

@admin.register(SavedRule)
class SavedRuleAdmin(admin.ModelAdmin):
    list_display = ('name', 'user', 'dataset', 'use_count', 'last_used_at', 'created_at')
    list_filter = ('created_at', 'last_used_at', 'user')
    search_fields = ('name', 'description', 'user__email', 'dataset__name')
    readonly_fields = ('id', 'use_count', 'last_used_at', 'created_at', 'updated_at')
    ordering = ('-use_count', '-created_at')
    
    fieldsets = (
        ('Basic Info', {
            'fields': ('id', 'user', 'dataset', 'name', 'description')
        }),
        ('Rule Configuration', {
            'fields': ('configuration',),
            'classes': ('collapse',)
        }),
        ('Usage Stats', {
            'fields': ('use_count', 'last_used_at')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at')
        }),
    )
    
    actions = ['reset_usage_count']
    
    @admin.action(description='Reset usage count for selected rules')
    def reset_usage_count(self, request, queryset):
        count = queryset.update(use_count=0, last_used_at=None)
        self.message_user(request, f'Usage count reset for {count} rules.')
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('user', 'dataset')
