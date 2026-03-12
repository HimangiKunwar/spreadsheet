from django.contrib import admin
from django.utils.html import format_html
from .models import Dataset, DatasetVersion

class DatasetVersionInline(admin.TabularInline):
    model = DatasetVersion
    extra = 0
    readonly_fields = ('version_number', 'change_description', 'created_at')
    can_delete = False
    
    def has_add_permission(self, request, obj=None):
        return False

@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ('name', 'user', 'file_type', 'formatted_size', 'row_count', 'column_count', 'version', 'created_at')
    list_filter = ('file_type', 'created_at', 'user')
    search_fields = ('name', 'original_filename', 'user__email')
    readonly_fields = ('id', 'file_path', 'file_size', 'row_count', 'column_count', 'schema', 'data', 'created_at', 'updated_at')
    ordering = ('-created_at',)
    date_hierarchy = 'created_at'
    inlines = [DatasetVersionInline]
    
    fieldsets = (
        ('Basic Info', {
            'fields': ('id', 'user', 'name', 'original_filename')
        }),
        ('File Details', {
            'fields': ('file_type', 'file_path', 'file_size')
        }),
        ('Data Info', {
            'fields': ('row_count', 'column_count', 'version', 'schema'),
            'classes': ('collapse',)
        }),
        ('Data Content', {
            'fields': ('data',),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at')
        }),
    )
    
    def formatted_size(self, obj):
        if obj.file_size < 1024:
            return f"{obj.file_size} B"
        elif obj.file_size < 1024 * 1024:
            return f"{obj.file_size / 1024:.1f} KB"
        else:
            return f"{obj.file_size / (1024 * 1024):.1f} MB"
    formatted_size.short_description = 'Size'
    
    actions = ['delete_with_files']
    
    @admin.action(description='Delete selected datasets with files')
    def delete_with_files(self, request, queryset):
        import os
        count = 0
        for dataset in queryset:
            if dataset.file_path and os.path.exists(dataset.file_path):
                try:
                    os.remove(dataset.file_path)
                except OSError:
                    pass
            count += 1
        queryset.delete()
        self.message_user(request, f'{count} datasets and their files deleted.')

@admin.register(DatasetVersion)
class DatasetVersionAdmin(admin.ModelAdmin):
    list_display = ('dataset', 'version_number', 'change_description', 'created_at')
    list_filter = ('created_at',)
    search_fields = ('dataset__name', 'change_description')
    readonly_fields = ('id', 'dataset', 'version_number', 'schema_snapshot', 'data_snapshot', 'created_at')
    ordering = ('-created_at',)
    
    fieldsets = (
        ('Version Info', {
            'fields': ('id', 'dataset', 'version_number', 'change_description')
        }),
        ('Schema Snapshot', {
            'fields': ('schema_snapshot',),
            'classes': ('collapse',)
        }),
        ('Data Snapshot', {
            'fields': ('data_snapshot',),
            'classes': ('collapse',)
        }),
        ('Timestamp', {
            'fields': ('created_at',)
        }),
    )
