from django.contrib import admin
from django.utils.html import format_html
from .models import Report, ReportTemplate

@admin.register(Report)
class ReportAdmin(admin.ModelAdmin):
    list_display = ('name', 'user', 'dataset', 'reconciliation', 'status', 'is_template', 'download_links', 'created_at')
    list_filter = ('status', 'is_template', 'created_at')
    search_fields = ('name', 'user__email', 'dataset__name')
    readonly_fields = ('id', 'pdf_path', 'pptx_path', 'xlsx_path', 'created_at', 'updated_at', 'generated_at')
    ordering = ('-created_at',)
    date_hierarchy = 'created_at'
    
    fieldsets = (
        ('Basic Info', {
            'fields': ('id', 'user', 'name', 'description', 'is_template')
        }),
        ('Data Sources', {
            'fields': ('dataset', 'reconciliation')
        }),
        ('Report Configuration', {
            'fields': ('configuration',),
            'classes': ('collapse',)
        }),
        ('Output Files', {
            'fields': ('status', 'pdf_path', 'pptx_path', 'xlsx_path', 'error_message')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at', 'generated_at')
        }),
    )
    
    def download_links(self, obj):
        links = []
        if obj.pdf_path:
            links.append(f'<a href="/api/reports/{obj.id}/download/?format=pdf" target="_blank">PDF</a>')
        if obj.xlsx_path:
            links.append(f'<a href="/api/reports/{obj.id}/download/?format=xlsx" target="_blank">Excel</a>')
        if obj.pptx_path:
            links.append(f'<a href="/api/reports/{obj.id}/download/?format=pptx" target="_blank">PowerPoint</a>')
        return format_html(' | '.join(links)) if links else '-'
    download_links.short_description = 'Downloads'
    
    actions = ['regenerate_reports', 'mark_as_template']
    
    @admin.action(description='Regenerate selected reports')
    def regenerate_reports(self, request, queryset):
        count = queryset.update(status='draft', error_message='', generated_at=None)
        self.message_user(request, f'{count} reports marked for regeneration.')
    
    @admin.action(description='Mark selected reports as templates')
    def mark_as_template(self, request, queryset):
        count = queryset.update(is_template=True)
        self.message_user(request, f'{count} reports marked as templates.')
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('user', 'dataset', 'reconciliation')

@admin.register(ReportTemplate)
class ReportTemplateAdmin(admin.ModelAdmin):
    list_display = ('name', 'category', 'is_public', 'created_at')
    list_filter = ('category', 'is_public', 'created_at')
    search_fields = ('name', 'description', 'category')
    readonly_fields = ('id', 'created_at')
    ordering = ('category', 'name')
    
    fieldsets = (
        ('Basic Info', {
            'fields': ('id', 'name', 'description', 'category', 'is_public')
        }),
        ('Template Configuration', {
            'fields': ('configuration', 'required_columns'),
            'classes': ('collapse',)
        }),
        ('Display', {
            'fields': ('thumbnail',)
        }),
        ('Timestamp', {
            'fields': ('created_at',)
        }),
    )
    
    actions = ['make_public', 'make_private']
    
    @admin.action(description='Make selected templates public')
    def make_public(self, request, queryset):
        count = queryset.update(is_public=True)
        self.message_user(request, f'{count} templates made public.')
    
    @admin.action(description='Make selected templates private')
    def make_private(self, request, queryset):
        count = queryset.update(is_public=False)
        self.message_user(request, f'{count} templates made private.')
