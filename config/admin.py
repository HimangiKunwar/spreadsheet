from django.contrib import admin
from .admin_dashboard import smartsheet_admin

# Import all admin classes
from authentication.admin import UserAdmin
from datasets.admin import DatasetAdmin, DatasetVersionAdmin
from reconciliation.admin import ReconciliationJobAdmin
from bulk_operations.admin import BulkOperationAdmin, SavedRuleAdmin
from reports.admin import ReportAdmin, ReportTemplateAdmin

# Import all models
from authentication.models import User
from datasets.models import Dataset, DatasetVersion
from reconciliation.models import ReconciliationJob
from bulk_operations.models import BulkOperation, SavedRule
from reports.models import Report, ReportTemplate

# Register models with custom admin site
smartsheet_admin.register(User, UserAdmin)
smartsheet_admin.register(Dataset, DatasetAdmin)
smartsheet_admin.register(DatasetVersion, DatasetVersionAdmin)
smartsheet_admin.register(ReconciliationJob, ReconciliationJobAdmin)
smartsheet_admin.register(BulkOperation, BulkOperationAdmin)
smartsheet_admin.register(SavedRule, SavedRuleAdmin)
smartsheet_admin.register(Report, ReportAdmin)
smartsheet_admin.register(ReportTemplate, ReportTemplateAdmin)

# Customize the default admin site as well
admin.site.site_header = 'SmartSheet Pro Administration'
admin.site.site_title = 'SmartSheet Pro Admin'
admin.site.index_title = 'Welcome to SmartSheet Pro Administration'