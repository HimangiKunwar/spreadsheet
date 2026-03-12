from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from datasets.models import Dataset, DatasetVersion
from reconciliation.models import ReconciliationJob
from bulk_operations.models import BulkOperation, SavedRule
from reports.models import Report, ReportTemplate
import json
import uuid
from datetime import datetime, timedelta
from django.utils import timezone

User = get_user_model()

class Command(BaseCommand):
    help = 'Create sample data for admin interface testing'

    def add_arguments(self, parser):
        parser.add_argument(
            '--users',
            type=int,
            default=5,
            help='Number of sample users to create',
        )
        parser.add_argument(
            '--datasets',
            type=int,
            default=10,
            help='Number of sample datasets to create',
        )

    def handle(self, *args, **options):
        self.stdout.write('Creating sample data for SmartSheet Pro admin...')
        
        # Create sample users
        users = self.create_users(options['users'])
        self.stdout.write(f'Created {len(users)} users')
        
        # Create sample datasets
        datasets = self.create_datasets(users, options['datasets'])
        self.stdout.write(f'Created {len(datasets)} datasets')
        
        # Create sample reconciliation jobs
        reconciliations = self.create_reconciliations(users, datasets)
        self.stdout.write(f'Created {len(reconciliations)} reconciliation jobs')
        
        # Create sample bulk operations
        operations = self.create_bulk_operations(users, datasets)
        self.stdout.write(f'Created {len(operations)} bulk operations')
        
        # Create sample reports
        reports = self.create_reports(users, datasets)
        self.stdout.write(f'Created {len(reports)} reports')
        
        # Create sample report templates
        templates = self.create_report_templates()
        self.stdout.write(f'Created {len(templates)} report templates')
        
        self.stdout.write(
            self.style.SUCCESS('Successfully created sample data!')
        )

    def create_users(self, count):
        users = []
        for i in range(count):
            user, created = User.objects.get_or_create(
                email=f'user{i+1}@example.com',
                defaults={
                    'first_name': f'User{i+1}',
                    'last_name': 'Test',
                    'is_active': True,
                    'preferences': {'theme': 'light', 'notifications': True}
                }
            )
            if created:
                user.set_password('testpass123')
                user.save()
            users.append(user)
        return users

    def create_datasets(self, users, count):
        datasets = []
        file_types = ['csv', 'xlsx', 'json', 'tsv']
        
        for i in range(count):
            dataset = Dataset.objects.create(
                user=users[i % len(users)],
                name=f'Sample Dataset {i+1}',
                original_filename=f'sample_data_{i+1}.{file_types[i % len(file_types)]}',
                file_path=f'/media/datasets/sample_{i+1}.{file_types[i % len(file_types)]}',
                file_type=file_types[i % len(file_types)],
                file_size=1024 * (i + 1) * 10,  # Varying file sizes
                schema={
                    'columns': [
                        {'name': 'id', 'type': 'integer'},
                        {'name': 'name', 'type': 'string'},
                        {'name': 'email', 'type': 'email'},
                        {'name': 'amount', 'type': 'currency'}
                    ]
                },
                data=[
                    {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'amount': 100.50},
                    {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com', 'amount': 250.75}
                ],
                row_count=100 + i * 50,
                column_count=4,
                version=1
            )
            
            # Create a version for some datasets
            if i % 3 == 0:
                DatasetVersion.objects.create(
                    dataset=dataset,
                    version_number=1,
                    schema_snapshot=dataset.schema,
                    data_snapshot=dataset.data,
                    change_description='Initial version'
                )
            
            datasets.append(dataset)
        return datasets

    def create_reconciliations(self, users, datasets):
        reconciliations = []
        statuses = ['completed', 'pending', 'running', 'failed']
        
        for i in range(min(5, len(datasets) // 2)):
            reconciliation = ReconciliationJob.objects.create(
                user=users[i % len(users)],
                name=f'Reconciliation Job {i+1}',
                source_dataset=datasets[i * 2] if i * 2 < len(datasets) else datasets[0],
                target_dataset=datasets[i * 2 + 1] if i * 2 + 1 < len(datasets) else datasets[1],
                source_key_columns=['id'],
                target_key_columns=['id'],
                compare_columns=['name', 'email'],
                fuzzy_match=i % 2 == 0,
                fuzzy_threshold=80,
                status=statuses[i % len(statuses)],
                results={'matches': 50, 'mismatches': 10, 'source_only': 5, 'target_only': 3},
                summary={'total_compared': 68, 'match_rate': 73.5}
            )
            
            if reconciliation.status == 'completed':
                reconciliation.completed_at = timezone.now() - timedelta(hours=i)
                reconciliation.save()
            
            reconciliations.append(reconciliation)
        return reconciliations

    def create_bulk_operations(self, users, datasets):
        operations = []
        
        for i in range(min(8, len(datasets))):
            # Create a saved rule first
            rule = SavedRule.objects.create(
                user=users[i % len(users)],
                dataset=datasets[i],
                name=f'Rule {i+1}',
                description=f'Sample rule for testing {i+1}',
                configuration={
                    'conditions': [{'column': 'amount', 'operator': 'greater_than', 'value': 100}],
                    'action': {'type': 'set_value', 'column': 'status', 'value': 'high_value'}
                },
                use_count=i + 1
            )
            
            # Create bulk operation
            operation = BulkOperation.objects.create(
                user=users[i % len(users)],
                dataset=datasets[i],
                saved_rule=rule,
                rule_config=rule.configuration,
                affected_rows=10 + i * 5,
                affected_indices=[1, 3, 5, 7, 9],
                undo_data={'original_values': [{'row': 1, 'column': 'status', 'value': 'normal'}]},
                dataset_version_before=1,
                dataset_version_after=2,
                is_undone=i % 4 == 0
            )
            operations.append(operation)
        return operations

    def create_reports(self, users, datasets):
        reports = []
        statuses = ['completed', 'generating', 'draft', 'failed']
        
        for i in range(min(6, len(datasets))):
            report = Report.objects.create(
                user=users[i % len(users)],
                dataset=datasets[i],
                name=f'Report {i+1}',
                description=f'Sample report for dataset {datasets[i].name}',
                configuration={
                    'charts': [{'type': 'bar', 'column': 'amount'}],
                    'summaries': ['count', 'sum', 'average']
                },
                status=statuses[i % len(statuses)],
                pdf_path=f'/media/reports/report_{i+1}.pdf' if i % 2 == 0 else '',
                xlsx_path=f'/media/reports/report_{i+1}.xlsx' if i % 3 == 0 else ''
            )
            
            if report.status == 'completed':
                report.generated_at = timezone.now() - timedelta(hours=i)
                report.save()
            
            reports.append(report)
        return reports

    def create_report_templates(self):
        templates = []
        categories = ['Financial', 'Operational', 'Analytics']
        
        template_configs = [
            {
                'name': 'Financial Summary',
                'category': 'Financial',
                'config': {'charts': [{'type': 'pie', 'column': 'category'}], 'summaries': ['sum', 'average']},
                'required_columns': ['amount', 'category']
            },
            {
                'name': 'Data Quality Report',
                'category': 'Operational',
                'config': {'charts': [{'type': 'bar', 'column': 'status'}], 'summaries': ['count']},
                'required_columns': ['status']
            },
            {
                'name': 'Trend Analysis',
                'category': 'Analytics',
                'config': {'charts': [{'type': 'line', 'column': 'date'}], 'summaries': ['count', 'trend']},
                'required_columns': ['date', 'value']
            }
        ]
        
        for template_config in template_configs:
            template = ReportTemplate.objects.create(
                name=template_config['name'],
                category=template_config['category'],
                description=f'Template for {template_config["name"].lower()}',
                configuration=template_config['config'],
                required_columns=template_config['required_columns'],
                is_public=True
            )
            templates.append(template)
        
        return templates