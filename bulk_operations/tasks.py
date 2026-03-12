"""
Async Tasks for Bulk Operations
"""
from celery import shared_task
from tasks.base import ChunkedTask
from .models import SavedRule, BulkOperation
from .services.rule_engine import RuleEngine
# from .services import RuleEngine
from datasets.models import Dataset
from datasets.services.type_detector import TypeDetector
# from datasets.services import TypeDetector


@shared_task(bind=True, base=ChunkedTask)
def execute_bulk_async(self, dataset_id, user_id, rule_config, save_as_rule=False, rule_name='', rule_description=''):
    """
    Execute bulk operation asynchronously
    """
    from authentication.models import User
    
    try:
        dataset = Dataset.objects.get(id=dataset_id)
        user = User.objects.get(id=user_id)
        
        self.update_progress(10, 100, 'Preparing operation...')
        
        # Save version before
        dataset.save_version("Before bulk operation")
        version_before = dataset.version
        
        self.update_progress(25, 100, 'Executing rules...')
        
        # Execute
        engine = RuleEngine(dataset.data)
        modified_data, undo_data = engine.execute(rule_config)
        
        self.update_progress(60, 100, 'Updating dataset...')
        
        dataset.data = modified_data
        dataset.row_count = len(modified_data)
        dataset.version += 1
        dataset.save()
        
        # Re-detect schema
        self.update_progress(75, 100, 'Re-detecting schema...')
        detector = TypeDetector(modified_data)
        dataset.schema = detector.detect_all()
        dataset.save()
        
        # Save rule if requested
        saved_rule = None
        if save_as_rule and rule_name:
            saved_rule = SavedRule.objects.create(
                user=user,
                name=rule_name,
                description=rule_description,
                configuration=rule_config,
            )
        
        # Record history
        self.update_progress(90, 100, 'Recording history...')
        operation = BulkOperation.objects.create(
            user=user,
            dataset=dataset,
            saved_rule=saved_rule,
            rule_config=rule_config,
            affected_rows=len(undo_data.get('affected_indices', [])),
            affected_indices=undo_data.get('affected_indices', []),
            undo_data=undo_data,
            dataset_version_before=version_before,
            dataset_version_after=dataset.version,
        )
        
        self.update_progress(100, 100, 'Complete!')
        
        return {
            'status': 'success',
            'operation_id': str(operation.id),
            'affected_rows': operation.affected_rows,
            'new_version': dataset.version,
        }
        
    except Exception as e:
        raise