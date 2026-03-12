"""
Async Tasks for Report Generation
"""
import os
import uuid
from celery import shared_task
from django.conf import settings
from django.utils import timezone

from tasks.base import ProgressTask
from .models import Report
from .services.pdf_generator import PDFGenerator
# from .services import PDFGenerator


@shared_task(bind=True, base=ProgressTask)
def generate_pdf_async(self, report_id, user_id):
    """
    Generate PDF report asynchronously
    """
    try:
        report = Report.objects.get(id=report_id)
        report.status = 'generating'
        report.save()
        
        self.update_progress(10, 100, 'Loading data...')
        
        data = report.dataset.data if report.dataset else []
        
        # Create output directory
        output_dir = os.path.join(settings.MEDIA_ROOT, 'reports', str(user_id))
        os.makedirs(output_dir, exist_ok=True)
        
        filename = f"{report.name.replace(' ', '_')}_{uuid.uuid4().hex[:8]}.pdf"
        filepath = os.path.join(output_dir, filename)
        
        self.update_progress(30, 100, 'Generating PDF...')
        
        generator = PDFGenerator(data, report.configuration)
        generator.generate(filepath)
        
        self.update_progress(90, 100, 'Saving...')
        
        report.pdf_path = filepath
        report.status = 'completed'
        report.generated_at = timezone.now()
        report.save()
        
        self.update_progress(100, 100, 'Complete!')
        
        return {
            'status': 'success',
            'report_id': str(report_id),
            'download_url': f'/api/reports/{report_id}/download/?format=pdf',
        }
        
    except Exception as e:
        try:
            report = Report.objects.get(id=report_id)
            report.status = 'failed'
            report.error_message = str(e)
            report.save()
        except:
            pass
        raise