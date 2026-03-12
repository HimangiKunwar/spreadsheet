import io
import uuid
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.http import HttpResponse

from .models import Report
from .serializers import ReportSerializer


class ReportViewSet(viewsets.ModelViewSet):
    """ViewSet for Report CRUD operations and download"""
    serializer_class = ReportSerializer
    permission_classes = [IsAuthenticated]
    lookup_field = 'pk'
    
    def get_queryset(self):
        return Report.objects.filter(user=self.request.user).order_by('-created_at')
    
    def perform_create(self, serializer):
        reconciliation = None
        reconciliation_id = self.request.data.get('reconciliation_id')
        if reconciliation_id:
            try:
                from reconciliation.models import ReconciliationJob
                reconciliation = ReconciliationJob.objects.get(
                    pk=reconciliation_id,
                    user=self.request.user
                )
            except ReconciliationJob.DoesNotExist:
                pass
        serializer.save(user=self.request.user, reconciliation=reconciliation)
    
    @action(detail=True, methods=['post'])
    def generate(self, request, pk=None):
        """Generate the report (PDF/Excel)"""
        try:
            report = self.get_object()
            
            # Update status to generating
            report.status = 'generating'
            report.save()
            
            # For now, mark as completed immediately
            # TODO: Add actual report generation logic here
            # This might involve:
            # - Fetching data from source/target datasets
            # - Processing reconciliation results
            # - Generating PDF using reportlab or Excel using openpyxl
            
            report.status = 'completed'
            report.save()
            
            # Send report ready notification
            try:
                from notifications.tasks import send_report_ready_email
                send_report_ready_email.delay(str(report.id))
            except ImportError:
                pass  # Notifications app not available
            
            return Response({
                'status': 'success',
                'message': 'Report generated successfully',
                'report_id': str(report.id)
            })
            
        except Exception as e:
            if 'report' in locals():
                report.status = 'failed'
                report.save()
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @action(detail=True, methods=['get', 'post'], url_path='download')
    def download(self, request, pk=None, format=None):
        """Download report as PDF or Excel"""
        print(f"=== DOWNLOAD FUNCTION CALLED === pk={pk}")
        
        report = self.get_object()
        
        # Get format from multiple sources
        if request.method == 'POST':
            format_type = request.data.get('format', 'pdf')
        else:
            format_type = (
                request.query_params.get('format') or  # Query parameter
                format or                              # URL format suffix
                'pdf'                                  # Default
            )
        print(f"Format requested: {format_type}")
        
        # Get reconciliation results if linked
        results = {}
        if report.reconciliation:
            results = report.reconciliation.results or {}
        
        try:
            if format_type == 'xlsx':
                return self._generate_excel(report, results)
            else:
                return self._generate_pdf(report, results)
        except Exception as e:
            print(f"Error generating {format_type}: {e}")
            return Response({'error': f'Failed to generate {format_type}'}, status=500)
    
    def _generate_pdf(self, report, results):
        """Generate PDF report"""
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
        
        output = io.BytesIO()
        p = canvas.Canvas(output, pagesize=letter)
        width, height = letter
        
        # Title
        p.setFont("Helvetica-Bold", 20)
        p.drawString(50, height - 50, f"Report: {report.name}")
        
        # Description
        p.setFont("Helvetica", 12)
        y_position = height - 80
        if report.description:
            p.drawString(50, y_position, f"Description: {report.description}")
            y_position -= 20
        
        # Status
        p.drawString(50, y_position, f"Status: {report.status}")
        y_position -= 20
        
        # Created date
        p.drawString(50, y_position, f"Created: {report.created_at.strftime('%Y-%m-%d %H:%M')}")
        y_position -= 40
        
        # Reconciliation summary if available
        if results:
            p.setFont("Helvetica-Bold", 14)
            p.drawString(50, y_position, "Reconciliation Summary")
            y_position -= 25
            
            p.setFont("Helvetica", 12)
            summary = results.get('summary', {})
            for key, value in summary.items():
                p.drawString(70, y_position, f"{key}: {value}")
                y_position -= 18
        
        p.save()
        output.seek(0)
        
        response = HttpResponse(output.getvalue(), content_type='application/pdf')
        response['Content-Disposition'] = f'attachment; filename="{report.name}.pdf"'
        return response
    
    def _generate_excel(self, report, results):
        """Generate Excel report"""
        import pandas as pd
        
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Summary sheet
            summary_data = {
                'Field': ['Report Name', 'Description', 'Status', 'Created'],
                'Value': [
                    report.name,
                    report.description or 'N/A',
                    report.status,
                    report.created_at.strftime('%Y-%m-%d %H:%M')
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Reconciliation data if available
            if results:
                summary = results.get('summary', {})
                if summary:
                    summary_df = pd.DataFrame([summary])
                    summary_df.to_excel(writer, sheet_name='Reconciliation Summary', index=False)
                
                # Add matches, mismatches sheets if data exists
                for key in ['matches', 'mismatches', 'source_only', 'target_only']:
                    data = results.get(key, [])
                    if data:
                        df = pd.DataFrame(data)
                        sheet_name = key.replace('_', ' ').title()[:31]  # Excel sheet name limit
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        output.seek(0)
        
        response = HttpResponse(
            output.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename="{report.name}.xlsx"'
        return response