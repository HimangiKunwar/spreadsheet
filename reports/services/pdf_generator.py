from reportlab.lib.pagesizes import letter, A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
from reportlab.lib.enums import TA_CENTER, TA_LEFT
import os

class PDFGenerator:
    @staticmethod
    def generate_pdf(report_config, data, output_path):
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Page settings
        page_settings = report_config.get('page_settings', {})
        page_size = letter if page_settings.get('size') == 'letter' else A4
        
        doc = SimpleDocTemplate(
            output_path,
            pagesize=page_size,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=18
        )
        
        # Build story
        story = []
        styles = getSampleStyleSheet()
        
        # Custom styles
        branding = report_config.get('branding', {})
        primary_color = branding.get('primary_color', '#3B82F6')
        
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            textColor=colors.HexColor(primary_color),
            alignment=TA_CENTER
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=16,
            spaceAfter=12,
            textColor=colors.HexColor(primary_color)
        )
        
        # Add logo if provided
        logo_path = branding.get('logo_path')
        if logo_path and os.path.exists(logo_path):
            try:
                logo = Image(logo_path, width=2*inch, height=1*inch)
                logo.hAlign = 'CENTER'
                story.append(logo)
                story.append(Spacer(1, 12))
            except:
                pass
        
        # Add report title
        report_title = report_config.get('title', 'Report')
        story.append(Paragraph(report_title, title_style))
        story.append(Spacer(1, 20))
        
        # Process sections
        sections = report_config.get('sections', [])
        
        for section in sections:
            section_type = section.get('type')
            
            if section_type == 'title':
                story.append(Paragraph(section['content'], heading_style))
                story.append(Spacer(1, 12))
            
            elif section_type == 'text':
                story.append(Paragraph(section['content'], styles['Normal']))
                story.append(Spacer(1, 12))
            
            elif section_type == 'chart':
                # Chart should be generated separately and path provided
                chart_path = section.get('chart_path')
                if chart_path and os.path.exists(chart_path):
                    try:
                        chart_img = Image(chart_path, width=6*inch, height=4*inch)
                        chart_img.hAlign = 'CENTER'
                        story.append(chart_img)
                        story.append(Spacer(1, 12))
                    except:
                        story.append(Paragraph("Chart could not be loaded", styles['Normal']))
                        story.append(Spacer(1, 12))
            
            elif section_type == 'summary':
                summary_data = section.get('data', [])
                if summary_data:
                    table_data = PDFGenerator._format_summary_table(summary_data)
                    if table_data:
                        table = Table(table_data)
                        table.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(primary_color)),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('FONTSIZE', (0, 0), (-1, 0), 12),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                            ('GRID', (0, 0), (-1, -1), 1, colors.black)
                        ]))
                        story.append(table)
                        story.append(Spacer(1, 12))
            
            elif section_type == 'table':
                columns = section.get('columns', [])
                max_rows = section.get('max_rows', 100)
                
                if columns and data:
                    table_data = PDFGenerator._format_data_table(data, columns, max_rows)
                    if table_data:
                        table = Table(table_data)
                        table.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(primary_color)),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('FONTSIZE', (0, 0), (-1, 0), 10),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                            ('GRID', (0, 0), (-1, -1), 1, colors.black),
                            ('FONTSIZE', (0, 1), (-1, -1), 8)
                        ]))
                        story.append(table)
                        
                        if len(data) > max_rows:
                            story.append(Spacer(1, 6))
                            story.append(Paragraph(f"Note: Showing first {max_rows} rows of {len(data)} total rows", styles['Italic']))
                        
                        story.append(Spacer(1, 12))
            
            elif section_type == 'page_break':
                story.append(PageBreak())
        
        # Build PDF
        doc.build(story)
        return output_path

    @staticmethod
    def _format_summary_table(summary_data):
        if not summary_data:
            return []
        
        # Convert summary data to table format
        headers = ['Metric']
        data_rows = []
        
        # Get all columns
        columns = list(summary_data.keys())
        headers.extend(columns)
        
        # Get all metrics
        if columns:
            metrics = list(summary_data[columns[0]].keys())
            
            for metric in metrics:
                row = [metric.title()]
                for column in columns:
                    value = summary_data[column].get(metric, 'N/A')
                    if isinstance(value, float):
                        row.append(f"{value:.2f}")
                    else:
                        row.append(str(value))
                data_rows.append(row)
        
        return [headers] + data_rows

    @staticmethod
    def _format_data_table(data, columns, max_rows):
        if not data or not columns:
            return []
        
        # Filter data to only include specified columns
        filtered_data = []
        for row in data[:max_rows]:
            filtered_row = []
            for col in columns:
                value = row.get(col, '')
                if value is None:
                    value = ''
                filtered_row.append(str(value)[:50])  # Truncate long values
            filtered_data.append(filtered_row)
        
        # Add headers
        headers = [col.replace('_', ' ').title() for col in columns]
        return [headers] + filtered_data