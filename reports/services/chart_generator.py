try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import pandas as pd
    HAS_CHART_DEPENDENCIES = True
except ImportError:
    HAS_CHART_DEPENDENCIES = False
import base64
import io
import os

class ChartGenerator:
    @staticmethod
    def generate_chart(data, chart_type, x_column, y_column, title="", save_path=None):
        if not HAS_CHART_DEPENDENCIES:
            raise ImportError("Chart dependencies not installed. Please install matplotlib and pandas.")
            
        df = pd.DataFrame(data)
        
        plt.figure(figsize=(10, 6))
        plt.style.use('default')
        
        if chart_type == 'bar':
            ChartGenerator._create_bar_chart(df, x_column, y_column, title)
        elif chart_type == 'horizontal_bar':
            ChartGenerator._create_horizontal_bar_chart(df, x_column, y_column, title)
        elif chart_type == 'line':
            ChartGenerator._create_line_chart(df, x_column, y_column, title)
        elif chart_type == 'pie':
            ChartGenerator._create_pie_chart(df, x_column, y_column, title)
        elif chart_type == 'area':
            ChartGenerator._create_area_chart(df, x_column, y_column, title)
        elif chart_type == 'scatter':
            ChartGenerator._create_scatter_chart(df, x_column, y_column, title)
        else:
            raise ValueError(f"Unsupported chart type: {chart_type}")
        
        plt.tight_layout()
        
        if save_path:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            plt.close()
            return save_path
        else:
            # Return base64 encoded image
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode()
            plt.close()
            return f"data:image/png;base64,{image_base64}"

    @staticmethod
    def _create_bar_chart(df, x_column, y_column, title):
        # Group by x_column and sum y_column
        grouped = df.groupby(x_column)[y_column].sum().sort_values(ascending=False)
        grouped.plot(kind='bar', color='steelblue')
        plt.title(title or f'{y_column} by {x_column}')
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        plt.xticks(rotation=45)

    @staticmethod
    def _create_horizontal_bar_chart(df, x_column, y_column, title):
        grouped = df.groupby(x_column)[y_column].sum().sort_values(ascending=True)
        grouped.plot(kind='barh', color='steelblue')
        plt.title(title or f'{y_column} by {x_column}')
        plt.xlabel(y_column)
        plt.ylabel(x_column)

    @staticmethod
    def _create_line_chart(df, x_column, y_column, title):
        grouped = df.groupby(x_column)[y_column].sum().sort_index()
        grouped.plot(kind='line', marker='o', color='steelblue')
        plt.title(title or f'{y_column} over {x_column}')
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        plt.xticks(rotation=45)

    @staticmethod
    def _create_pie_chart(df, x_column, y_column, title):
        grouped = df.groupby(x_column)[y_column].sum()
        plt.pie(grouped.values, labels=grouped.index, autopct='%1.1f%%', startangle=90)
        plt.title(title or f'{y_column} distribution by {x_column}')
        plt.axis('equal')

    @staticmethod
    def _create_area_chart(df, x_column, y_column, title):
        grouped = df.groupby(x_column)[y_column].sum().sort_index()
        grouped.plot(kind='area', alpha=0.7, color='steelblue')
        plt.title(title or f'{y_column} over {x_column}')
        plt.xlabel(x_column)
        plt.ylabel(y_column)
        plt.xticks(rotation=45)

    @staticmethod
    def _create_scatter_chart(df, x_column, y_column, title):
        plt.scatter(df[x_column], df[y_column], alpha=0.6, color='steelblue')
        plt.title(title or f'{y_column} vs {x_column}')
        plt.xlabel(x_column)
        plt.ylabel(y_column)