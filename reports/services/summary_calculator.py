try:
    import pandas as pd
    import numpy as np
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

class SummaryCalculator:
    @staticmethod
    def calculate_summary(data, columns=None, metrics=None, group_by=None):
        if not HAS_PANDAS:
            # Simple implementation without pandas
            return SummaryCalculator._calculate_summary_simple(data, columns, metrics, group_by)
            
        df = pd.DataFrame(data)
        
        if columns is None:
            # Auto-detect numeric columns
            if HAS_PANDAS:
                columns = df.select_dtypes(include=[np.number]).columns.tolist()
            else:
                # Simple numeric detection
                columns = []
                if data:
                    for key, value in data[0].items():
                        try:
                            float(value)
                            columns.append(key)
                        except:
                            pass
        
        if metrics is None:
            metrics = ['count', 'sum', 'mean', 'median', 'min', 'max', 'std', 'unique']
        
        results = {}
        
        if group_by:
            # Grouped summary
            for column in columns:
                if column in df.columns:
                    grouped = df.groupby(group_by)[column]
                    column_stats = {}
                    
                    for metric in metrics:
                        try:
                            if metric == 'count':
                                column_stats[metric] = grouped.count().to_dict()
                            elif metric == 'sum':
                                column_stats[metric] = grouped.sum().to_dict()
                            elif metric == 'mean':
                                column_stats[metric] = grouped.mean().round(2).to_dict()
                            elif metric == 'median':
                                column_stats[metric] = grouped.median().to_dict()
                            elif metric == 'min':
                                column_stats[metric] = grouped.min().to_dict()
                            elif metric == 'max':
                                column_stats[metric] = grouped.max().to_dict()
                            elif metric == 'std':
                                column_stats[metric] = grouped.std().round(2).to_dict()
                            elif metric == 'unique':
                                column_stats[metric] = grouped.nunique().to_dict()
                        except:
                            column_stats[metric] = {}
                    
                    results[column] = column_stats
        else:
            # Overall summary
            for column in columns:
                if column in df.columns:
                    series = df[column]
                    column_stats = {}
                    
                    for metric in metrics:
                        try:
                            if metric == 'count':
                                column_stats[metric] = int(series.count())
                            elif metric == 'sum':
                                column_stats[metric] = float(series.sum())
                            elif metric == 'mean':
                                column_stats[metric] = round(float(series.mean()), 2)
                            elif metric == 'median':
                                column_stats[metric] = float(series.median())
                            elif metric == 'min':
                                column_stats[metric] = float(series.min())
                            elif metric == 'max':
                                column_stats[metric] = float(series.max())
                            elif metric == 'std':
                                column_stats[metric] = round(float(series.std()), 2)
                            elif metric == 'unique':
                                column_stats[metric] = int(series.nunique())
                        except:
                            column_stats[metric] = None
                    
                    results[column] = column_stats
        
        return results

    @staticmethod
    def _calculate_summary_simple(data, columns=None, metrics=None, group_by=None):
        """Simple summary calculation without pandas"""
        if not data:
            return {}
            
        if columns is None:
            # Auto-detect numeric columns
            columns = []
            for key, value in data[0].items():
                try:
                    float(value)
                    columns.append(key)
                except:
                    pass
        
        if metrics is None:
            metrics = ['count', 'sum', 'mean', 'min', 'max']
        
        results = {}
        
        for column in columns:
            if column not in data[0]:
                continue
                
            values = []
            for row in data:
                try:
                    val = float(row.get(column, 0))
                    values.append(val)
                except:
                    pass
            
            if not values:
                continue
                
            column_stats = {}
            
            for metric in metrics:
                if metric == 'count':
                    column_stats[metric] = len(values)
                elif metric == 'sum':
                    column_stats[metric] = sum(values)
                elif metric == 'mean':
                    column_stats[metric] = round(sum(values) / len(values), 2)
                elif metric == 'min':
                    column_stats[metric] = min(values)
                elif metric == 'max':
                    column_stats[metric] = max(values)
                elif metric == 'median':
                    sorted_vals = sorted(values)
                    n = len(sorted_vals)
                    if n % 2 == 0:
                        column_stats[metric] = (sorted_vals[n//2-1] + sorted_vals[n//2]) / 2
                    else:
                        column_stats[metric] = sorted_vals[n//2]
            
            results[column] = column_stats
        
        return results

    @staticmethod
    def format_summary_table(summary_data):
        """Format summary data for table display"""
        if not summary_data:
            return []
        
        # Check if it's grouped data
        first_column = list(summary_data.keys())[0]
        first_metric = list(summary_data[first_column].keys())[0]
        
        if isinstance(summary_data[first_column][first_metric], dict):
            # Grouped data - create table with groups as rows
            groups = set()
            for column_data in summary_data.values():
                for metric_data in column_data.values():
                    if isinstance(metric_data, dict):
                        groups.update(metric_data.keys())
            
            table_data = []
            for group in sorted(groups):
                row = {'Group': group}
                for column, metrics in summary_data.items():
                    for metric, values in metrics.items():
                        if isinstance(values, dict) and group in values:
                            row[f'{column}_{metric}'] = values[group]
                table_data.append(row)
            
            return table_data
        else:
            # Non-grouped data - create table with columns as rows
            table_data = []
            for column, metrics in summary_data.items():
                row = {'Column': column}
                row.update(metrics)
                table_data.append(row)
            
            return table_data