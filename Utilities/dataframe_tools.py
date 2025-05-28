#dataframe_tools.py

import pandas as pd
from google.cloud import bigquery
from google.cloud import monitoring_v3
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import random # for pivot table darts


class DataFrameInspector:
    def __init__(self, df: pd.DataFrame):
        """Initialize with a DataFrame."""
        self.df = df

    def _get_column_data_types(self):
        """Returns a dictionary of column data types."""
        return self.df.dtypes.to_dict()

    def _get_unique_counts(self):
        """Returns a dictionary of the number of unique values per column."""
        return self.df.nunique().to_dict()

    def _get_unique_value_examples(self, limit=10):
        """
        Returns a dictionary of unique value examples (up to `limit` values).
        If a column has more unique values, appends a note.
        """
        unique_examples = {}
        for col in self.df.columns:
            unique_vals = self.df[col].dropna().unique()[:limit]  # Get up to `limit` unique values
            unique_str = ", ".join(map(str, unique_vals))
            if self.df[col].nunique() > limit:
                extra_count = self.df[col].nunique() - limit
                unique_str += f" (+ {extra_count} more...)"
            unique_examples[col] = unique_str
        return unique_examples

    def generate_summary(self):
        """Generates a DataFrame with column names, data types, unique value counts, and unique value examples."""
        data_types = self._get_column_data_types()
        unique_counts = self._get_unique_counts()
        unique_examples = self._get_unique_value_examples()

        summary_df = pd.DataFrame({
            "Column": list(data_types.keys()),
            "Data Type": list(data_types.values()),
            "Unique Values": list(unique_counts.values()),
            "Unique Examples": list(unique_examples.values())
        })
        summary_df.head()
        return summary_df


class PivotTable:
    def __init__(self, df):
        """
        Initialize the PivotTable with a pandas DataFrame.
        
        Args:
            df (pd.DataFrame): The source DataFrame to build pivot tables from.
        """
        self.df = df
        self.index = []  # List of column names for rows
        self.columns = []  # List of column names for columns
        self.values = []  # List of column names to summarize
        self.aggfunc = {}  # Dictionary mapping value columns to aggregation functions
        self.filters = []  # List to store filter conditions

    def add_row(self, col_name):
        """
        Add a column to the rows (index) of the pivot table.
        
        Args:
            col_name (str): Name of the column to add as a row.
        """
        if col_name not in self.df.columns:
            raise ValueError(f"Column '{col_name}' not found in DataFrame.")
        if col_name not in self.index:
            self.index.append(col_name)

    def remove_row(self, col_name):
        """
        Remove a column from the rows (index) of the pivot table.
        
        Args:
            col_name (str): Name of the column to remove from rows.
        """
        if col_name in self.index:
            self.index.remove(col_name)
        else:
            print(f"Column '{col_name}' not in rows.")

    def add_column(self, col_name):
        """
        Add a column to the columns of the pivot table.
        
        Args:
            col_name (str): Name of the column to add as a column.
        """
        if col_name not in self.df.columns:
            raise ValueError(f"Column '{col_name}' not found in DataFrame.")
        if col_name not in self.columns:
            self.columns.append(col_name)

    def remove_column(self, col_name):
        """
        Remove a column from the columns of the pivot table.
        
        Args:
            col_name (str): Name of the column to remove from columns.
        """
        if col_name in self.columns:
            self.columns.remove(col_name)
        else:
            print(f"Column '{col_name}' not in columns.")

    def add_value(self, col_name, aggfunc='sum'):
        """
        Add a value column to summarize with a specified aggregation function.
        
        Args:
            col_name (str): Name of the column to summarize.
            aggfunc (str or list): Aggregation function(s) (e.g., 'sum', 'mean', ['sum', 'mean']).
        """
        if col_name not in self.df.columns:
            raise ValueError(f"Column '{col_name}' not found in DataFrame.")
        if col_name not in self.values:
            self.values.append(col_name)
            self.aggfunc[col_name] = aggfunc
        else:
            print(f"Column '{col_name}' already in values.")

    def remove_value(self, col_name):
        """
        Remove a value column from the pivot table.
        
        Args:
            col_name (str): Name of the column to remove from values.
        """
        if col_name in self.values:
            self.values.remove(col_name)
            del self.aggfunc[col_name]
        else:
            print(f"Column '{col_name}' not in values.")

    def set_aggfunc(self, col_name, aggfunc):
        """
        Set or update the aggregation function for a value column.
        
        Args:
            col_name (str): Name of the value column.
            aggfunc (str or list): New aggregation function(s).
        """
        if col_name in self.values:
            self.aggfunc[col_name] = aggfunc
        else:
            raise ValueError(f"Column '{col_name}' not in values.")

    def add_filter(self, col_name, operator, value):
        """
        Add a filter condition to the pivot table.
        
        Args:
            col_name (str): The column to filter on.
            operator (str): The operator to use (e.g., '==', '!=', '>', '<', '>=', '<=', 'in').
            value: The value to compare against. For 'in', provide a list.
        """
        if col_name not in self.df.columns:
            raise ValueError(f"Column '{col_name}' not found in DataFrame.")
        
        # Construct the filter condition string
        if operator == 'in':
            if not isinstance(value, list):
                raise ValueError("'in' operator requires a list of values.")
            value_str = repr(value)  # e.g., ['North', 'South']
        else:
            value_str = repr(value)  # Handles strings (e.g., 'North') and numbers (e.g., 100)
        
        # Create the condition string with backticks for column names
        condition = f"`{col_name}` {operator} {value_str}"
        self.filters.append(condition)

    def remove_filter(self, condition):
        """
        Remove a specific filter condition from the pivot table.
        
        Args:
            condition (str): The exact filter condition string to remove.
        """
        if condition in self.filters:
            self.filters.remove(condition)
        else:
            print(f"Filter '{condition}' not found.")

    def clear_filters(self):
        """
        Remove all filter conditions from the pivot table.
        """
        self.filters = []

    def _apply_filters(self):
        """
        Apply all filter conditions to the DataFrame and return the filtered result.
        
        Returns:
            pd.DataFrame: The filtered DataFrame.
        
        Raises:
            ValueError: If a filter condition is invalid.
        """
        if not self.filters:
            return self.df
        query_str = " and ".join(self.filters)
        try:
            return self.df.query(query_str)
        except Exception as e:
            raise ValueError(f"Invalid filter condition: {e}")

    def generate(self):
        """
        Generate the pivot table based on the current configuration and filters.
        
        Returns:
            pd.DataFrame: The resulting pivot table.
        
        Raises:
            ValueError: If no values are specified, columns overlap, or filters are invalid.
        """
        if set(self.index) & set(self.columns):
            raise ValueError("Columns cannot be in both rows and columns.")
        if not self.values:
            raise ValueError("No values specified for pivot table.")
        
        # Apply filters before generating the pivot table
        filtered_df = self._apply_filters()
        
        return pd.pivot_table(filtered_df, values=self.values, index=self.index, 
                              columns=self.columns, aggfunc=self.aggfunc)

    def reset(self):
        """
        Reset the pivot table configuration to its initial state, including filters.
        """
        self.index = []
        self.columns = []
        self.values = []
        self.aggfunc = {}
        self.filters = []

    def show_config(self):
        """
        Display the current configuration of the pivot table, including filters.
        """
        print("Rows:", self.index)
        print("Columns:", self.columns)
        print("Values:", self.values)
        print("Aggregation Functions:", self.aggfunc)
        print("Filters:", self.filters)


class PivotChart:
    def __init__(self, pivot_tables, chart_type='bar', titles='Pivot Chart', style='darkgrid', 
                 data_labels=False, label_format=',.0f', label_fontsize=10, 
                 grid_cols=2, palette=None, auto_fontscale=False, sync_yaxis=False, **kwargs):
        # Handle single DataFrame or list of DataFrames
        if isinstance(pivot_tables, pd.DataFrame):
            self.pivot_tables = [pivot_tables]
        elif isinstance(pivot_tables, list):
            self.pivot_tables = pivot_tables
        else:
            raise ValueError("pivot_tables must be a DataFrame or a list of DataFrames.")
        
        self.chart_type = chart_type
        self.titles = titles if titles else ['Pivot Chart'] * len(self.pivot_tables)
        if isinstance(self.titles, str):
            self.titles = [self.titles]
        if len(self.titles) != len(self.pivot_tables):
            raise ValueError("Number of titles must match the number of pivot tables.")
        
        self.style = style
        self.data_labels = data_labels
        self.label_format = label_format
        self.label_fontsize = label_fontsize
        self.grid_cols = grid_cols
        self.palette = palette
        self.auto_fontscale = auto_fontscale
        self.sync_yaxis = sync_yaxis
        self.kwargs = kwargs

    def plot(self):
        # Set Seaborn style
        sns.set_style(self.style)
        
        num_charts = len(self.pivot_tables)
        grid_rows = int(np.ceil(num_charts / self.grid_cols))
        # Set figure size to scale with both columns and rows (8 inches wide per chart)
        fig, axes = plt.subplots(grid_rows, self.grid_cols, figsize=(8 * self.grid_cols, 6 * grid_rows))
        axes = axes.flatten() if num_charts > 1 else [axes]
        print(f"Axes type before plotting: {type(axes)}")
        
        # Compute font scale as per your adjustment
        if self.auto_fontscale:
            font_scale = min(4, 2 * grid_rows)  # Double font scale per row, capped at 4
        else:
            font_scale = 1
        
        # Apply color palette if specified
        if self.palette:
            sns.set_palette(self.palette)
        
        # Plot each pivot table
        for i, (pivot_table, title, ax) in enumerate(zip(self.pivot_tables, self.titles, axes)):
            if i < num_charts:
                # Flatten multi-index columns for plotting if necessary
                if isinstance(pivot_table.columns, pd.MultiIndex):
                    pivot_table.columns = ['_'.join(map(str, col)) for col in pivot_table.columns]
                
                # Plot the data
                plot_result = pivot_table.plot(kind=self.chart_type, title=title, ax=ax, **self.kwargs)
                print(f"Plot result type for {title}: {type(plot_result)}")
                
                # Intelligently set legend labels
                if ax.get_legend():
                    # Get column names (flattened if multi-index)
                    legend_labels = pivot_table.columns.tolist()
                    # Simplify labels if they’re long or complex
                    simplified_labels = [label.replace('_', ' ').title() for label in legend_labels]
                    ax.legend(simplified_labels, fontsize=8 * font_scale, title="Categories", title_fontsize=10 * font_scale)
                
                # Add data labels if enabled
                if self.data_labels:
                    effective_label_fontsize = self.label_fontsize * font_scale
                    self._add_data_labels(ax, effective_label_fontsize)
                
                # Explicitly set font sizes for all text elements
                ax.title.set_fontsize(12 * font_scale)  # Title
                ax.xaxis.label.set_fontsize(10 * font_scale)  # X-axis label
                ax.yaxis.label.set_fontsize(10 * font_scale)  # Y-axis label
                for label in ax.get_xticklabels():  # X-axis tick labels
                    label.set_fontsize(8 * font_scale)
                for label in ax.get_yticklabels():  # Y-axis tick labels
                    label.set_fontsize(8 * font_scale)
                if ax.get_legend():  # Legend
                    for text in ax.get_legend().get_texts():
                        text.set_fontsize(8 * font_scale)
            else:
                ax.set_visible(False)
        
        # Synchronize y-axes if requested
        if self.sync_yaxis and self.chart_type in ['bar', 'line']:
            y_min = min(ax.get_ylim()[0] for ax in axes if ax.get_visible())
            y_max = max(ax.get_ylim()[1] for ax in axes if ax.get_visible())
            for ax in axes:
                if ax.get_visible():
                    ax.set_ylim(y_min, y_max)
        
        plt.tight_layout()
        plt.show()
        print(f"Returning axes type: {type(axes)}")
        return axes

    def _add_data_labels(self, ax, label_fontsize):
        if self.chart_type == 'bar':
            for container in ax.containers:
                ax.bar_label(container, fmt=f'%{self.label_format}', fontsize=label_fontsize)
        # Add support for other chart types if needed
        else:
            print(f"Data labels not yet supported for chart type '{self.chart_type}'.")


class BigQueryExtractor:
    """
    A class streamline extracting data from BigQuery.

    Args:
        project_id (str): The Google Cloud project ID.
    """
    def __init__(self, project_id):
        """Initialize the extractor with a project ID and create a BigQuery client."""
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def check_api_quota(self):
        client = monitoring_v3.MetricServiceClient()
        project_name = f"projects/{self.project_id}"
        query = client.query_time_series(
            request={
                "name": project_name,
                "query": 'fetch api | metric "serviceruntime.googleapis.com/api/request_count" | filter metric.service = "bigquery.googleapis.com"'
            }
        )
        for time_series in query:
            print(f"API Requests (last recorded): {time_series.points[-1].value.int64_value}")


    def list_datasets_and_tables(self, use_cache=True):
        if use_cache and hasattr(self, '_cached_tables'):
            print(self._cached_tables)
            return
        
        datasets = list(self.client.list_datasets())
        if not datasets:
            print(f"No datasets found in project '{self.project_id}'.")
            return

        output = [f"Datasets in project '{self.project_id}':"]
        for dataset in datasets:
            dataset_id = dataset.dataset_id
            output.append(f"\nDataset: {dataset_id}")
            tables = list(self.client.list_tables(f"{self.project_id}.{dataset_id}"))
            if tables:
                output.append("Tables:")
                for table in tables:
                    full_table = self.client.get_table(table.reference)
                    size_bytes = full_table.num_bytes
                    size_str = self._format_size(size_bytes)
                    output.append(f"  - {table.table_id} ({size_str})")
            else:
                output.append("  No tables found.")
        
        self._cached_tables = "\n".join(output)
        print(self._cached_tables)

    def _format_size(self, size_bytes):
        """
        Convert bytes to a human-readable string (e.g., KB, MB, GB).

        Args:
            size_bytes (int): Size in bytes.

        Returns:
            str: Human-readable size string.
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} TB"  # Fallback for very large sizes

    def run_query(self, query, parameters=None):
        """
        Execute a single BigQuery query and return the result as a Pandas DataFrame.

        Args:
            query (str): The SQL query to execute.
            parameters (list of tuples, optional): Query parameters as (name, type, value).

        Returns:
            pandas.DataFrame: The query result as a DataFrame.
        """
        job_config = bigquery.QueryJobConfig()
        if parameters:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(name, param_type, value)
                for name, param_type, value in parameters
            ]
        query_job = self.client.query(query, job_config=job_config)
        return query_job.to_dataframe()

    def run_queries(self, query_list):
        """
        Execute multiple BigQuery queries and return a list of Pandas DataFrames.

        Args:
            query_list (list): A list of queries (str or dict with 'query' and 'parameters').

        Returns:
            list of pandas.DataFrame: A list containing the DataFrame result for each query.
        """
        results = []
        for item in query_list:
            if isinstance(item, str):
                df = self.run_query(item)
            elif isinstance(item, dict):
                query = item['query']
                parameters = item.get('parameters', None)
                df = self.run_query(query, parameters)
            else:
                raise ValueError("Each item must be a string or a dictionary with 'query' and 'parameters'")
            results.append(df)
        return results


class BigQueryInserter:
    """
    A class to handle inserting data into BigQuery tables.

    Args:
        project_id (str): The Google Cloud project ID.
    """
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def _build_table_id(self, dataset_id, table_id):
        """Helper to construct a full table ID."""
        return f"{self.project_id}.{dataset_id}.{table_id}"

    def _get_schema_from_dataframe(self, dataframe):
        """
        Build a BigQuery schema from a Pandas DataFrame.

        Args:
            dataframe (pandas.DataFrame): The DataFrame to infer schema from.

        Returns:
            list: List of bigquery.SchemaField objects.
        """
        schema = []
        for column, dtype in dataframe.dtypes.items():
            # Map Pandas dtypes to BigQuery types
            if pd.api.types.is_integer_dtype(dtype):
                field_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                field_type = "FLOAT"
            elif pd.api.types.is_bool_dtype(dtype):
                field_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                field_type = "TIMESTAMP"
            else:
                field_type = "STRING"  # Default for object, string, etc.
            schema.append(bigquery.SchemaField(column, field_type))
        return schema

    def create_table(self, dataset_id, table_id, dataframe=None, schema=None, overwrite=False):
        """
        Create a new table in BigQuery with a schema or inferred from a DataFrame.

        Args:
            dataset_id (str): The dataset ID.
            table_id (str): The table ID (without project or dataset prefix).
            dataframe (pandas.DataFrame, optional): DataFrame to infer schema and optionally insert data.
            schema (list, optional): List of bigquery.SchemaField objects.
            overwrite (bool): If True, overwrite the table if it exists.

        Returns:
            bigquery.Table: The created table object.

        Raises:
            ValueError: If neither dataframe nor schema is provided.
            google.api_core.exceptions.Conflict: If table exists and overwrite=False.
        """
        full_table_id = self._build_table_id(dataset_id, table_id)

        if not dataframe and not schema:
            raise ValueError("Must provide either a DataFrame or a schema to create a table.")

        # Determine schema
        if dataframe and not schema:
            schema = self._get_schema_from_dataframe(dataframe)
        elif not dataframe and schema:
            pass  # Schema is already provided
        else:
            # Both provided: use explicit schema, ignore DataFrame's inferred schema
            print("Both DataFrame and schema provided; using provided schema.")

        # Create the table
        table = bigquery.Table(full_table_id, schema=schema)
        if overwrite:
            self.client.delete_table(full_table_id, not_found_ok=True)
        table = self.client.create_table(table)

        # If DataFrame provided, insert data
        if dataframe is not None:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            load_job = self.client.load_table_from_dataframe(dataframe, full_table_id, job_config=job_config)
            load_job.result()
            print(f"Created and populated table {full_table_id} with {len(dataframe)} rows")
        else:
            print(f"Created empty table {full_table_id}")

        return table

    def insert_dataframe(self, df, table_id, dataset_id=None, write_disposition='APPEND'):
        """
        Insert a Pandas DataFrame into an existing BigQuery table.

        Args:
            df (pandas.DataFrame): The DataFrame to insert.
            table_id (str): The table ID (e.g., 'dataset.table' or just 'table' if dataset_id is provided).
            dataset_id (str, optional): The dataset ID if not included in table_id.
            write_disposition (str): 'APPEND', 'TRUNCATE', or 'WRITE_EMPTY'.

        Returns:
            bigquery.LoadJob: The completed load job object.
        """
        full_table_id = self._build_table_id(dataset_id, table_id) if dataset_id else table_id
        job_config = bigquery.LoadJobConfig(
            write_disposition=f"WRITE_{write_disposition}",
            autodetect=True
        )
        load_job = self.client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
        load_job.result()
        print(f"Loaded {len(df)} rows into {full_table_id}")
        return load_job


class PivotTable_AutoConfig:
    def __init__(self, df: pd.DataFrame = None, summary: pd.DataFrame = None, num_configs: int = 1, max_unique: int = 20):
        """
        Initialize the PivotTableConfigurator with a DataFrame or summary and configuration parameters.

        Args:
            df (pd.DataFrame, optional): The input pandas DataFrame.
            summary (pd.DataFrame, optional): Summary DataFrame from DataFrameInspector.
            num_configs (int): Number of base pivot table configurations to generate (default: 1).
            max_unique (int): Maximum number of unique values for columns to be considered for rows/columns (default: 20).

        Raises:
            ValueError: If neither df nor summary is provided.
        """
        if df is None and summary is None:
            raise ValueError("Either df or summary must be provided.")
        self.df = df  # Store the DataFrame for later use in pivot table generation
        if df is not None:
            inspector = DataFrameInspector(df)
            self.summary = inspector.generate_summary()
        else:
            self.summary = summary
        self.num_configs = num_configs
        self.max_unique = max_unique
        self.potential_rows_cols = self._identify_potential_rows_columns()
        self.potential_values = self._identify_potential_values()
        self.last_configs = []  # Class-level variable to store the most recent configurations

    def _identify_potential_rows_columns(self):
        """
        Identify columns suitable for rows or columns in a pivot table.

        Returns:
            list: List of column names that are categorical or have limited unique values.
        """
        potential = self.summary[
            (self.summary['Unique Values'] <= self.max_unique) &
            ((self.summary['Data Type'] == 'object') | (self.summary['Data Type'] == 'category'))
        ]
        return potential['Column'].tolist()

    def _identify_potential_values(self):
        """
        Identify columns suitable for values in a pivot table (numeric columns).

        Returns:
            list: List of column names with numeric data types.
        """
        numeric_types = ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 
                         'float16', 'float32', 'float64']
        potential = self.summary[self.summary['Data Type'].astype(str).isin(numeric_types)]
        return potential['Column'].tolist()

    def generate_configurations(self, custom_aggfunc=None):
        """
        Generate a specified number of pivot table configurations, optionally with a custom aggregation function.

        Args:
            custom_aggfunc (str or callable, optional): Custom aggregation function to use instead of default logic.

        Returns:
            list: List of dictionaries, each containing 'index', 'columns', 'values', and 'aggfunc' for a PivotTable.

        Notes:
            - Ensures row and column selections are distinct.
            - Selects aggregation functions based on value column data types if custom_aggfunc=None.
        """
        configurations = []
        for _ in range(self.num_configs):
            if len(self.potential_rows_cols) >= 2 and len(self.potential_values) >= 1:
                row = random.choice(self.potential_rows_cols)
                col = random.choice([c for c in self.potential_rows_cols if c != row])
                val = random.choice(self.potential_values)
                if custom_aggfunc:
                    aggfunc = custom_aggfunc
                else:
                    val_dtype = self.summary[self.summary['Column'] == val]['Data Type'].values[0]
                    aggfunc = 'sum' if 'int' in str(val_dtype) else 'mean' if 'float' in str(val_dtype) else 'sum'
                configurations.append({
                    'index': [row],
                    'columns': [col],
                    'values': [val],
                    'aggfunc': {val: aggfunc}
                })
            elif len(self.potential_rows_cols) >= 1 and len(self.potential_values) >= 1:
                row = random.choice(self.potential_rows_cols)
                val = random.choice(self.potential_values)
                if custom_aggfunc:
                    aggfunc = custom_aggfunc
                else:
                    val_dtype = self.summary[self.summary['Column'] == val]['Data Type'].values[0]
                    aggfunc = 'sum' if 'int' in str(val_dtype) else 'mean' if 'float' in str(val_dtype) else 'sum'
                configurations.append({
                    'index': [row],
                    'columns': [],
                    'values': [val],
                    'aggfunc': {val: aggfunc}
                })
            else:
                print("Not enough suitable columns to generate configurations.")
                break
        
        self.last_configs = configurations  # Store as the most recent configurations
        return configurations

    def generate_triple_aggfunc_configurations(self):
        """
        Generate configurations with three variations (sum, mean, count) for each base configuration.

        Returns:
            list: List of dictionaries, each containing 'index', 'columns', 'values', and 'aggfunc' for a PivotTable,
                  with num_configs * 3 total configurations.
        """
        base_configs = []
        aggfuncs = ['sum', 'mean', 'count']
        
        # Generate base configurations (row, col, value)
        for _ in range(self.num_configs):
            if len(self.potential_rows_cols) >= 2 and len(self.potential_values) >= 1:
                row = random.choice(self.potential_rows_cols)
                col = random.choice([c for c in self.potential_rows_cols if c != row])
                val = random.choice(self.potential_values)
                base_configs.append({
                    'index': [row],
                    'columns': [col],
                    'values': [val]
                })
            elif len(self.potential_rows_cols) >= 1 and len(self.potential_values) >= 1:
                row = random.choice(self.potential_rows_cols)
                val = random.choice(self.potential_values)
                base_configs.append({
                    'index': [row],
                    'columns': [],
                    'values': [val]
                })
            else:
                print("Not enough suitable columns to generate configurations.")
                break
        
        # Generate three variations for each base config
        configurations = []
        for base_config in base_configs:
            val = base_config['values'][0]
            for aggfunc in aggfuncs:
                config = base_config.copy()
                config['aggfunc'] = {val: aggfunc}
                configurations.append(config)
        
        self.last_configs = configurations  # Store as the most recent configurations
        return configurations

    def generate_pivot_tables(self, return_configs: bool = False):
        """
        Generate pivot tables using the most recently generated configurations.

        Args:
            return_configs (bool): If True, returns a list of tuples (config, pivot_table); 
                                  if False, returns a list of pivot tables (default: False).

        Returns:
            list: Depending on return_configs, either a list of pivot tables (pd.DataFrame) or 
                  a list of tuples (dict, pd.DataFrame) with configurations and pivot tables.

        Raises:
            ValueError: If no DataFrame or no configurations are available.
        """
        if self.df is None:
            raise ValueError("A DataFrame must be provided during initialization to generate pivot tables.")
        if not self.last_configs:
            raise ValueError("No configurations available. Run generate_configurations or generate_triple_aggfunc_configurations first.")
        
        results = []
        for config in self.last_configs:
            try:
                # Instantiate PivotTable with the stored DataFrame
                pt = PivotTable(self.df)
                
                # Apply configuration
                pt.index = config['index']
                pt.columns = config['columns']
                pt.values = config['values']
                pt.aggfunc = config['aggfunc']
                
                # Generate the pivot table
                pivot_table = pt.generate()
                
                # Store result based on return_configs flag
                if return_configs:
                    results.append((config, pivot_table))
                else:
                    results.append(pivot_table)
            except Exception as e:
                print(f"Failed to generate pivot table for config {config}: {e}")
                continue
                
        return results

    def generate_titles(self):
        """
        Generate intelligent titles for the most recent pivot table configurations.

        Returns:
            list: List of descriptive titles based on the index, columns, values, and aggfunc of each configuration.
        """
        if not self.last_configs:
            raise ValueError("No configurations available. Run generate_configurations or generate_triple_aggfunc_configurations first.")

        titles = []
        for config in self.last_configs:
            index_name = config['index'][0] if config['index'] else "All"
            columns_name = f" by {config['columns'][0]}" if config['columns'] else ""
            values_name = config['values'][0]
            aggfunc = list(config['aggfunc'].values())[0]  # Get the aggfunc for the value column

            # Construct the title
            title = f"{values_name} {aggfunc.capitalize()} {index_name}{columns_name}"
            titles.append(title)

        return titles





