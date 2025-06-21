# etl_pipeline.py

import os
import shutil
from datetime import datetime
import pandas as pd
import pandas_gbq
import kagglehub
from dataframe_tools import DataFrameInspector

# Configuration
DATASET = "shahriarkabir/procurement-kpi-analysis-dataset"
PROJECT_ID = "vivid-alchemy-388523"
DEST_TABLE = "Procurement_KPI_Sample.data"

SCHEMA = [
    {'name': 'PO_ID', 'type': 'STRING'},
    {'name': 'Supplier', 'type': 'STRING'},
    {'name': 'Order_Date', 'type': 'DATE'},
    {'name': 'Delivery_Date', 'type': 'DATE'},
    {'name': 'Item_Category', 'type': 'STRING'},
    {'name': 'Order_Status', 'type': 'STRING'},
    {'name': 'Quantity', 'type': 'INTEGER'},
    {'name': 'Unit_Price', 'type': 'FLOAT'},
    {'name': 'Negotiated_Price', 'type': 'FLOAT'},
    {'name': 'Defective_Units', 'type': 'FLOAT'},
    {'name': 'Compliance', 'type': 'STRING'},
    {'name': 'Order_Week_Start', 'type': 'DATE'},
    {'name': 'Order_Month', 'type': 'DATE'},
    {'name': 'Delivery_Week_Start', 'type': 'DATE'},
    {'name': 'Delivery_Month', 'type': 'DATE'},
]

def download_kaggle_dataset(dataset_name: str) -> str:
    print("⬇️ Downloading dataset from Kaggle...")
    dataset_path = kagglehub.dataset_download(dataset_name)
    print(f"✅ Dataset downloaded to: {dataset_path}")
    return dataset_path

def extract_csv(dataset_path: str) -> str:
    print("📁 Searching for CSV file in the dataset folder...")
    for file in os.listdir(dataset_path):
        if file.endswith('.csv'):
            source = os.path.join(dataset_path, file)
            dest = os.path.join(os.getcwd(), file)
            shutil.move(source, dest)
            print(f"✅ Found and moved CSV file to: {dest}")
            return dest
    raise FileNotFoundError("❌ No CSV file found in the dataset folder")

def preprocess_dataframe(csv_file: str) -> pd.DataFrame:
    print("📊 Loading CSV into pandas DataFrame...")
    df = pd.read_csv(csv_file)
    print(f"✅ DataFrame loaded with shape: {df.shape}")
    
    print("🧪 Generating summary of the DataFrame...")
    summary = DataFrameInspector(df).generate_summary()
    print(summary)

    print("🗓️ Converting date columns...")
    df['Order_Date'] = pd.to_datetime(df['Order_Date'])
    df['Delivery_Date'] = pd.to_datetime(df['Delivery_Date'])

    print("🧮 Adding time period columns...")
    df['Order_Week_Start'] = df['Order_Date'].dt.to_period('W-SAT').dt.start_time
    df['Order_Month'] = df['Order_Date'].dt.to_period('M').dt.start_time
    df['Delivery_Week_Start'] = df['Delivery_Date'].dt.to_period('W-SAT').dt.start_time
    df['Delivery_Month'] = df['Delivery_Date'].dt.to_period('M').dt.start_time

    print("📌 Sample of date columns:")
    print(df[['Order_Date', 'Order_Week_Start', 'Order_Month', 'Delivery_Date', 'Delivery_Week_Start', 'Delivery_Month']].head())
    
    return df

def upload_to_bigquery(df: pd.DataFrame, table: str, project: str, schema: list):
    print("🚀 Uploading DataFrame to BigQuery...")
    pandas_gbq.to_gbq(
        df,
        table,
        project_id=project,
        if_exists='replace',
        table_schema=schema,
        progress_bar=True
    )
    print(f"✅ Data uploaded to BigQuery table `{table}`")

def main():
    print("🔄 Starting ETL pipeline for Procurement KPI Analysis...")
    dataset_path = download_kaggle_dataset(DATASET)
    csv_file = extract_csv(dataset_path)
    df = preprocess_dataframe(csv_file)
    upload_to_bigquery(df, DEST_TABLE, PROJECT_ID, SCHEMA)
    print("🎉 Done")

if __name__ == "__main__":
    main()


