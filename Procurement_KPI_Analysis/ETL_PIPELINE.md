# ETL Pipeline – Procurement KPI Analysis

This document outlines the ETL (Extract, Transform, Load) pipeline developed for the **Procurement KPI Analysis Project**. The pipeline automates the ingestion of raw procurement data, enriches it with temporal insights, and loads the processed dataset into **Google BigQuery** for downstream analysis.

<br><br>

## 🔁 Overview

**Purpose**: Transform a raw Kaggle dataset into a clean, analytics-ready table for Procurement KPI exploration.

**Pipeline Steps**:
1. **Extract** data from Kaggle.
2. **Transform** it using `pandas`, adding calculated date-based columns.
3. **Load** the final DataFrame into BigQuery under a predefined schema.

<br><br>

## 📁 Dataset Source

- **Name**: [shahriarkabir/procurement-kpi-analysis-dataset](https://www.kaggle.com/datasets/shahriarkabir/procurement-kpi-analysis-dataset)
- **Tool**: [`kagglehub`](https://pypi.org/project/kagglehub/) used for downloading directly into the project environment.

<br><br>

## 🧰 Technologies Used

| Tool / Library     | Purpose                             |
|--------------------|-------------------------------------|
| `pandas`           | Data manipulation and transformation |
| `pandas_gbq`       | Uploading DataFrames to BigQuery     |
| `kagglehub`        | Programmatic dataset access from Kaggle |
| `dataframe_tools`  | Custom DataFrame inspector for EDA   |
| `Google BigQuery`  | Cloud-based data warehouse storage   |

<br><br>

## 🧪 Transformations Applied

- **Date Parsing**: Converted `Order_Date` and `Delivery_Date` to datetime objects.
- **Time Period Derivations**:
  - `Order_Week_Start`: Week start (Saturday) of the order date
  - `Order_Month`: Month start of the order date
  - `Delivery_Week_Start`: Week start (Saturday) of the delivery date
  - `Delivery_Month`: Month start of the delivery date
- **Data Summary**: Used a custom `DataFrameInspector` class to inspect missing values, datatypes, and value distributions during preprocessing.

<br><br>

## 🧱 BigQuery Schema

| Column               | Type     | Description                                |
|----------------------|----------|--------------------------------------------|
| PO_ID                | STRING   | Unique identifier for the purchase order   |
| Supplier             | STRING   | Supplier name                              |
| Order_Date           | DATE     | Date when the order was placed             |
| Delivery_Date        | DATE     | Date when the order was delivered          |
| Item_Category        | STRING   | Product category                           |
| Order_Status         | STRING   | Current order status                       |
| Quantity             | INTEGER  | Quantity ordered                           |
| Unit_Price           | FLOAT    | Unit price listed                          |
| Negotiated_Price     | FLOAT    | Final negotiated price                     |
| Defective_Units      | FLOAT    | Count of units marked defective            |
| Compliance           | STRING   | Compliance status                          |
| Order_Week_Start     | DATE     | Start of the week of the order             |
| Order_Month          | DATE     | Start of the month of the order            |
| Delivery_Week_Start  | DATE     | Start of the week of delivery              |
| Delivery_Month       | DATE     | Start of the month of delivery             |

<br><br>

## 🚀 Output Table

- **Destination**: `Procurement_KPI_Sample.data`
- **Project**: `vivid-alchemy-388523`
- **Mode**: `replace` — the dataset is overwritten on each ETL run to maintain consistency during experimentation.

<br><br>

## 🧩 Full Script

The full ETL implementation is located in the project as [`etl_pipeline.py`](etl_pipeline.py). The `main()` function orchestrates all steps, providing logging throughout the process.

```bash
python etl_pipeline.py
```

<br><br>

## 📌 Related Files

- QUERIES_AND_INSIGHTS.md: Key questions and SQL queries used to derive business insights.
- VISUALIZATIONS.md: Visual representations of KPI trends and supplier performance.

<br><br>

---
