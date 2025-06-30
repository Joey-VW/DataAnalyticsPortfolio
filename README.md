# 📊 Data Analytics Portfolio

Welcome to my data analytics portfolio! I'm a data professional with 7+ years of experience in data analytics, process automation, and team leadership. This repository highlights my capabilities in **SQL (BigQuery)**, **Python**, and **data visualization**, with a focus on practical, end-to-end analytics solutions.

<br><br>

## 🚀 Featured Project: [`Procurement KPI Analysis`](./Procurement_KPI_Analysis)

A full-stack analytics project that demonstrates how raw procurement data is transformed, analyzed, and visualized to provide actionable business insights.

### 🔄 1. ETL Pipeline Automation  
- **Files**: [`etl_pipeline.py`](./Procurement_KPI_Analysis/etl_pipeline.py), [`ETL_PIPELINE.md`](./Procurement_KPI_Analysis/ETL_PIPELINE.md)  
- **Tech**: Python, Google BigQuery  
- **Description**: 
  - Automated an ETL process to ingest, clean, and enrich a procurement dataset.
  - Leveraged KaggleHub, pandas, and pandas-gbq for seamless integration with BigQuery.
- **Outcome**: Produced a clean, time-aware dataset suitable for advanced analysis and dashboarding.

### 📊 2. KPI Exploration and Analysis  
- **Files**: [`QUERIES_AND_INSIGHTS.md`](./Procurement_KPI_Analysis/QUERIES_AND_INSIGHTS.md)  
- **Tech**: SQL (BigQuery)  
- **Description**: 
  - Created a suite of analytical queries to explore supplier performance and category trends.
  - Focused on metrics such as average costs, negotiation savings, delivery speed & reliability, defect rate, and compliance.

### 📈 3. KPI Visualization  
- **Files**: [`VISUALIZATIONS.md`](./Procurement_KPI_Analysis/VISUALIZATIONS.md)  
- **Tech**: Looker Studio  
- **Description**: 
  - Built an interactive dashboard to visualize procurement KPIs using the transformed data.
  - Emphasized top-level readability.

<br><br>

## 🕸️ Featured Project: [`ScrapeX`](./ScrapeX) – Post Scraping Utility for X

An automation tool that programmatically scrapes tweet content and engagement data from X (formerly Twitter), with a CLI wrapper for flexible execution.

### 🧠 1. Scraping Engine & Class Design  
- **Files**: [`scrapeX.py`](./ScrapeX/scrapeX.py), [`scrapeX/README.md`](./ScrapeX/README.md)  
- **Tech**: Python, Selenium, Threading  
- **Description**:  
  - Scrapes tweets using a class-based Selenium engine with configurable time limits and engagement scraping.
  - Handles authentication, scrolling, duplicate avoidance, and partial-save recovery.

### 💻 2. Command-Line Interface  
- **File**: [`scrapeX_CLI.py`](./scrapeX/scrapeX_CLI.py)  
- **Tech**: Python, argparse  
- **Description**:  
  - Launch scraping jobs via terminal with full argument control.
  - Integrates seamlessly with automation scripts and environment variables.

### 📦 3. Output and Use Cases  
- **Format**: JSON  
- **Use Cases**:  
  - Social sentiment analysis  
  - Topic clustering for NLP pipelines  
  - Quote tweet engagement tracking
 
<br><br>

## ➡️ Featured Project: [`Quote-to-Cash Workflow Audit`](./Quote-to-Cash%20Workflow%20Audit)
An exploratory audit of the Quote-to-Cash process using mock data to simulate Salesforce → Zuora → RevPro integrations.

### 🔍 1. Dataset Inspection & Join Logic
- **Files**: [`audit.ipynb`](./Quote-to-Cash%20Workflow%20Audit/audit.ipynb), [`qtc_mock_datasource.xlsx`](./Quote-to-Cash%20Workflow%20Audit/qtc_mock_datasource.xlsx)
- **Tech**: Python, pandas, Jupyter Notebook
- **Description**:
  - Synthesizes opportunity, subscription, and revenue recognition data from three systems.
  - Applies summary statistics and workflow mapping to identify timing gaps in the QTC lifecycle.

### 📊 2. Lag Time Analysis
- **Metrics Tracked**:
  - `days_to_close`: Opportunity creation → deal won
  - `days_to_start`: Deal won → subscription activation
  - `days_to_revenue`: Subscription start → revenue recognition
- **Visualization**: Histograms and descriptive stats to explore bottlenecks or delays.

### 📈 3. Outcome
- Illustrates how real businesses can audit their revenue workflows for friction and efficiency.
- Demonstrates mock analysis workflows that are portable to real datasets and CRM systems.

<br><br>

## 🧰 Utilities

### [`Utilities/dataframe_tools.py`](./Utilities/dataframe_tools.py)
A lightweight helper module containing a custom `DataFrameInspector` class for quickly profiling DataFrames. Used during the ETL preprocessing stage.

<br><br>


## 🛠️ Skills Overview

- **Languages**: Python, SQL (BigQuery), JavaScript (Apps Script)
- **Tools**: Looker Studio, Tableau, Google Workspace, Excel
- **Core Techniques**: ETL pipelines, data wrangling, dashboard design, KPI reporting, automation

<br><br>


## 📬 Contact

[📧 joey.wisto@gmail.com](mailto:joey.wisto@gmail.com) | [🔗 LinkedIn](https://www.linkedin.com/in/joey-wisto)

<br><br>

Feel free to explore the files in each folder to learn more about my approach to analytics—and don’t hesitate to reach out!
