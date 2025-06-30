# 🧾 Quote-to-Cash Workflow Audit

This project showcases a mock analysis of a typical **Quote-to-Cash (QTC)** process using synthesized data. The notebook (`audit.ipynb`) walks through how opportunities in Salesforce flow into subscriptions in Zuora, and how revenue is ultimately recognized in RevPro.

It demonstrates how data from multiple systems can be joined, audited, and visualized to uncover timing inefficiencies or revenue delays.

## 📁 Files

- **`audit.ipynb`**  
  An interactive Jupyter notebook that loads, inspects, and analyzes the quote-to-cash data. Includes:
  - Exploratory summaries
  - Conversion rate analysis
  - Timing metrics (e.g., days to close, days to revenue)
  - Visualizations of the workflow lags

- **`qtc_mock_datasource.xlsx`**  
  A mock dataset representing the three key systems involved:
  - `salesforce_opportunities`: CRM stage tracking
  - `zuora_subscriptions`: Subscription lifecycle & billing
  - `revpro_revenue`: Revenue recognition tracking

## 📊 Key Metrics Analyzed

- **Days to Close** – From opportunity creation to deal closure
- **Days to Start** – From close date to subscription activation
- **Days to Revenue** – From subscription start to first recognized revenue

## 🛠 Tools & Technologies

- Python
- Pandas
- Seaborn/Matplotlib
- Jupyter Notebook

---

This project is part of my [Data Analytics Portfolio](https://github.com/Joey-VW/DataAnalyticsPortfolio) and is intended to demonstrate my ability to synthesize multi-system data into actionable business insights.

