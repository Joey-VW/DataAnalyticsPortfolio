# 🌎 Supplier Summary
```sql
SELECT
  Supplier,
  AVG(Unit_Price) AS `Avg Unit Cost`,
  AVG(Negotiated_Price) AS `Avg Negotiated Price`,
  SAFE_DIVIDE(SUM((Unit_Price - Negotiated_Price) * Quantity), SUM(Unit_Price * Quantity)) AS `Negotiation Savings %`,
  AVG(DATE_DIFF(DATE(Delivery_Date), DATE(Order_Date), DAY)) AS `Average Lead Days`,
  SAFE_DIVIDE(
    COUNTIF(DATE_DIFF(DATE(Delivery_Date), DATE(Order_Date), DAY) <= 10), -- assuming a 10-day delivery target
    COUNT(Delivery_Date)
  ) AS `On-Time Delivery Rate`,
  SAFE_DIVIDE(SUM(Defective_Units), SUM(Quantity)) AS `Defect Rate`,
  SAFE_DIVIDE(COUNTIF(Compliance = 'No'), COUNT(*)) AS `Noncompliance Rate`
FROM `projectID.Procurement_KPI_Sample.data`
GROUP BY Supplier
```
^ This query summarizes supplier performance by calculating average costs, negotiation savings, delivery speed & reliability, defect rate, and compliance issues.
<br>
##### _Output:_
![image](https://github.com/user-attachments/assets/00a83ab9-d970-4c50-8943-c804111f570f)

#### What we can infer so far:
- **Epsilon_Group** is the most well-rounded and reliable supplier.
- **Alpha_Inc** is strong in cost and quality, but delivery needs improvement.
- **Gamma_Co** is best for fast and timely delivery but weaker in quality and price.
- Consider avoiding **Delta_Logistics** unless cost is a higher priority than quality or compliance.

<br><br><br>
# 🔎 A Deeper Look
#### These indicators can be segmented by category for deeper insights.

The following BigQuery script creates five tables, each representing one of the above measurements broken down by category (results shown below):
```sql
DECLARE suppliers ARRAY<STRING>;
DECLARE metrics ARRAY<STRUCT<expr STRING, alias STRING>>;
DECLARE sql STRING;
DECLARE table_name STRING;

-- Get unique supplier list
SET suppliers = (
  SELECT ARRAY_AGG(DISTINCT Supplier ORDER BY Supplier)
  FROM `projectID.Procurement_KPI_Sample.data`
);

-- construct KPIs: Avg Unit Price, Avg Negotiated Price, Negotiation Savings %, Defect Rate, and Noncompliance Rate.
SET metrics = [
  STRUCT('AVG(CASE WHEN Supplier = "{SUPPLIER}" THEN Unit_Price END)', 'Avg Unit Price'),
  STRUCT('AVG(CASE WHEN Supplier = "{SUPPLIER}" THEN Negotiated_Price END)', 'Avg Negotiated Price'),
  STRUCT(
    'SAFE_DIVIDE(SUM(CASE WHEN Supplier = "{SUPPLIER}" THEN (Unit_Price - Negotiated_Price) * Quantity ELSE 0 END), SUM(CASE WHEN Supplier = "{SUPPLIER}" THEN Unit_Price * Quantity ELSE 0 END))',
    'Negotiation Savings %'
  ),
  STRUCT(
    'SAFE_DIVIDE(SUM(CASE WHEN Supplier = "{SUPPLIER}" THEN Defective_Units ELSE 0 END), SUM(CASE WHEN Supplier = "{SUPPLIER}" THEN Quantity ELSE 0 END))',
    'Defect Rate'
  ),
  STRUCT(
    'SAFE_DIVIDE(COUNTIF(Supplier = "{SUPPLIER}" AND Compliance = "No"), COUNTIF(Supplier = "{SUPPLIER}"))',
    'Noncompliance Rate'
  )
];

-- loop through each metric, set a table name, and construct the query.
FOR metric IN (SELECT * FROM UNNEST(metrics)) DO

  -- Sanitize alias, remove spaces, %, etc.
  SET table_name = 'Pivot_' || REPLACE(REPLACE(REPLACE(metric.alias, ' ', '_'), '%', 'pct'), '-', '_');

  -- 🏗️
  SET sql = (
    SELECT FORMAT('''
      CREATE OR REPLACE TABLE `projectID.Procurement_KPI_Sample.%s` AS
      SELECT
        Item_Category,
        %s
      FROM `projectID.Procurement_KPI_Sample.data`
      GROUP BY Item_Category
      ORDER BY Item_Category
    ''',
      table_name,
      STRING_AGG(
        FORMAT('(%s) AS `%s`', REPLACE(metric.expr, '{SUPPLIER}', s), s),
        ', '
      )
    )
    FROM UNNEST(suppliers) AS s
  );

  
  EXECUTE IMMEDIATE sql;

END FOR;
```

### 📌 Avg Unit Price
![image](https://github.com/user-attachments/assets/25bbc30d-02cc-439e-94c6-a57b3a440f21)

### 📌 Avg Negotiated Price
![image](https://github.com/user-attachments/assets/7f3dd1bc-4853-445e-b376-0ec8f5683669)

### 📌 Negotiation Savings %
![image](https://github.com/user-attachments/assets/d3f1067f-b011-42eb-b91f-ddd07652560f)

### 📌 Defect Rate
![image](https://github.com/user-attachments/assets/20deaf18-0f63-4c75-addb-3fcd9e4ee848)

### 📌 Noncompliance Rate
![image](https://github.com/user-attachments/assets/d8d717f5-4b04-4604-a675-301c1f06949a)

<br>

### ℹ️ Category Insights:

| Category         | Best Overall Supplier         | Worst Supplier             | Key Notes                                                  |
|------------------|-------------------------------|----------------------------|------------------------------------------------------------|
| Electronics      | Alpha_Inc / Epsilon_Group     | Delta_Logistics            | Great cost:performance from Alpha & Epsilon                |
| MRO              | Epsilon_Group                 | Delta_Logistics / Beta     | Epsilon balances negotiation, low defects/compliance       |
| Office Supplies  | Epsilon_Group                 | Delta_Logistics            | Epsilon has 0% noncompliance + low defects                 |
| Packaging        | Alpha_Inc                     | Delta_Logistics / Beta     | Alpha combines strong negotiation and quality              |
| Raw Materials    | Epsilon_Group                 | Delta_Logistics            | Epsilon wins despite not being cheapest                    |


<br><br>
See Visualizations.md for a showcase of the dashboard created from this data.
<br><br>


