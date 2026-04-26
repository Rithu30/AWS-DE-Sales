# AWS Serverless Sales Analytics Pipeline #

An end-to-end cloud data engineering project that ingests raw CSV sales data, processes and transforms it using AWS serverless services, queries it with Amazon Athena, and visualizes it through interactive Power BI dashboards.  

Architecture diagram available in **`SalesArchitecture.png`**

----------------------
**Project Overview**

This project addresses a practical issue: **large-scale analysis of raw sales data stored in CSV files is challenging**. This pipeline uses serverless AWS services to automate the entire process, from uploading a CSV file to viewing live dashboards, instead of opening Excel by hand.
The pipeline joins two datasets, cleans the data, enriches it with derived columns, and outputs a clean **`sales_analytics`** table ready for reporting.

-----------------------------------
**AWS Services Used**

| Service | Role in Pipeline |
|---|---|
| **Amazon S3** | Data lake storage: raw, processed, and final buckets |
| **AWS Lambda** | Serverless trigger function activated on every S3 file upload |
| **AWS Glue Crawler** | Automatically scans S3 data and detects schema |
| **AWS Glue Data Catalog** | Central metadata repository for all table schemas |
| **AWS Glue ETL** | PySpark-based transformation job which joins, cleans, enriches |
| **Amazon Athena** | Serverless SQL engine to query data directly from S3 |
| **Amazon IAM** | Permissions and roles for all services |
| **Power BI** | Business intelligence dashboards and visualizations |

-----------------------------------------
**Technologies Used**

| Technology | Purpose |
|---|---|
| AWS S3 | Data lake storage |
| AWS Lambda | Event-driven trigger |
| AWS Glue | ETL & schema catalog |
| Amazon Athena | Serverless SQL queries |
| Apache Parquet | Columnar output format |
| PySpark | Data transformation engine |
| Power BI Desktop | Dashboard & visualization |
| Simba ODBC Driver | Athena -> Power BI connector |

-----------------------------------------

**Power BI Dashboard Pages** 

| Page | Visuals Configured | Key Insight |
|---|---|---|
| **Executive Overview** | KPI cards, line chart, donut chart, bar chart | Total revenue, orders, top customers |
| **Product Performance** | Bar chart, matrix, treemap, slicers | Best-selling products and categories |
| **Customer Intelligence** | Scatter chart, tier bar, ranked table | High-value customers and geographies |
| **Time Intelligence** | Waterfall, area chart, column+line combo | Revenue trends and seasonality |

