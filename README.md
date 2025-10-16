# 🛍️ Retail Analytics Project

## Project Overview
This project demonstrates a complete **Retail Analytics pipeline**, from raw data cleaning to actionable business insights. The goal was to transform messy retail datasets into meaningful KPIs and visualizations that support data-driven decision-making.

## Problem Statement
A retail company wants to understand its customers, products, and revenue trends to make data-driven decisions. They face challenges in:
Identifying top customers and high-value segments.
Understanding product performance and repeat purchase behavior.
Tracking customer retention and churn risk.
Monitoring revenue trends and operational KPIs.

## Technologies & Tools Used
**Python (Pandas, sqlalchemy), SQL (PostgreSQL), Power BI, Excel, VS Code**


## ETL & Data Cleaning
- **ETL Pipeline (Used Python to clean and transform → Cleaned data is loaded to PostgreSQL)**
- **Extract:** Read raw CSV files (`customers`, `products`, `orders`, `order_items`, `reviews`).
- **Transform:**
- **Generic Cleaning**: Cleaned and standardized  100K+ records of dataset. Striped strings, replaced empty/blank/nan values with `NA`, converted date columns, filled numeric nulls with 0.  
- **Table-specific Cleaning**:
  - **Orders**: Updated `order_status` based on business rules, filled missing `payment_method`.  
  - **Reviews**: Converted ratings to numeric, handled missing reviews.  
  - **Customers**: Normalized `gender` and email addresses.  
- **Automation**: Built Python ETL scripts to extract CSVs, clean data, and load into **PostgreSQL**. 
-**Load**: Insert cleaned data into PostgreSQL tables.
-**Business Value:**
Ensures all transactional data is reliable, consistent, and query-ready for analysis.

## PostgreSQL Transformations & EDA
-Tables and the structure (schema) created before loading the data from the ETL Pipeline
- `customers`, `products`, `orders`, `order_items`, `reviews`
- Relationships enforced using **foreign keys**.
- Performed additional cleaning and transformations directly in **PostgreSQL**.  
- Conducted **Exploratory Data Analysis (EDA)** to understand sales, products, and customer behaviors.  
- Generated KPIs for business performance evaluation.  


## Business Analysis & KPIs
Using the cleaned data, implemented key analytics to solve business problems:

1. **Customer RFM Analysis** – Segmented customers based on Recency, Frequency, and Monetary value.  
2. **Pareto Analysis** – Identified top 10% high-value customers contributing to revenue.  
3. **Product Performance** – Analyzed sales performance across products to identify best and worst performers.  
4. **Cohort Retention Analysis** – Measured customer retention over time to evaluate loyalty trends.

## Visualization & Reporting
- Connected **Power BI** directly to PostgreSQL via **Direct Query**.  
- Built **interactive dashboards** to display KPIs and analytics results.  
- Provided visual insights into customer segments, sales trends, product performance, and retention.
### Power BI Dashboard Snapshot
![Retail Analytics Dashboard](dashboards/dashboard_snapshot.png)


## Project Highlights
- Built an **end-to-end analytics workflow** from raw data to business insights.  
- Automated repetitive data cleaning tasks, saving hours of manual work.  
- Enabled data-driven decision-making through interactive visualizations.


## 📂 Repository Structure
