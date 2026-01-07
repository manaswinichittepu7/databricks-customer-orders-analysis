# Databricks Customer Orders Analysis

This project performs a comprehensive analysis on customer orders using **Apache Spark (PySpark)** in **Databricks**.

## 🔍 Analysis Overview

The notebook computes:
- Total orders per customer
- Total spend per customer
- Order status distribution
- Customer order frequency
- Monthly order trends
- Combined analytics (top spenders / frequent low spenders)

## 📁 Repository Structure

databricks-customer-orders-analysis/
│
├── customer_orders_analysis.py # Main PySpark analysis (exported from Databricks)
├── README.md # Project documentation
└── .gitignore # Git ignore file


---

## 🚀 How to Run (Local Setup)

> Note: This project was developed in Databricks.  
> You can still review and run the logic locally with PySpark.

### 1️⃣ Install dependencies
```bash
pip install pyspark
2️⃣ Run the analysis
python customer_orders_analysis.py

💾 Data Output

Processed results are written in Parquet format using Spark:

Stored in Databricks File System (DBFS)

Format optimized for analytics and scalability

Example output path:

dbfs:/FileStore/tables/final_customer_orders

📈 Sample Business Insights

Identified customers with high order frequency but low average spend

Ranked customers by total spend using window functions

Analyzed order lifecycle through status distribution

Derived monthly ordering trends for time-based analysis

🧩 Skills Demonstrated

Spark joins and aggregations

Handling ambiguous columns after joins

Window functions (rank, dense_rank)

Time-based grouping (year, month)

Writing and reading Parquet files

Git version control & project structuring

🎯 Why This Project Matters

This project reflects real-world data engineering and analytics workflows, including:

Multi-table joins

Large-scale aggregation logic

Performance-aware Spark usage

Clean, production-style code organization

👤 Author

Manaswini Chittepu
Graduate Student – Data Science
Focused on Data Engineering & Analytics


⭐ Future Enhancements

Add sample input datasets

Convert analysis to Spark SQL

Add visualizations (Power BI / Tableau)

Parameterize pipelines for reusability
