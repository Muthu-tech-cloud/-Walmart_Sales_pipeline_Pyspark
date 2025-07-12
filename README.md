**Project Title
Walmart Sales & Customer Insights with PySpark**

📊 **Objective**
This project uses Apache Spark (PySpark) to analyze Walmart sales data across various dimensions such as state, product category, and customers. It demonstrates how to read CSV files, clean column names, join datasets, and extract insights through aggregation and filtering.

📁 **Input Files**
This project expects two CSV files located on your system:

customers.csv

Columns: Customer Id, Name, City, State, Zip Code

salestxns.csv

Columns: sales Txn Id, Category Id, Category Name, Product Id, Product Name, Price, Quantity, Customer Id

⚙️ **Technologies Used**
Python 3.x

Apache Spark 3.x

PySpark (Spark SQL)

🔧 Setup & Requirements
Install Java JDK (Java 8+)

Install Apache Spark

Install PySpark:

bash
Copy
Edit
pip install pyspark
Place the customers.csv and salestxns.csv files in the appropriate paths (update paths in the script if necessary).

🚀 How to Run
Run the script using spark-submit or directly inside a Python environment (e.g., VS Code or Jupyter Notebook):

bash
Copy
Edit
spark-submit spark_session.py
✅ Features & Queries Covered
1. Total Unique Customers
Counts the number of distinct customers in the dataset.

2. Total Sales by State
Joins sales_df with customers_df and aggregates total sales grouped by State.

3. Top 10 Most Purchased Products
Shows products with the highest total Quantity sold.

4. Average Transaction Value
Computes the average value of each transaction (Price * Quantity).

5. Top 5 Customers by Total Spending
Identifies the top spenders by aggregating total purchases.

6. Product Purchases by a Specific Customer
Filters and shows products purchased by customer with ID 256.

7. Category with Highest Sales
Identifies the product category with the highest revenue.

8. State-wise Sales Comparison
Compares total sales by state.

9. Detailed Customer Purchase Report
Combines customer name with:

Total purchases

Number of transactions

Average transaction value

📌 Notes
Column names are cleaned using .replace('\ufeff', '') and .replace('\xa0', '') to remove invisible characters like BOM and NBSP.

Customer Id is used to join both datasets.

The dataset does not contain a date field, so monthly trends are skipped or can be simulated if required.
