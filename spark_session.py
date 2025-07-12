from pyspark.sql import SparkSession
import sys
import io
from pyspark.sql.functions import col, sum as _sum, avg, count as _count, round

# Fix Unicode printing in terminal
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read Multiple CSV Files") \
    .getOrCreate()

# Read customers.csv 
customers_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(r"C:\Users\uie76632\Desktop\customers.csv") 

# Clean column names (remove BOM and NBSP, if any)
cleaned_customer_cols = [col.strip().replace('\ufeff', '').replace('\xa0', '') for col in customers_df.columns]
customers_df = customers_df.toDF(*cleaned_customer_cols)

print("Cleaned Customer Columns:", customers_df.columns)

#Read salestxns.csv
sales_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(r"C:\Users\uie76632\Desktop\salestxns.csv")

# Clean sales column names (safe option)
cleaned_sales_cols = [col.strip().replace('\ufeff', '').replace('\xa0', '') for col in sales_df.columns]
sales_df = sales_df.toDF(*cleaned_sales_cols)

print("Cleaned Sales Columns:", sales_df.columns)


# Query 1: Count unique customers
unique_customers = customers_df.select("Customer ID").distinct().count()
print(f"Total Unique Customers: {unique_customers}")

# Query 2: total sales amount for each state
# Compute total per transaction (Price * Quantity)
sales_df = sales_df.withColumn("Total", col("Price") * col("Quantity"))

# Join both DataFrames on "Customer Id"
joined_df = sales_df.join(customers_df, on="Customer Id", how="inner")

# Group by "State" and compute total sales
sales_by_state = joined_df.groupBy("State").agg(
    _sum("Total").alias("TotalSales")
)

sales_by_state.orderBy("TotalSales", ascending=False).show(truncate=False)

# Query 3: Find top 10 products sold
top_products = sales_df.groupBy("Product Name").agg(
    _sum("Quantity").alias("TotalQuantitySold")
)

top_products.orderBy("TotalQuantitySold", ascending=False).show(10, truncate=False)

# Query 4: Average Transaction Value
sales_df = sales_df.withColumn("Total", col("Price") * col("Quantity"))

avg_transaction = sales_df.select(avg("Total").alias("AvgTransactionValue"))
avg_transaction.show()

# Query 5: Top 5 Customers by Total Spent
top_customers = sales_df.groupBy("Customer Id").agg(
    _sum("Total").alias("TotalSpent")
).orderBy("TotalSpent", ascending=False).limit(5)

top_customers.show(truncate=False)

# Query 6: Product Purchases by Specific Customer
customer_256_products = sales_df.filter(col("Customer Id") == 256) \
    .select("Product Name", "Quantity", (col("Price") * col("Quantity")).alias("AmountSpent"))

customer_256_products.show(truncate=False)

# # Query 7: Monthly Sales Trend
# sales_df = sales_df.withColumn("TxnMonth", date_format(to_date("Txn Date", "yyyy-MM-dd"), "yyyy-MM"))

# monthly_sales = sales_df.groupBy("TxnMonth").agg(_sum("Total").alias("MonthlySales"))
# monthly_sales.orderBy("TxnMonth").show(truncate=False)

# Query 8: Category-wise Sales
category_sales = sales_df.groupBy("Category Name").agg(
    _sum("Total").alias("CategorySales")
).orderBy("CategorySales", ascending=False)

category_sales.show(1, truncate=False)

# query 9: Show top category sales
joined_df = sales_df.join(customers_df, on="Customer Id", how="inner")

sales_df = sales_df.withColumn("Total", col("Price") * col("Quantity"))

# Group by State
state_sales = joined_df.groupBy("State").agg(
    _sum("Total").alias("TotalSales")
).orderBy("TotalSales", ascending=False)

state_sales.show(truncate=False)

# Query 10: Customer Report with Total Spent and Transaction Count
sales_df = sales_df.withColumn("Total", col("Price") * col("Quantity"))

# Aggregate per customer
customer_report = sales_df.groupBy("Customer Id").agg(
    _sum("Total").alias("TotalPurchases"),
    _count("*").alias("TotalTransactions"),
    round(_sum("Total") / _count("*"), 2).alias("AvgTransactionValue")
)

# Join with customers_df to get names
detailed_report = customer_report.join(customers_df, on="Customer Id", how="left") \
    .select(
        "Customer Id", 
        "Name", 
        "TotalPurchases", 
        "TotalTransactions", 
        "AvgTransactionValue"
    ) \
    .orderBy("TotalPurchases", ascending=False)

# Show final report
detailed_report.filter(col("Name").isNotNull()).show()
