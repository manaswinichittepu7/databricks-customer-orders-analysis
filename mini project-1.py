# Databricks notebook source
# MAGIC %md
# MAGIC #read and process data
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
spark= SparkSession.builder.appName("customerdataprocessing").getOrCreate()
spark

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/manaswini.chittepu4@gmail.com/customers.csv")

# COMMAND ----------

df.show(5)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import *

df = df.withColumn(
    "registration_date",
    to_date(col("registration_date"), "yyyy-MM-dd")
).withColumn(
    "is_active",
    col("is_active").cast("boolean")
)


# COMMAND ----------

df.show(5)
df.printSchema()

# COMMAND ----------

df = df.fillna({
    "city": "Unknown",
    "state": "Unknown",
    "country": "Unknown"
})


# COMMAND ----------

df = df.withColumn(
    "registration_year",
    year(col("registration_date"))
)


# COMMAND ----------

unique_cities = df.select(countDistinct("city")).collect()
unique_cities


# COMMAND ----------

df.groupBy("city").count().orderBy(col("count").desc()).show()


# COMMAND ----------

#pivot
df.groupBy("state").pivot("is_active").count().show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank, dense_rank, row_number


# COMMAND ----------

#windows
window_spec = Window.partitionBy("state") \
                    .orderBy(col("registration_date").desc())

# Apply ranking functions
df = df.withColumn("rank", rank().over(window_spec)) \
       .withColumn("dense_rank", dense_rank().over(window_spec)) \
       .withColumn("row_number", row_number().over(window_spec))

df.show(5)

# COMMAND ----------

output_path = "dbfs:/FileStore/tables/processed_customers"
df.write.mode("overwrite").parquet(output_path)


# COMMAND ----------

orders=df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/manaswini.chittepu4@gmail.com/orders.csv")
orders.show(5)

# COMMAND ----------

customers_orders_df = df.join(
    orders,
    df.customer_id == orders.customer_id,
    "inner"
).drop(orders.customer_id)



# COMMAND ----------

customers_orders_df.show(5)

# COMMAND ----------

#total orders per customer
customers_orders_count = customers_orders_df.groupBy('customer_id').count()
customers_orders_count.show()


# COMMAND ----------

#customers with the highest number of orders
customers_orders_count = customers_orders_df \
    .groupBy('customer_id') \
    .count() \
    .orderBy(col('count').desc())

customers_orders_count.show()


# COMMAND ----------

from pyspark.sql.functions import sum, col

customer_total_spend = customers_orders_df \
    .groupBy("customer_id") \
    .agg(sum("total_amount").alias("total_spend")) \
    .orderBy(col("total_spend").desc())

customer_total_spend.show()


# COMMAND ----------

#order by status
# order By status
orders_status_count = customers_orders_df.groupBy('status').count()
orders_status_count.show()


# COMMAND ----------

customers_status_count = customers_orders_df \
    .groupBy("customer_id", "status") \
    .count()

customers_status_count.show()


# COMMAND ----------

#orders by month
orders_per_month = customers_orders_df \
    .groupBy(month("order_date").alias("order_month")) \
    .count() \
    .orderBy("order_month")

orders_per_month.show()


# COMMAND ----------

#Orders per CUSTOMER per MONTH
from pyspark.sql.functions import month

customer_orders_per_month = customers_orders_df \
    .groupBy(
        "customer_id",
        month("order_date").alias("order_month")
    ) \
    .count() \
    .orderBy("customer_id", "order_month")

customer_orders_per_month.show()



# COMMAND ----------

from pyspark.sql.functions import sum

customer_total_spend = customers_orders_df \
    .groupBy("customer_id") \
    .agg(sum("total_amount").alias("total_spent"))


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

window_spec = Window.orderBy(col("total_spent").desc())

ranked_customers = customer_total_spend \
    .withColumn("dense_rank", dense_rank().over(window_spec))

ranked_customers.show(10)


# COMMAND ----------

#Customers with HIGH order frequency but LOW total spend
from pyspark.sql.functions import col

customer_spend_vs_orders = customers_orders_count \
    .join(customer_total_spend, "customer_id", "inner") \
    .orderBy(col("count").desc(), col("total_spent").asc())

customer_spend_vs_orders.show(5)


# COMMAND ----------

output_path = "dbfs:/FileStore/tables/final_customer_orders"

customers_orders_df.write \
    .mode("overwrite") \
    .parquet(output_path)
