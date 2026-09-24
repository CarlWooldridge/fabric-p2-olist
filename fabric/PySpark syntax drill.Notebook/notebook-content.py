# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "<olistlh-lakehouse-id>",
# META       "default_lakehouse_name": "OlistLH",
# META       "default_lakehouse_workspace_id": "<p2-olist-workspace-id>",
# META       "known_lakehouses": [
# META         {
# META           "id": "<olistlh-lakehouse-id>"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F          # F.col, F.avg, F.sum, F.when … — the usual alias
from pyspark.sql.window import Window           # for row_number() in cell 5

orders   = spark.table("gold.fact_orders")      # a DataFrame: lazy — nothing is read yet
lines    = spark.table("gold.fact_order_items")
products = spark.table("gold.dim_product")
customers = spark.table("gold.dim_customer")
orders.printSchema()                            # an action-free look at the columns and types


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(orders.groupBy("order_status")                 # one group per status
       .count()                                 # adds a column named "count"
       .orderBy(F.desc("count"))                # biggest first
       .show())                                 # show() is the ACTION — this is when Spark runs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reviewed = orders.filter(F.col("review_score").isNotNull())    # .where() is the same method
print(reviewed.count())                                        # count() on a DataFrame = an action
reviewed.agg(F.avg("review_score").alias("avg_review")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(orders.filter(F.col("is_late")).count())   # a boolean column is already a condition

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(lines.join(products, on="product_sk", how="inner")
      .groupBy("category_display")
      .agg(F.sum("price").alias("revenue"))
      .orderBy(F.desc("revenue"))
      .limit(3)
      .withColumn("revenue", F.format_number("revenue", 2))
      .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w = Window.partitionBy("customer_unique_id").orderBy(F.desc("purchase_date"))  # restart per person
latest = (orders.join(customers, on="customer_sk")             # SCD2 version → the person
                .withColumn("rn", F.row_number().over(w))      # 1 = newest order per person
                .filter(F.col("rn") == 1))
print(latest.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

banded = orders.withColumn("delivery_band",
            F.when(F.col("days_to_deliver").isNull(), "not delivered")
             .when(F.col("days_to_deliver") <= 7,  "fast")
             .when(F.col("days_to_deliver") <= 20, "normal")
             .otherwise("slow"))                       # first match wins, like CASE WHEN
banded = banded.withColumn("review_int", F.col("review_score").cast("int"))
banded.createOrReplaceTempView("banded")               # hand it to SQL
print(customers.dropDuplicates(["customer_unique_id"]).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT delivery_band, COUNT(*) AS orders FROM banded GROUP BY delivery_band ORDER BY orders DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
