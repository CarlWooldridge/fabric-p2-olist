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

from pyspark.sql.functions import input_file_name, current_timestamp

files = [
    "olist_orders_dataset", "olist_order_items_dataset", "olist_customers_dataset",
    "olist_sellers_dataset", "olist_products_dataset", "olist_order_payments_dataset",
    "olist_order_reviews_dataset", "olist_geolocation_dataset",
    "product_category_name_translation",
]

for f in files:
    df = (spark.read
          .option("header", "true")
          .option("multiLine", "true")      # review comments contain embedded newlines
          .option("escape", '"')
          .csv(f"Files/raw/{f}.csv"))       # deliberately NO inferSchema — bronze is all-string
    (df.withColumn("_source_file", input_file_name())
       .withColumn("_ingested_at", current_timestamp())
       .write.mode("overwrite").saveAsTable(f"bronze.{f}"))
    print(f, spark.table(f"bronze.{f}").count())   # count the table that landed, not the CSV read

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
