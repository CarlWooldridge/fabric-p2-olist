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

spark.sql("""
CREATE TABLE IF NOT EXISTS meta.load_watermark (
    table_name STRING,
    last_loaded_ts TIMESTAMP
) USING DELTA
""")
spark.sql("DELETE FROM meta.load_watermark WHERE table_name = 'orders'")   # re-runnable: never two rows
spark.sql("""
INSERT INTO meta.load_watermark
VALUES ('orders', TIMESTAMP '2017-12-31 23:59:59')
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, max as _max
from delta.tables import DeltaTable

wm = (spark.table("meta.load_watermark")
        .filter("table_name = 'orders'").collect()[0]["last_loaded_ts"])
(spark.table("silver.orders").filter(col("order_purchase_timestamp") <= wm)
    .write.mode("overwrite").saveAsTable("gold.fact_orders_incremental"))
print("initial rows:", spark.table("gold.fact_orders_incremental").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
