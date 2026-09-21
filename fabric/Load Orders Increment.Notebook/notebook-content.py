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

from pyspark.sql.functions import col, max as _max
from delta.tables import DeltaTable

wm = (spark.table("meta.load_watermark")
        .filter("table_name = 'orders'").collect()[0]["last_loaded_ts"])
print("watermark:", wm)

new_rows = spark.table("silver.orders").filter(col("order_purchase_timestamp") > wm)
print("rows to load:", new_rows.count())

tgt = DeltaTable.forName(spark, "gold.fact_orders_incremental")
(tgt.alias("t")
    .merge(new_rows.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

new_wm = new_rows.agg(_max("order_purchase_timestamp")).collect()[0][0]
if new_wm is not None:                    # nothing new = leave the watermark alone
    spark.sql(f"""
    UPDATE meta.load_watermark
    SET last_loaded_ts = TIMESTAMP '{new_wm}'
    WHERE table_name = 'orders'
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
