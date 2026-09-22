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

from pyspark.sql import functions as F

spark.sql("DROP TABLE IF EXISTS gold.lab_fragmented")
src = spark.table("gold.fact_order_items")
for i in range(30):
    (src.where(F.col("order_key") % 30 == i)
        .repartition(8)                                   # ask for 8 files per load
        .write.mode("append").saveAsTable("gold.lab_fragmented"))

d = spark.sql("DESCRIBE DETAIL gold.lab_fragmented").first()
print("rows:", spark.table("gold.lab_fragmented").count(), " files:", d["numFiles"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

def measure(label):
    t = spark.table("gold.lab_fragmented")
    d = spark.sql("DESCRIBE DETAIL gold.lab_fragmented").first()
    busiest = t.groupBy("seller_sk").count().orderBy(F.desc("count")).first()["seller_sk"]
    spread = (t.where(F.col("seller_sk") == busiest)
               .select(F.input_file_name()).distinct().count())
    print(f"{label:22} files {d['numFiles']:>4} | busiest seller (sk {busiest}) spread over {spread:>3} files")

measure("before")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 256 * 1024)   # this session only

spark.sql("OPTIMIZE gold.lab_fragmented")
measure("after OPTIMIZE")

spark.sql("OPTIMIZE gold.lab_fragmented ZORDER BY (seller_sk) VORDER")
measure("after ZORDER")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY gold.lab_fragmented LIMIT 3;   -- read the version, operation, operationParameters columns

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) AS rows_at_v0  FROM gold.lab_fragmented VERSION AS OF 0;
# MAGIC SELECT COUNT(*) AS rows_at_v14 FROM gold.lab_fragmented VERSION AS OF 14;
# MAGIC SELECT COUNT(*) AS rows_now    FROM gold.lab_fragmented;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC VACUUM gold.lab_fragmented RETAIN 168 HOURS;
# MAGIC SELECT COUNT(*) FROM gold.lab_fragmented VERSION AS OF 0;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM gold.lab_fragmented RETAIN 0 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")   # back on at once
measure("after VACUUM")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM gold.lab_fragmented VERSION AS OF 0;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
