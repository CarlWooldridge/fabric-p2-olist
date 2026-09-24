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

src = spark.table("gold.dim_payment_type")
src.write.mode("overwrite").saveAsTable("silver.lab_managed")            # managed: Fabric owns the files
src.write.mode("overwrite").format("delta").save("Files/lab/pay_type")   # data first, in Files
spark.sql("CREATE TABLE silver.lab_external USING DELTA LOCATION 'Files/lab/pay_type'")
spark.sql("DESCRIBE EXTENDED silver.lab_external").show(50, False)       # Type: EXTERNAL
spark.sql("DROP TABLE silver.lab_managed"); spark.sql("DROP TABLE silver.lab_external")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path = ("abfss://<p2-olist-workspace-id>@onelake.dfs.fabric.microsoft.com/"
        "<olistlh-lakehouse-id>/Files/lab/pay_type")          # P2 Olist / OlistLH
spark.sql(f"CREATE TABLE silver.lab_external USING DELTA LOCATION '{path}'")
spark.sql("DESCRIBE EXTENDED silver.lab_external").show(50, False)
spark.sql("SELECT COUNT(*) FROM silver.lab_external").show()                 # expect 6

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE silver.lab_external")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
