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

# PARAMETERS CELL ********************

run_id, activity, message = "manual", "manual", "test"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""CREATE TABLE IF NOT EXISTS meta.load_errors
            (logged_at TIMESTAMP, run_id STRING, activity STRING, message STRING) USING DELTA""")
spark.sql("INSERT INTO meta.load_errors SELECT current_timestamp(), ?, ?, ?",
            args=[run_id, activity, message[:4000]])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
