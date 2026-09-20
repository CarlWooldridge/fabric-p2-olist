-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "<olistlh-lakehouse-id>",
-- META       "default_lakehouse_name": "OlistLH",
-- META       "default_lakehouse_workspace_id": "<p2-olist-workspace-id>",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "<olistlh-lakehouse-id>"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC 
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
-- MAGIC 
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS meta")   # pipeline plumbing, not medallion data

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }
