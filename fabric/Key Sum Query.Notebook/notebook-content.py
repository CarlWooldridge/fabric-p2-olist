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

# MAGIC %%sql
# MAGIC SELECT 'products' t, COUNT(*) n, SUM(product_sk) key_sum FROM gold.dim_product
# MAGIC UNION ALL 
# MAGIC SELECT 'sellers', COUNT(*), SUM(seller_sk) FROM gold.dim_seller
# MAGIC UNION ALL 
# MAGIC SELECT 'orders',  COUNT(*), SUM(order_key)  FROM gold.key_order
# MAGIC UNION ALL 
# MAGIC SELECT 'lines',   COUNT(*), SUM(order_key)  FROM gold.fact_order_items;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
