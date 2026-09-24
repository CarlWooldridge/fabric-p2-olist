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

# Join on column *names* so each key appears once; drop bronze's lineage columns,
# which every table carries and would otherwise collide.
t = lambda n: spark.table(f"bronze.{n}").drop("_source_file", "_ingested_at")
flat = (t("olist_order_items_dataset")
        .join(t("olist_orders_dataset"),    "order_id")
        .join(t("olist_customers_dataset"), "customer_id")
        .join(t("olist_products_dataset"),  "product_id")
        .join(t("olist_sellers_dataset"),   "seller_id")
        .join(t("olist_order_reviews_dataset")
                .select("order_id", "review_score", "review_comment_message"),
              "order_id", "left"))
flat.write.mode("overwrite").saveAsTable("silver.bad_flat_order_items")
print(flat.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
