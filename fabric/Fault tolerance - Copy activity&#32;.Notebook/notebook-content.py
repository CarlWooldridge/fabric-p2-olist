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

import os, shutil

src = "/lakehouse/default/Files/raw/olist_order_items_dataset.csv"
dst = "/lakehouse/default/Files/test/order_items_bad.csv"
os.makedirs(os.path.dirname(dst), exist_ok=True)
shutil.copyfile(src, dst)

with open(dst, "rb") as fh:                  # make sure the last line ends before we append
    fh.seek(-1, os.SEEK_END)
    ends_clean = fh.read(1) == b"\n"
with open(dst, "a", encoding="utf-8", newline="") as fh:
    if not ends_clean:
        fh.write("\n")
    # same 7 columns as the header; price is "abc"
    fh.write('"bad_order_0001",1,"bad_product","bad_seller",2018-01-01 00:00:00,abc,10.00\n')

with open(dst, encoding="utf-8") as fh:
    lines = fh.read().splitlines()
print("data rows:", len(lines) - 1)
print("last row:", lines[-1])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS staging.order_items_typed (
# MAGIC   order_id STRING, order_item_id INT, product_id STRING, seller_id STRING,
# MAGIC   shipping_limit_date TIMESTAMP, price DECIMAL(10,2), freight_value DECIMAL(10,2)) USING DELTA;
# MAGIC DELETE FROM staging.order_items_typed;     -- empty before every run, so Append counts are clean

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM staging.order_items_typed;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
