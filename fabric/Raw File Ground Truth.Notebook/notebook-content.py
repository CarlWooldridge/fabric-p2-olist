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

import csv, glob, os
for path in sorted(glob.glob("/lakehouse/default/Files/raw/*.csv")):
    with open(path, encoding="utf-8", newline="") as fh:
        print(f"{os.path.basename(path):45} {sum(1 for _ in csv.reader(fh)) - 1:>9,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import csv, glob, os

rows = []
for path in sorted(glob.glob("/lakehouse/default/Files/raw/*.csv")):
    name = os.path.basename(path)[:-4]
    with open(path, encoding="utf-8", newline="") as fh:
        raw = sum(1 for _ in csv.reader(fh)) - 1          # minus the header
    bronze = spark.table(f"bronze.{name}").count()
    rows.append((name, raw, bronze, bronze - raw, "OK" if raw == bronze else "MISMATCH"))

compare = spark.createDataFrame(rows, "table STRING, raw_records INT, bronze_rows INT, diff INT, status STRING")
display(compare.orderBy("status", "table"))                # MISMATCH sorts above OK
print("all match:", all(r[3] == 0 for r in rows))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
