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

WITH_UNIQUE = True   # counts values occurring exactly once: one extra pass per column

rows = []
for t in [r.tableName for r in spark.sql("SHOW TABLES IN bronze").collect()]:
    df = spark.table(f"bronze.{t}").drop("_source_file", "_ingested_at").cache()
    n  = df.count()

    aggs = []
    for c in df.columns:                      # backticks: some source columns are misspelled, none are safe to assume
        col = F.col(f"`{c}`")
        aggs += [
            F.count(col).alias(f"{c}#nonnull"),                                    # count() skips nulls
            F.coalesce(F.sum((F.trim(col) == F.lit("")).cast("int")), F.lit(0)).alias(f"{c}#empty"),
            F.countDistinct(col).alias(f"{c}#distinct"),                           # also skips nulls
            F.min(col).alias(f"{c}#min"),
            F.max(col).alias(f"{c}#max"),
        ]
    st = df.agg(*aggs).first().asDict()

    for c in df.columns:
        nulls, distinct = n - st[f"{c}#nonnull"], st[f"{c}#distinct"]
        unique = None
        if WITH_UNIQUE:
            unique = (df.groupBy(F.col(f"`{c}`").alias("v")).count()
                        .where("count = 1 AND v IS NOT NULL").count())
        rows.append((t, c, n, nulls, round(100.0 * nulls / n, 2), int(st[f"{c}#empty"]),
                     distinct, unique, distinct == n and nulls == 0,
                     str(st[f"{c}#min"])[:40], str(st[f"{c}#max"])[:40]))
    df.unpersist()

profile = spark.createDataFrame(rows, """table STRING, column STRING, rows INT, nulls INT, null_pct DOUBLE,
    empty_strings INT, distinct_vals INT, unique_vals INT, is_key BOOLEAN, min_val STRING, max_val STRING""")
profile.write.mode("overwrite").saveAsTable("meta.bronze_profile")   # the note, versioned and re-runnable
display(profile.orderBy("table", "column"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
