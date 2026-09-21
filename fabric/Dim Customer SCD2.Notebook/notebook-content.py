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

spark.sql("DROP TABLE IF EXISTS gold.dim_customer")   # re-runnable from here: the increment isn't
spark.sql("""
CREATE TABLE gold.dim_customer (
    customer_sk         INT,        -- one per VERSION: what the fact points at
    customer_key        INT,        -- one per PERSON: same across all their versions
    customer_unique_id  STRING,
    customer_name       STRING,
    customer_zip_prefix STRING,
    customer_city       STRING,
    customer_state      STRING,
    customer_lat        DOUBLE,
    customer_lng        DOUBLE,
    valid_from          TIMESTAMP,
    valid_to            TIMESTAMP,
    is_current          BOOLEAN
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, Window as W
from delta.tables import DeltaTable

WM  = "2017-12-31 23:59:59"                            # Day 11's watermark
END = F.lit("9999-12-31").cast("timestamp")

# Every order = one sighting of where that person lived at that moment
obs = (spark.table("silver.customers")
    .join(spark.table("silver.orders").select("customer_id", "order_purchase_timestamp"), "customer_id")
    .select("customer_unique_id", "customer_name",
            F.col("zip_prefix").alias("customer_zip_prefix"),
            F.col("city").alias("customer_city"),
            F.col("state").alias("customer_state"),
            F.col("order_purchase_timestamp").alias("seen_at"))
    .withColumn("addr", F.concat_ws("|", "customer_zip_prefix", "customer_city", "customer_state")))

# Gold copies lat/lng onto the dimension from the one geo table (Day 10 Steps 2 and 6)
zg = spark.table("silver.geo_zip_prefix").select(
        F.col("zip_prefix").alias("customer_zip_prefix"),
        F.col("lat").alias("customer_lat"), F.col("lng").alias("customer_lng"))

COLS = ["customer_sk", "customer_key", "customer_unique_id", "customer_name", "customer_zip_prefix",
        "customer_city", "customer_state", "customer_lat", "customer_lng",
        "valid_from", "valid_to", "is_current"]

def shape(df):
    return (df.join(zg, "customer_zip_prefix", "left")          # one lookup: the one geo table
              .withColumn("customer_city", F.initcap("customer_city"))   # display: 'Sao Paulo'
              .select(*COLS))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

latest = W.partitionBy("customer_unique_id").orderBy(F.col("seen_at").desc())
initial = (obs.where(F.col("seen_at") <= WM)
    .withColumn("rn", F.row_number().over(latest)).where("rn = 1")
    .withColumn("customer_key", F.row_number().over(W.orderBy("customer_unique_id")).cast("int"))
    .withColumn("customer_sk", F.col("customer_key"))              # version 1: the numbers coincide
    .withColumn("valid_from", F.lit("1900-01-01").cast("timestamp"))
    .withColumn("valid_to", END)
    .withColumn("is_current", F.lit(True)))
shape(initial).write.mode("append").saveAsTable("gold.dim_customer")

d = spark.table("gold.dim_customer")
print("rows:", d.count(), " current:", d.where("is_current").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim = spark.table("gold.dim_customer")
cur = dim.where("is_current").select(
    "customer_unique_id", "customer_key",
    F.concat_ws("|", "customer_zip_prefix", F.lower("customer_city"), "customer_state").alias("cur_addr"))

by_person = W.partitionBy("customer_unique_id").orderBy("seen_at")

versions = (obs.where(F.col("seen_at") > WM)
    # 1. Keep only orders where the address changed from the person's previous order
    .withColumn("prev", F.lag("addr").over(by_person))
    .where(F.col("prev").isNull() | (F.col("addr") != F.col("prev")))
    .withColumn("n", F.row_number().over(by_person))
    # 2. A person's first island that just repeats their current address is not a change
    .join(cur, "customer_unique_id", "left")
    .where(~((F.col("n") == 1) & F.col("addr").eqNullSafe(F.col("cur_addr"))))
    # 3. Each version ends where the next one starts; the last one is current
    .withColumn("valid_from", F.col("seen_at"))
    .withColumn("valid_to", F.coalesce(F.lead("seen_at").over(by_person), END))
    .withColumn("is_current", F.col("valid_to") == END))

# Keys: continue numbering after the current maximum
max_sk, max_key = dim.agg(F.max("customer_sk"), F.max("customer_key")).first()
new_people = (versions.where(F.col("customer_key").isNull()).select("customer_unique_id").distinct()
    .withColumn("new_key", (F.row_number().over(W.orderBy("customer_unique_id")) + max_key).cast("int")))
versions = (versions.join(new_people, "customer_unique_id", "left")
    .withColumn("customer_key", F.coalesce("customer_key", "new_key"))
    .withColumn("customer_sk", (F.row_number().over(W.orderBy("customer_unique_id", "valid_from"))
                                + max_sk).cast("int"))
    .localCheckpoint())   # FREEZE the result now — the merge below changes the rows `cur` reads

# EXPIRE: close each mover's current row at the moment their first new version starts
movers = (versions.where(F.col("cur_addr").isNotNull())
          .groupBy("customer_unique_id").agg(F.min("valid_from").alias("new_from")))
print("existing customers who moved:", movers.count())

(DeltaTable.forName(spark, "gold.dim_customer").alias("d")
    .merge(movers.alias("s"), "d.customer_unique_id = s.customer_unique_id AND d.is_current")
    .whenMatchedUpdate(set={"valid_to": "s.new_from", "is_current": "false"})
    .execute())

# INSERT: every new version — movers' new addresses and brand-new customers alike
shape(versions).write.mode("append").saveAsTable("gold.dim_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                           AS all_rows,
# MAGIC   SUM(CASE WHEN is_current THEN 1 ELSE 0 END)        AS current_rows,
# MAGIC   COUNT(DISTINCT customer_key)                       AS people,
# MAGIC   COUNT(DISTINCT customer_sk)                        AS versions
# MAGIC FROM gold.dim_customer;
# MAGIC 
# MAGIC -- Must return NO rows: every person has exactly one current version
# MAGIC SELECT customer_unique_id FROM gold.dim_customer
# MAGIC GROUP BY customer_unique_id HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1;
# MAGIC 
# MAGIC -- Must return NO rows: one person, one durable key
# MAGIC SELECT customer_unique_id FROM gold.dim_customer
# MAGIC GROUP BY customer_unique_id HAVING COUNT(DISTINCT customer_key) > 1;
# MAGIC 
# MAGIC -- Must return NO rows: each version ends exactly where the next begins — no gaps, no overlaps
# MAGIC SELECT * FROM (
# MAGIC   SELECT customer_unique_id, valid_to,
# MAGIC          LEAD(valid_from) OVER (PARTITION BY customer_unique_id ORDER BY valid_from) AS next_from
# MAGIC   FROM gold.dim_customer) t
# MAGIC WHERE next_from IS NOT NULL AND valid_to <> next_from;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT customer_name, customer_zip_prefix, customer_city, customer_state, valid_from, valid_to, is_current
# MAGIC FROM gold.dim_customer
# MAGIC WHERE customer_key IN (SELECT customer_key FROM gold.dim_customer GROUP BY customer_key HAVING COUNT(*) > 1)
# MAGIC ORDER BY customer_key, valid_from
# MAGIC LIMIT 1000;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
