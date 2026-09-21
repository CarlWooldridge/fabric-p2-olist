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

from pyspark.sql import functions as F, Window as W

def numbered(df, natural_key, sk, offset=0):
    return df.withColumn(sk, (F.row_number().over(W.orderBy(natural_key)) + offset).cast("int"))

def keyed(src, natural_key, sk, table):
    """Surrogate keys that never move. Keys already in gold.<table> are kept; only IDs gold has
    never seen get new keys, numbered after the current max. First run: plain 1..n."""
    if not spark.catalog.tableExists(f"gold.{table}"):
        return numbered(src, natural_key, sk)
    issued = spark.table(f"gold.{table}").where(F.col(sk) != -1).select(natural_key, sk)
    top = issued.agg(F.max(sk)).first()[0] or 0
    new = numbered(src.join(issued, natural_key, "left_anti"), natural_key, sk, offset=top)
    # localCheckpoint: materialize now, because the result overwrites the table it was read from
    return src.join(issued, natural_key).unionByName(new).localCheckpoint()

def with_unknown(df, sk, **labels):
    """Append the -1 'Unknown' row that unmatched fact rows point at."""
    unknown = df.limit(1).select(*[
        (F.lit(-1) if c == sk else F.lit(labels.get(c))).cast(t).alias(c) for c, t in df.dtypes])
    return df.unionByName(unknown)

def save(df, name):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"gold.{name}")
    print(f"{name:18} {spark.table(f'gold.{name}').count():>8,}")

d = spark.sql("SELECT explode(sequence(DATE'2016-01-01', DATE'2018-12-31', INTERVAL 1 DAY)) AS date_key")
save(d.select("date_key",
        F.year("date_key").alias("year"),
        F.quarter("date_key").alias("quarter"),
        F.month("date_key").alias("month"),
        (F.year("date_key") * 100 + F.month("date_key")).alias("month_key"),   # 201801 — sorts right
        F.date_format("date_key", "MMM yyyy").alias("month_name"),
        F.dayofweek("date_key").alias("day_of_week"),                           # 1 = Sunday
        F.date_format("date_key", "EEE").alias("day_name"),
        F.dayofweek("date_key").isin(1, 7).alias("is_weekend")), "dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save(with_unknown(
        keyed(spark.table("silver.products"), "product_id", "product_sk", "dim_product")
        .select("product_sk", "product_id", "product_name", "category", "category_display",
                "product_weight_g", "product_photos_qty"),
        "product_sk", product_name="Unknown", category_display="Unknown"), "dim_product")

zg = spark.table("silver.geo_zip_prefix").select("zip_prefix", "lat", "lng")
save(with_unknown(
        keyed(spark.table("silver.sellers"), "seller_id", "seller_sk", "dim_seller")
        .join(zg, "zip_prefix", "left")
        .select("seller_sk", "seller_id", "seller_name",
                F.col("zip_prefix").alias("seller_zip_prefix"),
                F.initcap("city").alias("seller_city"),
                F.col("state").alias("seller_state"),
                F.col("lat").alias("seller_lat"),
                F.col("lng").alias("seller_lng")),
        "seller_sk", seller_name="Unknown", seller_state="??"), "dim_seller")

# dim_customer came from Day 12 — add its Unknown row (delete first, so this cell re-runs safely)
spark.sql("DELETE FROM gold.dim_customer WHERE customer_sk = -1")
spark.sql("""INSERT INTO gold.dim_customer
             VALUES (-1, -1, NULL, 'Unknown', NULL, NULL, '??', NULL, NULL, NULL, NULL, true)""")
print("dim_customer", spark.table("gold.dim_customer").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders = spark.table("silver.orders")
save(keyed(orders.select("order_id"), "order_id", "order_key", "key_order"), "key_order")   # hex id -> INT, kept
cust   = spark.table("silver.customers").select("customer_id", "customer_unique_id")
ver    = spark.table("gold.dim_customer").where("customer_sk <> -1").select(
            F.col("customer_unique_id").alias("v_uid"), "customer_sk", "valid_from", "valid_to",
            "customer_state", "customer_lat", "customer_lng")

o = (orders
    .join(spark.table("gold.key_order"), "order_id")
    .join(cust, "customer_id")
    # POINT-IN-TIME: the version that was current when this order was placed
    .join(ver, (F.col("customer_unique_id") == F.col("v_uid"))
             & (F.col("order_purchase_timestamp") >= F.col("valid_from"))
             & (F.col("order_purchase_timestamp") <  F.col("valid_to")), "left")
    .withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
    .withColumn("customer_sk", F.coalesce("customer_sk", F.lit(-1))))

items = spark.table("silver.order_items")
agg_items = items.groupBy("order_id").agg(F.count("*").alias("item_count"),
                                          F.sum("price").alias("items_value"),
                                          F.sum("freight_value").alias("freight_total"))
pay = spark.table("silver.order_payments").groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_total"),
        F.max("payment_installments").alias("payment_installments"))
rev = spark.table("silver.order_reviews").groupBy("order_id").agg(       # 551 orders have 2+ reviews
        F.round(F.avg("review_score"), 2).alias("review_score"))

dt = F.to_date
save(o.join(agg_items, "order_id", "left").join(pay, "order_id", "left").join(rev, "order_id", "left")
    .select("order_key", "customer_sk", "order_status", "purchase_date",
            F.hour("order_purchase_timestamp").alias("purchase_hour"),
            dt("order_approved_at").alias("approved_date"),
            dt("order_delivered_carrier_date").alias("delivered_carrier_date"),
            dt("order_delivered_customer_date").alias("delivered_customer_date"),
            dt("order_estimated_delivery_date").alias("estimated_delivery_date"),
            F.datediff("order_approved_at", "order_purchase_timestamp").alias("days_to_approve"),
            F.datediff("order_delivered_customer_date", "order_purchase_timestamp").alias("days_to_deliver"),
            F.datediff("order_delivered_customer_date", "order_estimated_delivery_date").alias("days_vs_estimate"),
            (dt("order_delivered_customer_date") > dt("order_estimated_delivery_date")).alias("is_late"),
            F.coalesce("item_count", F.lit(0)).alias("item_count"),              # 775 orders: 0 lines
            "items_value", "freight_total", "payment_total", "payment_installments", "review_score"),
     "fact_orders")

def km(lat1, lng1, lat2, lng2):
    """Great-circle distance (haversine)."""
    r = F.radians
    a = (F.pow(F.sin((r(lat2) - r(lat1)) / 2), 2)
         + F.cos(r(lat1)) * F.cos(r(lat2)) * F.pow(F.sin((r(lng2) - r(lng1)) / 2), 2))
    return F.round(6371 * 2 * F.asin(F.sqrt(a)), 1)

prod = spark.table("gold.dim_product").select("product_id", "product_sk")
sell = spark.table("gold.dim_seller").select("seller_id", "seller_sk", "seller_state", "seller_lat", "seller_lng")
save(items
    # Only order_key from the header — customer, date and status reach the lines through it.
    # Customer location is borrowed here just to compute distance, then dropped.
    .join(o.select("order_id", "order_key", "customer_state", "customer_lat", "customer_lng"), "order_id")
    .join(prod, "product_id", "left").join(sell, "seller_id", "left")
    .select("order_key", "order_item_id",
            F.coalesce("product_sk", F.lit(-1)).alias("product_sk"),
            F.coalesce("seller_sk", F.lit(-1)).alias("seller_sk"),
            F.to_date("shipping_limit_date").alias("shipping_limit_date"),   # per-line seller deadline
            "price", "freight_value",
            km("customer_lat", "customer_lng", "seller_lat", "seller_lng").alias("distance_km"),
            (F.col("customer_state") == F.col("seller_state")).alias("is_same_state")),
     "fact_order_items")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t, sk in [("dim_customer", "customer_sk"), ("dim_seller", "seller_sk"), ("dim_product", "product_sk")]:
    df = spark.table(f"gold.{t}")
    print(f"{t:18} rows {df.count():>7,}  distinct keys {df.select(sk).distinct().count():>7,}")

for f, fks in [("fact_orders", ["customer_sk"]), ("fact_order_items", ["product_sk", "seller_sk"])]:
    df = spark.table(f"gold.{f}")
    print(f, {k: df.where(F.col(k) == -1).count() for k in fks})

fo, fi = spark.table("gold.fact_orders"), spark.table("gold.fact_order_items")
print("header keys unique:", fo.count() == fo.select("order_key").distinct().count(),
      " lines with no header:", fi.join(fo, "order_key", "left_anti").count(),
      " no distance:", fi.where("distance_km IS NULL").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
