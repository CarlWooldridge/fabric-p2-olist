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

TS, MONEY = "timestamp", "decimal(10,2)"
SPEC = {
    "olist_orders_dataset": ("orders", {
        "order_purchase_timestamp": TS, "order_approved_at": TS, "order_delivered_carrier_date": TS,
        "order_delivered_customer_date": TS, "order_estimated_delivery_date": TS}),
    "olist_order_items_dataset": ("order_items", {
        "order_item_id": "int", "shipping_limit_date": TS, "price": MONEY, "freight_value": MONEY}),
    "olist_order_payments_dataset": ("order_payments", {
        "payment_sequential": "int", "payment_installments": "int", "payment_value": MONEY}),
    "olist_order_reviews_dataset": ("order_reviews", {
        "review_score": "int", "review_creation_date": TS, "review_answer_timestamp": TS}),
}

for bronze_name, (silver_name, types) in SPEC.items():
    df = spark.table(f"bronze.{bronze_name}").drop("_source_file", "_ingested_at")
    for c, t in types.items():
        df = (df.withColumnRenamed(c, f"{c}__raw")
                .withColumn(c, F.expr(f"try_cast(`{c}__raw` AS {t})")))
    # A value present in bronze but null after the cast = a failed conversion
    lost = df.agg(*[F.sum((F.col(f"{c}__raw").isNotNull() & F.col(c).isNull()).cast("int")).alias(c)
                    for c in types]).first().asDict()
    df = df.drop(*[f"{c}__raw" for c in types])
    if silver_name == "orders":
        df = df.withColumn("order_status", F.lower(F.trim("order_status")))
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"silver.{silver_name}")
    print(f"{silver_name:15} {df.count():>8,}  failed casts: {lost}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("silver.orders").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

ACCENTED, PLAIN = "áàâãäéèêëíìîïóòôõöúùûüçñ", "aaaaaeeeeiiiiooooouuuucn"

def norm_city(c):
    """'São Paulo ' -> 'sao paulo'. One definition, used for every city column."""
    return F.translate(F.lower(F.trim(c)), ACCENTED, PLAIN)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

geo = spark.table("bronze.olist_geolocation_dataset")
pts = (geo
    .select(F.col("geolocation_zip_code_prefix").alias("zip_prefix"),
            F.expr("try_cast(geolocation_lat AS double)").alias("lat"),
            F.expr("try_cast(geolocation_lng AS double)").alias("lng"),
            norm_city("geolocation_city").alias("city"),
            F.upper(F.trim("geolocation_state")).alias("state")))

# 1. Filter FIRST — Brazil spans roughly lat -34 → +6, lng -74 → -34
br = pts.where("lat BETWEEN -34 AND 6 AND lng BETWEEN -74 AND -34")
print("raw points:", pts.count(), " outside Brazil:", pts.count() - br.count())

# 2. Then one row per prefix: median point, most common city/state
zip_geo = (br.groupBy("zip_prefix").agg(
        F.expr("percentile(lat, 0.5)").alias("lat"),
        F.expr("percentile(lng, 0.5)").alias("lng"),
        F.expr("mode(city)").alias("city"),
        F.expr("mode(state)").alias("state"),
        F.countDistinct("city").alias("city_count"),       # >1 = the prefix spans several towns
        F.count("*").alias("observation_count"),
        (F.max("lat") - F.min("lat")).alias("lat_spread"))  # >1° ≈ 110 km of stray points
    .withColumn("geo_source", F.lit("observed")))           # Step 6 adds the prefixes geo never saw
zip_geo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.geo_zip_prefix")

print("prefixes:", zip_geo.count(),
      " spread > 1°:", zip_geo.where("lat_spread > 1").count(),
      " multi-town:", zip_geo.where("city_count > 1").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prod  = spark.table("bronze.olist_products_dataset")
trans = spark.table("bronze.product_category_name_translation")

joined = prod.join(trans, on="product_category_name", how="left")
print("products:", prod.count(), " after join:", joined.count(),
      " untranslated:", joined.where(F.col("product_category_name_english").isNull()).count())

# Two categories missing from the translation file, and four typos inside it
FIX = {"pc_gamer": "pc_gamer",
       "portateis_cozinha_e_preparadores_de_alimentos": "portable_kitchen_food_preparers",
       "costruction_tools_garden": "construction_tools_garden",
       "costruction_tools_tools": "construction_tools_tools",
       "fashio_female_clothing": "fashion_female_clothing",
       "home_confort": "home_comfort"}
fix = F.create_map(*[F.lit(x) for kv in FIX.items() for x in kv])

raw_cat = F.coalesce("product_category_name_english", "product_category_name")
names = spark.table("bronze.synthetic_product_names").select("product_id", "product_name")

silver_products = (joined
    .withColumn("category", F.coalesce(fix[raw_cat], raw_cat, F.lit("unknown")))
    .withColumn("category_display", F.initcap(F.regexp_replace("category", "_", " ")))
    .join(names, "product_id", "left")
    .select("product_id", "product_name", "category", "category_display",
            F.col("product_category_name").alias("category_pt"),
            F.expr("try_cast(product_name_lenght AS int)").alias("product_name_length"),   # source typo
            F.expr("try_cast(product_description_lenght AS int)").alias("product_description_length"),
            F.expr("try_cast(product_photos_qty AS int)").alias("product_photos_qty"),
            F.expr("try_cast(product_weight_g AS int)").alias("product_weight_g"),
            F.expr("try_cast(product_length_cm AS int)").alias("product_length_cm"),
            F.expr("try_cast(product_height_cm AS int)").alias("product_height_cm"),
            F.expr("try_cast(product_width_cm AS int)").alias("product_width_cm")))
silver_products.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.products")
print("unknown category:", silver_products.where("category = 'unknown'").count(),
      " no name:", silver_products.where("product_name IS NULL").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cust = spark.table("bronze.olist_customers_dataset")
print("rows:", cust.count(),
      " distinct customer_id:", cust.select("customer_id").distinct().count(),
      " distinct customer_unique_id:", cust.select("customer_unique_id").distinct().count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

cust_names = spark.table("bronze.synthetic_customer_names").select("customer_unique_id", "customer_name")

silver_customers = (cust
    .select("customer_id", "customer_unique_id",
            F.col("customer_zip_code_prefix").alias("zip_prefix"),     # stays a STRING: leading zeros
            norm_city("customer_city").alias("city"),
            F.upper(F.trim("customer_state")).alias("state"))
    .join(cust_names, "customer_unique_id", "left"))
silver_customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.customers")

moved = (silver_customers.groupBy("customer_unique_id")
         .agg(F.countDistinct("zip_prefix").alias("zips")).where("zips > 1").count())
print("rows:", silver_customers.count(), " people with 2+ addresses:", moved,
      " no name:", silver_customers.where("customer_name IS NULL").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sell  = spark.table("bronze.olist_sellers_dataset")
zipc  = spark.table("silver.geo_zip_prefix").select("zip_prefix", F.col("city").alias("geo_city"))
snames = spark.table("bronze.synthetic_seller_names").select("seller_id", "seller_name")

city = F.regexp_replace(                                   # 'andira-pr' -> 'andira'
    norm_city(F.split("seller_city", r"\s*[/,]\s*|\s+-\s+").getItem(0)), r"-[a-z]{2}$", "")
silver_sellers = (sell
    .select("seller_id",
            F.col("seller_zip_code_prefix").alias("zip_prefix"),
            city.alias("city_raw"),
            F.upper(F.trim("seller_state")).alias("state"))
    .join(zipc, "zip_prefix", "left")
    .withColumn("city", F.when(F.col("city_raw").rlike("[0-9@]|^[a-z]{2}$|^$"), F.col("geo_city"))
                         .otherwise(F.col("city_raw")))
    .join(snames, "seller_id", "left")
    .select("seller_id", "seller_name", "zip_prefix", "city", "state"))
silver_sellers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.sellers")
print("rows:", silver_sellers.count(),
      " city still junk:", silver_sellers.where(F.col("city").rlike("[/,@0-9]")).count(),
      " no name:", silver_sellers.where("seller_name IS NULL").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

observed = spark.table("silver.geo_zip_prefix").where("geo_source = 'observed'").localCheckpoint()

# Town centers from the same in-Brazil points as Step 2 — a working step, not a table
towns = (spark.table("bronze.olist_geolocation_dataset")
    .select(F.expr("try_cast(geolocation_lat AS double)").alias("lat"),
            F.expr("try_cast(geolocation_lng AS double)").alias("lng"),
            norm_city("geolocation_city").alias("city"),
            F.upper(F.trim("geolocation_state")).alias("state"))
    .where("lat BETWEEN -34 AND 6 AND lng BETWEEN -74 AND -34")
    .groupBy("city", "state").agg(F.expr("percentile(lat, 0.5)").alias("lat"),
                                  F.expr("percentile(lng, 0.5)").alias("lng")))

# Every prefix a customer or seller uses that geo never saw, with its most common town
used = (spark.table("silver.customers").select("zip_prefix", "city", "state")
        .unionByName(spark.table("silver.sellers").select("zip_prefix", "city", "state")))
gaps = (used.join(observed.select("zip_prefix"), "zip_prefix", "left_anti")
        .groupBy("zip_prefix").agg(F.expr("mode(city)").alias("city"), F.expr("mode(state)").alias("state"))
        .join(towns, ["city", "state"], "left")
        .withColumn("geo_source", F.when(F.col("lat").isNotNull(), "city_fallback").otherwise("unresolved")))

(observed.unionByName(gaps, allowMissingColumns=True)
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.geo_zip_prefix"))
spark.sql("DROP TABLE IF EXISTS silver.geo_city")          # superseded: one geo table

geo = spark.table("silver.geo_zip_prefix")
display(geo.groupBy("geo_source").count())
for name in ["customers", "sellers"]:
    t = spark.table(f"silver.{name}")
    print(f"{name:10} no geo row: {t.join(geo, 'zip_prefix', 'left_anti').count():>3}"
          f" | no lat/lng: {t.join(geo, 'zip_prefix').where('lat IS NULL').count():>3}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, LongType

schema_name = "silver"

# 1. Fetch all tables from the specified schema
tables = spark.catalog.listTables(schema_name)

# 2. Iterate and fetch row counts
counts_data = []
for t in tables:
    # Full table path to handle cross-schema queries safely
    full_table_name = f"{schema_name}.{t.name}"
    
    try:
        # Run count on each table DataFrame
        row_count = spark.table(full_table_name).count()
        counts_data.append((full_table_name, row_count))
    except Exception as e:
        print(f"Skipping {full_table_name} due to error: {e}")

# 3. Convert results into a neat summary DataFrame
summary_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("row_count", LongType(), False)
])

df_counts = spark.createDataFrame(counts_data, schema=summary_schema).orderBy(F.col("table_name"))
df_counts.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
