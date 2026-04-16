# ----------------------------------------
# Silver Layer: Trips Data (Staging + CDC)
# ----------------------------------------
# Cleans and validates raw trip data from Bronze layer.
# Applies data quality rules, standardises schema, and prepares data for CDC upsert.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# =========================================================
# STAGING VIEW (Cleans + Validates Raw Streaming Data)
# =========================================================

@dp.view(
    name="trips_silver_staging",
    comment="Validated and transformed trips data prepared for CDC processing"
)

# --- Data Quality Rules (Expectation Layer) ---
# These ensure only valid records are passed downstream
@dp.expect("valid_date", "year(business_date) >= 2020")
@dp.expect("valid_driver_rating", "driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")
def trips_silver():

    # --- Stream from Bronze Layer ---
    # Reads continuously from Auto Loader output table
    df_bronze = spark.readStream.table("transportation.bronze.trips")

    # --- Standardise Text Fields ---
    # Ensures consistent categorisation for analytics
    df_bronze = df_bronze.withColumn(
        "passenger_type",
        F.lower("passenger_type")
    )

    # --- Select + Rename Columns ---
    # Keeps only business-relevant attributes for Silver layer
    df_silver = df_bronze.select(
        F.col("trip_id").alias("id"),
        F.col("date").cast("date").alias("business_date"),
        F.col("city_id").alias("city_id"),
        F.col("passenger_type").alias("passenger_category"),
        F.col("distance_travelled_km").alias("distance_kms"),
        F.col("fare_amount").alias("sales_amt"),
        F.col("passenger_rating"),
        F.col("driver_rating"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp"),
    )

    # --- Add Processing Metadata ---
    # Tracks when this Silver transformation occurred
    df_silver = df_silver.withColumn(
        "silver_processed_timestamp",
        F.current_timestamp()
    )

    return df_silver


# =========================================================
# SILVER TABLE (CDC TARGET)
# =========================================================

# Persistent Silver table storing cleaned + deduplicated trips
dp.create_streaming_table(
    name="transportation.silver.trips",
    comment="Cleaned and validated trips table with CDC upsert support",
    table_properties={
        "quality": "silver",
        "layer": "silver",

        # Delta Lake optimisations + change tracking
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)


# =========================================================
# CDC UPSERT FLOW (SCD Type 1)
# =========================================================

# Ensures no duplicate trips and maintains latest record per trip ID
dp.create_auto_cdc_flow(
    target="transportation.silver.trips",
    source="trips_silver_staging",
    keys=["id"],

    # Uses processing timestamp to resolve latest record
    sequence_by=F.col("silver_processed_timestamp"),

    stored_as_scd_type=1,
    except_column_list=[],
)
