# ----------------------------------------
# Silver Layer: City Dimension
# ----------------------------------------
# Standardises city reference data from Bronze layer.
# Keeps only business-relevant fields and adds lineage tracking.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="transportation.silver.city",
    comment="Cleaned city dimension table for analytics and joins",
    table_properties={
        "quality": "silver",
        "layer": "silver",

        # Enable Delta Lake optimisations + change tracking
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    }
)
def city_silver():

    # --- Read from Bronze Layer ---
    # Source of truth for raw city reference data
    df_bronze = spark.read.table("transportation.bronze.city")

    # --- Standardise Schema ---
    # Keep only required business attributes for downstream joins
    df_silver = df_bronze.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp")
    )

    # --- Add Silver Metadata ---
    # Tracks when this cleaned version was created
    df_silver = df_silver.withColumn(
        "silver_processed_timestamp",
        F.current_timestamp()
    )

    return df_silver
