# ----------------------------------------
# Bronze Layer: Trips Data Ingestion
# ----------------------------------------
# Streams raw trip data from AWS S3 using Auto Loader.
# Handles schema evolution, renames inconsistent columns, and adds ingestion metadata.

from pyspark import pipelines as dp
import pyspark.sql.functions as F

# --- Configuration ---
# Source location of raw trip data in S3
SOURCE_PATH = "s3://goodcabs-lljx1/data-store/trips"


@dp.table(
    name="transportation.bronze.trips",
    comment="Streaming ingestion of raw trip data using Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",

        # Delta Lake optimisations + change tracking
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def trips_bronze():

    # --- Stream Ingestion via Auto Loader ---
    # cloudFiles enables incremental processing of new S3 files
    # schemaEvolutionMode="rescue" captures unexpected columns safely
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)  # controls ingestion throughput
        .load(SOURCE_PATH)
    )

    # --- Standardise Column Names ---
    # Fix inconsistent schema from raw CSV (important for downstream joins)
    df = df.withColumnRenamed(
        "distance_travelled(km)",
        "distance_travelled_km"
    )

    # --- Add Ingestion Metadata ---
    # Used for lineage tracking and debugging pipeline issues
    df = (
        df.withColumn("file_name", F.col("_metadata.file_path"))
          .withColumn("ingest_datetime", F.current_timestamp())
    )

    return df
