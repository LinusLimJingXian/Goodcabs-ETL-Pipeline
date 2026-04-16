# ----------------------------------------
# Bronze Layer: City Data Ingestion
# ----------------------------------------
# Reads raw city data from AWS S3 (CSV format),
# adds ingestion metadata, and stores it as a Delta table.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# --- Configuration ---
# Source location of raw city CSV files in S3
SOURCE_PATH = "s3://goodcabs-lljx1/data-store/city"


@dp.materialized_view(
    name="transportation.bronze.city",
    comment="Raw city data ingested from S3 with minimal transformation",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",

        # Enable Delta Lake optimisations + change tracking
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():

    # --- Read Raw Data ---
    # Batch read (dimension table) with schema inference
    # PERMISSIVE mode captures malformed records instead of failing
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("mergeSchema", "true")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(SOURCE_PATH)
    )

    # --- Add Metadata Columns ---
    # Track data lineage and ingestion timing for auditing/debugging
    df = (
        df.withColumn("file_name", col("_metadata.file_path"))
          .withColumn("ingest_datetime", current_timestamp())
    )

    return df
