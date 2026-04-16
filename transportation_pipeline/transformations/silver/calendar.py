# ----------------------------------------
# Silver Layer: Calendar Dimension
# ----------------------------------------
# Generates a full date spine between configurable start/end dates.
# Enriches each date with business-ready time attributes and holiday flags.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# --- Configuration ---
# Date range is controlled via Databricks job/pipeline parameters
start_date = spark.conf.get("start_date")
end_date = spark.conf.get("end_date")


@dp.materialized_view(
    name="transportation.silver.calendar",
    comment="Enriched calendar dimension with time attributes and Indian holidays",
    table_properties={
        "quality": "silver",
        "layer": "silver",

        # Enable Delta Lake optimisations + change tracking
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def calendar():

    # --- Generate Date Spine ---
    # Creates a continuous sequence of dates for the reporting period
    df = spark.sql(
        f"""
        SELECT explode(sequence(
            to_date('{start_date}'),
            to_date('{end_date}'),
            interval 1 day
        )) AS date
        """
    )

    # --- Core Date Attributes ---
    # Used for joins, partitioning, and time-based analytics
    df = (
        df.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
          .withColumn("year", F.year("date"))
          .withColumn("month", F.month("date"))
          .withColumn("quarter", F.quarter("date"))
    )

    # --- Day-Level Attributes ---
    # Enables weekday/weekend filtering and behavioural analysis
    df = (
        df.withColumn("day_of_month", F.dayofmonth("date"))
          .withColumn("day_of_week", F.date_format("date", "EEEE"))
          .withColumn("day_of_week_abbr", F.date_format("date", "EEE"))
          .withColumn("day_of_week_num", F.dayofweek("date"))
    )

    # --- Month / Quarter Labels ---
    # Useful for reporting layers and BI-friendly dimensions
    df = (
        df.withColumn("month_name", F.date_format("date", "MMMM"))
          .withColumn(
              "month_year",
              F.concat(F.date_format("date", "MMMM"), F.lit(" "), F.col("year"))
          )
          .withColumn(
              "quarter_year",
              F.concat(F.lit("Q"), F.col("quarter"), F.lit(" "), F.col("year"))
          )
    )

    # --- Week-Level Attributes ---
    df = (
        df.withColumn("week_of_year", F.weekofyear("date"))
          .withColumn("day_of_year", F.dayofyear("date"))
    )

    # --- Business Flags ---
    # Used for segmentation in analytics (weekday vs weekend behaviour)
    df = (
        df.withColumn(
            "is_weekend",
            F.col("day_of_week_num").isin([1, 7])  # Sunday/Saturday
        )
        .withColumn(
            "is_weekday",
            ~F.col("day_of_week_num").isin([1, 7])
        )
    )

    # --- Holiday Logic (India-specific) ---
    # Hardcoded national holidays for business reporting consistency
    df = (
        df.withColumn(
            "holiday_name",
            F.when((F.month("date") == 1) & (F.dayofmonth("date") == 26), "Republic Day")
             .when((F.month("date") == 8) & (F.dayofmonth("date") == 15), "Independence Day")
             .when((F.month("date") == 10) & (F.dayofmonth("date") == 2), "Gandhi Jayanti")
        )
        .withColumn(
            "is_holiday",
            F.col("holiday_name").isNotNull()
        )
    )

    # --- Audit Column ---
    # Tracks when this record was generated in the pipeline
    df = df.withColumn("silver_processed_timestamp", F.current_timestamp())

    # --- Final Projection ---
    # Explicit column ordering for clean downstream consumption
    return df.select(
        "date",
        "date_key",
        "year",
        "month",
        "day_of_month",
        "day_of_week",
        "day_of_week_abbr",
        "month_name",
        "month_year",
        "quarter",
        "quarter_year",
        "week_of_year",
        "day_of_year",
        "is_weekday",
        "is_weekend",
        "is_holiday",
        "holiday_name",
        "silver_processed_timestamp"
    )
