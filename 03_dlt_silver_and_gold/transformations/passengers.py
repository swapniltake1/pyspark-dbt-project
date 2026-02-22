import dlt
from pyspark.sql.functions import col, current_timestamp, trim, initcap, lower
from pyspark.sql.types import LongType


# =========================================================
# BRONZE LAYER
# Purpose:
#   Ingest raw passengers streaming data
# =========================================================
@dlt.table(
    name="stage_passengers",
    comment="Bronze layer: Raw passengers data"
)
def stage_passengers():
    return (
        spark.readStream
             .format("delta")
             .load("/Volumes/airline_catalog/reservation_schema/airline_volume/bronze/customers/data")
    )


# =========================================================
# CLEANED STREAMING VIEW
# Purpose:
#   - Standardize text columns
#   - Cast business keys
#   - Add audit timestamp
#   - Remove rescued column
# =========================================================
@dlt.view(
    name="passengers_cleaned",
    comment="Standardized passengers dataset prepared for CDC"
)

@dlt.expect_or_drop("passenger_id_not_null", "passenger_id IS NOT NULL")
def passengers_cleaned():

    df = dlt.read_stream("stage_passengers")

    transformed_df = (
        df.withColumn("modifiedDate", current_timestamp())
          .drop("_rescued_data")
    )

    return transformed_df

dlt.create_streaming_table(name="silver_passengers")
# =========================================================
# SILVER LAYER (AUTO CDC - SCD TYPE 1)
# Purpose:
#   - Automatically manage inserts + updates
#   - Maintain latest state of passenger record
#   - DLT internally performs MERGE
#
# IMPORTANT:
#   Do NOT define @dlt.table for silver_passengers
# =========================================================
dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="passengers_cleaned",
    keys=["passenger_id"],
    sequence_by=col("passenger_id"),
    stored_as_scd_type=1
)