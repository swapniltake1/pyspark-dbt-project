import dlt
from pyspark.sql.functions import (
    col,
    current_timestamp,
    trim,
    upper,
    to_timestamp,
    unix_timestamp
)
from pyspark.sql.types import LongType


# =========================================================
# BRONZE LAYER
# Purpose:
#   Ingest raw flights streaming data from Unity Catalog
# =========================================================
@dlt.table(
    name="stage_flights",
    comment="Bronze layer: Raw flights streaming data"
)

def stage_flights():
    return (
        spark.readStream
             .format("delta")
             .load("/Volumes/airline_catalog/reservation_schema/airline_volume/bronze/flights/data")
    )


# =========================================================
# CLEANED STREAMING VIEW
# Purpose:
#   - Enforce schema casting
#   - Normalize status values
#   - Convert timestamps
#   - Derive flight duration metric
#   - Add audit timestamp
# =========================================================
@dlt.view(
    name="flights_cleaned",
    comment="Standardized and enriched flights dataset ready for CDC"
)

@dlt.expect_or_drop("flight_id_not_null", "flight_id IS NOT NULL")
def flights_cleaned():

    df = dlt.read_stream("stage_flights")

    transformed_df = (
        df.withColumn("modifiedDate", current_timestamp())
          .drop("_rescued_data")
    )

    return transformed_df

dlt.create_streaming_table(name="silver_flights")

# =========================================================
# SILVER LAYER (AUTO CDC - SCD TYPE 1)
# Purpose:
#   - Automatically manage INSERT + UPDATE
#   - Maintain latest flight record state
#   - Internally performs MERGE logic
#
# IMPORTANT:
#   Do NOT define @dlt.table for silver_flights
# =========================================================
dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="flights_cleaned",
    keys=["flight_id"],
    sequence_by=col("flight_id"),
    stored_as_scd_type=1
)