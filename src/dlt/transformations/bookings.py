import dlt
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DoubleType


# =========================================================
# BRONZE LAYER
# Purpose:
#   Ingest raw streaming bookings data from Unity Catalog Volume
# =========================================================
@dlt.table(
    name="stage_bookings",
    comment="Bronze layer: Raw streaming bookings data"
)
def stage_bookings():
    return (
        spark.readStream
             .format("delta")
             .load("/Volumes/airline_catalog/reservation_schema/airline_volume/bronze/bookings/data")
    )


# =========================================================
# CLEANED STREAMING VIEW
# Purpose:
#   - Cast numeric columns
#   - Add audit column
#   - Remove rescued data column
#   - Prepare dataset for CDC processing
# =========================================================
@dlt.view(
    name="bookings_cleaned",
    comment="Standardized and enriched bookings data ready for CDC"
)

@dlt.expect_or_drop("booking_id_not_null", "booking_id IS NOT NULL")
@dlt.expect_or_drop("passenger_id_not_null", "passenger_id IS NOT NULL")
@dlt.expect_or_drop("flight_id_not_null", "flight_id IS NOT NULL")
@dlt.expect_or_drop("airport_id_not_null", "airport_id IS NOT NULL")
@dlt.expect_or_drop("amount_positive", "amount > 0")
@dlt.expect_or_drop("valid_currency_format", "amount IS NOT NULL")
def bookings_cleaned():

    df = dlt.read_stream("stage_bookings")

    transformed_df = (
        df.withColumn("amount", col("amount").cast(DoubleType()))
          .withColumn("modifiedDate", current_timestamp())
          .drop("_rescued_data")
    )

    return transformed_df

dlt.create_streaming_table(name="silver_bookings")
# =========================================================
# SILVER LAYER (AUTO CDC - SCD TYPE 1)
# Purpose:
#   - Automatically manage INSERT + UPDATE
#   - Maintain latest state of booking record
#   - Perform managed MERGE internally
#
# Important:
#   - Do NOT define @dlt.table for silver_bookings
#   - DLT automatically creates and manages this table
# =========================================================
dlt.create_auto_cdc_flow(
    target="silver_bookings",          # Target table created by DLT
    source="bookings_cleaned",         # Cleaned streaming source
    keys=["booking_id"],               # Business key
    sequence_by=col("booking_id"),   # Ordering column for updates
    stored_as_scd_type=1               # SCD Type 1 (latest state only)
)