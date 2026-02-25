import dlt
from pyspark.sql.functions import col, current_timestamp, trim, initcap, upper
from pyspark.sql.types import LongType

# =========================================================
# BRONZE LAYER
# =========================================================
@dlt.table(
    name="stage_airports",
    comment="Bronze layer: Raw airports streaming data"
)
def stage_airports():
    return (
        spark.readStream
             .format("delta")
             .load("/Volumes/airline_catalog/reservation_schema/airline_volume/bronze/airports/data")
    )

# =========================================================
# CLEANED VIEW
# =========================================================
@dlt.view(
    name="airports_cleaned",
    comment="Standardized airports dataset prepared for CDC"
)

#@dlt.expect("airport_id_not_null", "airport_id IS NOT NULL")
def airports_cleaned():
    df = dlt.read_stream("stage_airports")
    df = df.withColumn("modifiedDate", current_timestamp())\
           .drop("_rescued_data")
    return df

# =========================================================
# DECLARE TARGET TABLE (REQUIRED FOR CDC)
# =========================================================
dlt.create_streaming_table(
    name="silver_airports",
    comment="Silver airports table managed via CDC"
)

# =========================================================
# AUTO CDC FLOW
# =========================================================
dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="airports_cleaned",
    keys=["airport_id"],
    sequence_by=col("airport_id"),
    stored_as_scd_type=1
)