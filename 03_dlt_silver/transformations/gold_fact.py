import dlt
from pyspark.sql.functions import col


@dlt.table(
    name="gold_booking_master",
    comment="Business-ready consolidated booking table"
)
def gold_booking_master():

    # Read silver tables as STATIC sources (not streaming)
    bookings = dlt.read("silver_bookings").alias("b")
    passengers = dlt.read("silver_passengers").alias("p")
    flights = dlt.read("silver_flights").alias("f")
    airports = dlt.read("silver_airports").alias("a")

    return (
        bookings
            .join(passengers, col("b.passenger_id") == col("p.passenger_id"), "left")
            .join(flights, col("b.flight_id") == col("f.flight_id"), "left")
            .join(airports, col("b.airport_id") == col("a.airport_id"), "left")
            .select(
                col("b.booking_id"),
                col("b.booking_date"),
                col("b.amount"),
                col("p.name"),
                col("p.gender"),
                col("p.nationality"),
                col("f.airline"),
                col("f.origin"),
                col("f.destination"),
                col("f.flight_date"),
                col("a.airport_name"),
                col("a.city"),
                col("a.country")
            )
    )