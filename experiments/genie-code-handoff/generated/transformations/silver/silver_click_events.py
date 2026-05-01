from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_click_events",
    comment="Cleaned and deduplicated click events with data quality enforcement"
)
@dp.expect_all_or_drop({
    "valid_user_id": "user_id IS NOT NULL",
    "valid_session_id": "session_id IS NOT NULL",
    "valid_event_timestamp": "event_timestamp IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL",
    "recent_event": "event_timestamp >= current_timestamp() - INTERVAL 90 DAYS"
})
def silver_click_events():
    return (
        spark.readStream.table("bronze_click_events")
        .withColumn("event_timestamp", F.col("event_timestamp").cast("timestamp"))
        .withWatermark("event_timestamp", "1 hour")
        .dropDuplicatesWithinWatermark(["event_id"])
        .select(
            F.col("event_id").cast("string"),
            F.col("user_id").cast("string"),
            F.col("session_id").cast("string"),
            F.col("event_timestamp"),
            F.col("event_type").cast("string"),
            F.col("device_type").cast("string")
        )
    )
