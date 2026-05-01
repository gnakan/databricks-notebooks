from pyspark import pipelines as dp


@dp.table(
    name="bronze_click_events",
    comment="Raw click event data ingested from JSON files via Auto Loader"
)
def bronze_click_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/gnakan_experiments/tl_experiments/click_events_raw/")
    )
