from pyspark.sql import functions as F

# ---------------------------
# 1) Tabla Bronze - Raw Read from Volumes
# ---------------------------
VOLUME_PATH = "/Volumes/projectviews/bronze/raw_files"

@dlt.table(
    name="bronze_projectviews_raw",
    comment="Raw projectviews data from bronze volume"
)
def bronze_projectviews():
    return (
        spark.readStream.format("text")
        .option("recursiveFileLookup", "true")
        .load(VOLUME_PATH)
        .filter(~F.col("value").startswith('""'))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_path", F.col("_metadata.file_path"))
    )