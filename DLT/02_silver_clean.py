import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, TimestampType, DateType
from datetime import datetime
import json
import uuid

# Configuration

DQ_CONFIG = {
    "domain_code_regex": r"^[a-z]+(\.[a-z]+)*$",
    "min_rows_expected": 1
}
RUN_ID = str(uuid.uuid4())

# ---------------------------
# Parse Function
# ---------------------------
def parse_bronze_data(bronze_df):
    split_col = F.split(F.col("value"), " ", 4)
    parsed = (
        bronze_df
        .withColumn("split", split_col)
        .filter(F.size("split") == 4)
        .select(
            F.col("split")[0].alias("domain_code"),
            F.col("split")[2].cast("long").alias("count_views"),
            F.col("_source_path").alias("file_path"),
            F.col("_ingested_at")
        )
        .withColumn("date_str", F.regexp_extract(F.col("file_path"), r"projectviews-(\d{8})-(\d{6})", 1))
        .withColumn("time_str", F.regexp_extract(F.col("file_path"), r"projectviews-(\d{8})-(\d{6})", 2))
        .withColumn(
            "datetime_concat",
            F.when(
                (F.col("date_str") != "") & (F.col("time_str") != ""),
                F.concat_ws("", F.col("date_str"), F.col("time_str"))
            )
        )
        .withColumn(
            "event_timestamp",
            F.when(
                F.length(F.col("datetime_concat")) == 14,
                F.to_timestamp(F.col("datetime_concat"), "yyyyMMddHHmmss")
            ).cast(TimestampType())
        )
        .withColumn("event_date", F.to_date(F.col("event_timestamp")).cast(DateType()))
        .withColumn("run_id", F.lit(RUN_ID))
        .drop("split", "date_str", "time_str", "datetime_concat")
        .withColumn("domain_code", F.col("domain_code").cast(StringType()))
        .withColumn("count_views", F.col("count_views").cast(LongType()))
    )
    return parsed

# ---------------------------
# 2) Silver Clean Table
# ---------------------------
@dlt.table(
    name="silver_projectviews_clean",
    comment="Clean projectviews data with DQ validations",
    partition_cols=["event_date"],
    table_properties={
        "pipelines.reset.allowed": "true"
    }
)
@dlt.expect_all({
    "valid_domain": f"domain_code RLIKE '{DQ_CONFIG['domain_code_regex']}'",
    "positive_views": "count_views >= 0",
    "not_null_domain": "domain_code IS NOT NULL",
    "not_null_timestamp": "event_timestamp IS NOT NULL"
})
def silver_clean():
    bronze_df = dlt.read_stream("bronze_projectviews_raw")
    parsed = parse_bronze_data(bronze_df)
    return parsed.filter(
        F.col("domain_code").isNotNull() &
        F.col("domain_code").rlike(DQ_CONFIG["domain_code_regex"]) &
        (F.col("count_views") >= 0) &
        F.col("event_timestamp").isNotNull()
    )

# ---------------------------
# 3) Quarantine Table
# ---------------------------
@dlt.table(
    name="silver_projectviews_quarantine",
    comment="Records that failed DQ checks",
    table_properties={
        "pipelines.reset.allowed": "true"
    }
)
def silver_quarantine():
    bronze_df = dlt.read_stream("bronze_projectviews_raw")
    parsed = parse_bronze_data(bronze_df)
    return parsed.filter(
        F.col("domain_code").isNull() |
        (~F.col("domain_code").rlike(DQ_CONFIG["domain_code_regex"])) |
        (F.col("count_views") < 0) |
        F.col("event_timestamp").isNull()
    ).withColumn(
        "dq_reason",
        F.when(F.col("domain_code").isNull(), "null_domain")
        .when(~F.col("domain_code").rlike(DQ_CONFIG["domain_code_regex"]), "invalid_domain")
        .when(F.col("count_views") < 0, "negative_views")
        .when(F.col("event_timestamp").isNull(), "null_timestamp")
        .otherwise("unknown_dq_failure")
    )

# ---------------------------
# 4) DQ Report Table
# ---------------------------
@dlt.table(
    name="silver_dq_reports",
    comment="Data quality metrics and reports",
    table_properties={
        "pipelines.reset.allowed": "true"
    }
)
def dq_reports():
    clean_df = dlt.read("silver_projectviews_clean")
    total_count = clean_df.count()
    dq_summary = {
        "run_id": RUN_ID,
        "total_valid_rows": total_count,
        "execution_timestamp": datetime.utcnow().isoformat() + "Z",
        "table_name": "silver_projectviews_clean"
    }
    return spark.createDataFrame(
        [(json.dumps(dq_summary), RUN_ID, datetime.utcnow())],
        ["dq_report_json", "run_id", "execution_timestamp"]
    )
