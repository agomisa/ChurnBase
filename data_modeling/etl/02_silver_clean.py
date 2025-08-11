from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, LongType, TimestampType, DateType
)
from datetime import datetime
import json
import uuid

spark = SparkSession.builder.getOrCreate()

# ---------------------------
# Config
# ---------------------------
VOLUME_PATH      = "/Volumes/projectviews/bronze/raw_files"   # bronze volume path
SILVER_TABLE     = "projectviews.silver.projectviews_clean"
QUAR_TABLE       = "projectviews.silver.projectviews_quarantine"
DQ_REPORT_TABLE  = "projectviews.silver.dq_reports"

DQ_CONFIG = {
    "domain_code_regex": r"^[a-z]+(\.[a-z]+)*$",
    "max_null_pct": 0.05,                # if > 5% null in critical col -> alert (kept for report)
    "min_rows_expected": 1
}

RUN_ID = str(uuid.uuid4())  # trace a single run across outputs

# ---------------------------
# 1) Parse raw DataFrame
# ---------------------------
def parse_raw_df(bronze_path: str) -> DataFrame:
    """
    Read raw plain text lines from bronze volume and extract:
      - domain_code (field 0)
      - count_views (field 2)
      - event_timestamp/event_date from filename: projectviews-YYYYMMDD-HHMMSS.*
    """
    raw = (
        spark.read.format("text")
        .option("recursiveFileLookup", "true")
        .load(bronze_path)
        .filter(~F.col("value").startswith('""'))  # keep as-is from your snippet
    )

    # Split line into 4 tokens: we keep 0 and 2 as in your code
    split_col = F.split(F.col("value"), " ", 4)

    parsed = (
        raw
        .withColumn("split", split_col)
        .filter(F.size("split") == 4)
        .select(
            F.col("split")[0].alias("domain_code"),
            F.col("split")[2].cast("long").alias("count_views"),
            F.col("_metadata.file_path").alias("file_path")
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
        .drop("date_str", "time_str", "datetime_concat")
    )

    # Optional: repartition by event_date for better downstream IO
    parsed = parsed.repartition("event_date")
    return parsed


# ---------------------------
# 2) Enforce schema + casts
# ---------------------------
def enforce_and_cast_schema(df: DataFrame) -> DataFrame:
    # make sure all expected columns exist and have consistent types
    needed = {
        "domain_code": StringType(),
        "count_views": LongType(),
        "event_timestamp": TimestampType(),
        "event_date": DateType(),
        "run_id": StringType(),
        "file_path": StringType()
    }
    for c, t in needed.items():
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(t))
        else:
            df = df.withColumn(c, F.col(c).cast(t))
    return df


# ---------------------------
# 3) DQ checks
# ---------------------------
def run_dq_checks(df: DataFrame, config: dict = DQ_CONFIG) -> dict:
    """
    Returns dict with:
      - valid_df: rows passing checks
      - quarantine_df: rows failing >=1 check
      - dq_report: small DF with JSON report
    """
    total_rows = df.count()

    expected_cols = ["domain_code", "count_views", "event_timestamp", "event_date"]
    missing_cols = [c for c in expected_cols if c not in df.columns]

    # Null counts/pct (single pass per column)
    null_counts = {}
    null_pct = {}
    for c in expected_cols:
        cnt = df.filter(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")).count() if c in df.columns else total_rows
        null_counts[c] = cnt
        null_pct[c] = (cnt / total_rows) if total_rows > 0 else None

    # Domain regex validity
    regex = config["domain_code_regex"]
    invalid_domain_df = df.filter(F.col("domain_code").isNull() | (~F.col("domain_code").rlike(regex)))
    invalid_domain_cnt = invalid_domain_df.count()

    # Non-negative
    negative_vals_df = df.filter(F.col("count_views") < 0)
    negative_cnt = negative_vals_df.count()

    # Build quarantine (union)
    failing_frames = []
    if invalid_domain_cnt > 0:
        failing_frames.append(invalid_domain_df.withColumn("dq_reason", F.lit("invalid_domain")))
    if negative_cnt > 0:
        failing_frames.append(negative_vals_df.withColumn("dq_reason", F.lit("negative_values")))

    if failing_frames:
        quarantine_df = failing_frames[0]
        for f in failing_frames[1:]:
            quarantine_df = quarantine_df.unionByName(f, allowMissingColumns=True)
        quarantine_df = quarantine_df.dropDuplicates()
        # valid = df EXCEPT quarantine (full-row hash anti-join)
        from pyspark.sql.functions import sha2, concat_ws
        hash_col = "row_hash"
        df_h = df.withColumn(hash_col, sha2(concat_ws("||", *df.columns), 256))
        q_h  = quarantine_df.withColumn(hash_col, sha2(concat_ws("||", *quarantine_df.columns), 256))
        valid_df = df_h.join(q_h.select(hash_col).withColumn("flag", F.lit(1)), on=hash_col, how="left_anti").drop(hash_col)
    else:
        quarantine_df = df.limit(0)
        valid_df = df

    dq_summary = {
        "run_id": RUN_ID,
        "total_rows": total_rows,
        "checks": {
            "missing_columns": missing_cols,
            "null_counts": null_counts,
            "null_pct": null_pct,
            "invalid_domain_count": invalid_domain_cnt,
            "negative_values_count": negative_cnt,
            "min_rows_ok": total_rows >= config.get("min_rows_expected", 1),
        },
        "generated_at": datetime.utcnow().isoformat() + "Z"
    }

    dq_report_df = spark.createDataFrame(
        [(json.dumps(dq_summary), RUN_ID, datetime.utcnow())],
        ["dq_report_json", "run_id", "execution_timestamp"]
    )

    return {"valid_df": valid_df, "quarantine_df": quarantine_df, "dq_report": dq_report_df}


# ---------------------------
# 4) Write outputs (Delta UC tables)
# ---------------------------
def write_outputs(valid_df: DataFrame, quarantine_df: DataFrame, dq_report_df: DataFrame,
                  silver_table: str, quarantine_table: str, dq_report_table: str,
                  partition_by: list = ["event_date"]):
    (
        valid_df.write
        .format("delta")
        .mode("overwrite") # append
        .partitionBy(*partition_by)
        .saveAsTable(silver_table)
    )

    if quarantine_df.count() > 0:
        (
            quarantine_df.write
            .format("delta")
            .mode("overwrite") # append
            .saveAsTable(quarantine_table)
        )

    (
        dq_report_df.write
        .format("delta")
        .mode("overwrite") # append
        .saveAsTable(dq_report_table)
    )


# ---------------------------
# Orchestration (run)
# ---------------------------
if __name__ == "__main__":
    parsed = parse_raw_df(VOLUME_PATH)
    parsed_c = enforce_and_cast_schema(parsed)
    results = run_dq_checks(parsed_c, config=DQ_CONFIG)

    write_outputs(
        results["valid_df"],
        results["quarantine_df"],
        results["dq_report"],
        SILVER_TABLE,
        QUAR_TABLE,
        DQ_REPORT_TABLE
    )

    print(f"Silver written: {SILVER_TABLE} | Quarantine: {QUAR_TABLE} | DQ: {DQ_REPORT_TABLE} | run_id={RUN_ID}")
