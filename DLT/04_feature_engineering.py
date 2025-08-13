import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dlt.table(
    name="gold_daily_projectviews_patched",
    comment="gold_daily_projectviews padded with missing days as count_views=0 if gaps exist and a label is missing true"
)
def patched_daily():
    
    # check for gaps
    df = dlt.read("gold_daily_projectviews")
    w = Window.partitionBy("domain_code").orderBy("event_date")
    gaps_df = (
        df
        .withColumn("prev_date", F.lag("event_date").over(w)) 
        .withColumn("gap_days", F.datediff("event_date", "prev_date")) 
        .filter(F.col("prev_date").isNotNull() & (F.col("gap_days") > 1))
    )
    print(f'Found {gaps_df.count()} gaps')

    per_dom = df.groupBy("domain_code").agg(
        F.min("event_date").alias("start_date"),
        F.max("event_date").alias("end_date")
    )
    expanded = (
        per_dom
        .withColumn(
            "event_date",
            F.explode(F.sequence("start_date", "end_date", F.expr("interval 1 day")))
        )
        .select("domain_code", "event_date")
    )

    patched = (
        expanded
        .join(df, ["domain_code", "event_date"], "left") 
        .withColumn("is_missing", F.when(F.col("count_views").isNull(), True).otherwise(False))
        .withColumn("count_views", F.coalesce(F.col("count_views"), F.lit(0)).cast("long"))
    )
    return patched


@dlt.table(
    name="gold_projectviews_fe",
    comment="Churn detection over daily views per domain"
)
def gold_churn():
    n = 7
    threshold_factor = 0.3

    # Read the daily patched summary gold table as input
    df = dlt.read("gold_daily_projectviews_patched")

    # Window specification: partition by domain_code and order by event_date
    w_order = Window.partitionBy("domain_code").orderBy("event_date")

    # Rolling average of past 3 days (excluding current day)
    avg_past_3d = F.avg("count_views").over(w_order.rowsBetween(-3, -1))
    df = df.withColumn("avg_views_past_3d", avg_past_3d)

    # Create columns for the next n days' views
    for i in range(1, n+1):
        df = df.withColumn(f"views_plus_{i}", F.lead("count_views", i).over(w_order))

    # Minimum views in the next n days
    future_cols = [F.col(f"views_plus_{i}") for i in range(1, n+1)]
    df = df.withColumn("min_views_future", F.least(*future_cols))

    # Churn threshold = past average views × threshold factor
    df = df.withColumn("threshold", F.col("avg_views_past_3d") * F.lit(threshold_factor))

    # Churn flag = 1 if min future views ≤ threshold, else 0
    df = df.withColumn(
        "churn",
        F.when(F.col("min_views_future") <= F.col("threshold"), F.lit(1)).otherwise(F.lit(0))
    )

    # Filter out rows where past average or future views are null
    df = df.filter(
        F.col("min_views_future").isNotNull() &
        F.col("avg_views_past_3d").isNotNull()
    )
    return df.drop('is_missing')

@dlt.table(
    name="vw_churn_retention",
    comment="Churn and retention funnel aggregated by domain"
)
def gold_churn_retention():
    # Read the churn table from gold layer
    df = dlt.read("gold_projectviews_fe")

    # Window for min join_date per domain
    window_spec = Window.partitionBy("domain_code")

    # Select and enrich data
    df_report = (
        df.select(
            F.col("domain_code"),
            F.col("event_date"),
            F.col("count_views"),
            F.col("churn")
        )
        .withColumn("join_date", F.min("event_date").over(window_spec))
    )

    return df_report
