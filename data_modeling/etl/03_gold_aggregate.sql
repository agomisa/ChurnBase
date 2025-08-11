
CREATE OR REFRESH LIVE TABLE projectviews_gold_projectviews_daily_summary
PARTITIONED BY (event_date)
AS
SELECT
  domain_code,
  event_date,
  SUM(count_views) AS count_views
FROM LIVE.projectviews_silver_projectviews_clean
GROUP BY domain_code, event_date