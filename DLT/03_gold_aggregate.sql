CREATE OR REPLACE LIVE TABLE gold_daily_projectviews
PARTITIONED BY (event_date)
AS
SELECT
  domain_code,
  event_date,
  SUM(count_views) AS count_views
FROM projectviews.default.silver_projectviews_clean
GROUP BY domain_code, event_date