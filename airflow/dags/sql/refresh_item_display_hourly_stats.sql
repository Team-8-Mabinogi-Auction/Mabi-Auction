-- Recomputes the last 2 hours each run to handle late-arriving events.
WITH params AS (
  SELECT
    (TIMESTAMPTZ '{{ data_interval_start }}' - INTERVAL '1 hour') AS window_start,
    (TIMESTAMPTZ '{{ data_interval_end }}') AS window_end
),
reg AS (
  SELECT
    date_trunc('hour', l.registered_at) AS bucket_hour,
    l.item_display_name,
    max(l.price) AS reg_price_max,
    min(l.price) AS reg_price_min,
    avg(l.price)::numeric AS reg_price_avg,
    count(*) AS reg_count
  FROM mabi_auction.auction_logs l
  CROSS JOIN params p
  WHERE l.event_type = 'REGISTER'
    AND l.registered_at IS NOT NULL
    AND l.registered_at >= p.window_start
    AND l.registered_at < p.window_end
  GROUP BY 1, 2
),
sold AS (
  SELECT
    date_trunc('hour', l.ended_at) AS bucket_hour,
    l.item_display_name,
    max(l.price) AS sold_price_max,
    min(l.price) AS sold_price_min,
    avg(l.price)::numeric AS sold_price_avg,
    count(*) AS sold_count
  FROM mabi_auction.auction_logs l
  CROSS JOIN params p
  WHERE l.event_type = 'SOLD'
    AND l.ended_at IS NOT NULL
    AND l.ended_at >= p.window_start
    AND l.ended_at < p.window_end
  GROUP BY 1, 2
),
merged AS (
  SELECT
    COALESCE(reg.bucket_hour, sold.bucket_hour) AS bucket_hour,
    COALESCE(reg.item_display_name, sold.item_display_name) AS item_display_name,

    reg.reg_price_max,
    reg.reg_price_min,
    reg.reg_price_avg,
    reg.reg_count,

    sold.sold_price_max,
    sold.sold_price_min,
    sold.sold_price_avg,
    sold.sold_count
  FROM reg
  FULL OUTER JOIN sold
    ON reg.bucket_hour = sold.bucket_hour
   AND reg.item_display_name = sold.item_display_name
)
INSERT INTO mabi_analytics.item_display_hourly_stats (
  bucket_hour,
  item_display_name,
  reg_price_max,
  reg_price_min,
  reg_price_avg,
  reg_count,
  sold_price_max,
  sold_price_min,
  sold_price_avg,
  sold_count
)
SELECT
  bucket_hour,
  item_display_name,
  reg_price_max,
  reg_price_min,
  reg_price_avg,
  reg_count,
  sold_price_max,
  sold_price_min,
  sold_price_avg,
  sold_count
FROM merged
ON CONFLICT (bucket_hour, item_display_name) DO UPDATE SET
  reg_price_max   = EXCLUDED.reg_price_max,
  reg_price_min   = EXCLUDED.reg_price_min,
  reg_price_avg   = EXCLUDED.reg_price_avg,
  reg_count       = EXCLUDED.reg_count,
  sold_price_max  = EXCLUDED.sold_price_max,
  sold_price_min  = EXCLUDED.sold_price_min,
  sold_price_avg  = EXCLUDED.sold_price_avg,
  sold_count      = EXCLUDED.sold_count;


