CREATE SCHEMA IF NOT EXISTS mabi_analytics;

CREATE TABLE IF NOT EXISTS mabi_analytics.item_display_hourly_stats (
  bucket_hour timestamptz NOT NULL,
  item_display_name varchar(100) NOT NULL,

  reg_price_max bigint,
  reg_price_min bigint,
  reg_price_avg numeric,
  reg_count bigint,

  sold_price_max bigint,
  sold_price_min bigint,
  sold_price_avg numeric,
  sold_count bigint,

  PRIMARY KEY (bucket_hour, item_display_name)
);


