CREATE TABLE addotnet.ad_event_daily
(
    event_date            Date   DEFAULT toDate('1975-01-01'),
    event_month_id        Int32,
    event_month_name      String,
    event_year            UInt16,
    advertiser_lid        Int64  DEFAULT toInt64(0),
    advertiser_hid        Int64  DEFAULT toInt64(0),
    campaign_id           Int64  DEFAULT toInt64(0),
    adgroup_lid           Int64  DEFAULT toInt64(0),
    adgroup_hid           Int64  DEFAULT toInt64(0),
    provider_account_lid  Int64  DEFAULT toInt64(0),
    provider_account_hid  Int64  DEFAULT toInt64(0),
    feed_advertiser_id    Int64  DEFAULT toInt64(0),
    affiliate_account_lid Int64  DEFAULT toInt64(0),
    affiliate_account_hid Int64  DEFAULT toInt64(0),
    sid                   Int64  DEFAULT toInt64(0),
    said                  String DEFAULT '',
    requests              Nullable(UInt64),
    ad_returns            Nullable(UInt64),
    raw_clicks            Nullable(UInt64),
    paid_clicks           Nullable(UInt64),
    pub_payout            Nullable(Float64),
    revenue               Nullable(Float64),
    dollars_worth         Nullable(Float64),
    actions_worth         Nullable(Float64),
    event_fires_count     Nullable(Int64)
)
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/ad_event_daily/{shard}',
             '{replica}') PARTITION BY (event_date) PRIMARY KEY
        (
         event_date,
         event_month_id,
         event_year,
         affiliate_account_lid,
         affiliate_account_hid,
         sid,
         provider_account_lid,
         provider_account_hid,
         feed_advertiser_id,
         advertiser_lid,
         advertiser_hid,
         campaign_id,
         adgroup_lid,
         adgroup_hid,
         said
            )
        ORDER BY
            (
             event_date,
             event_month_id,
             event_year,
             affiliate_account_lid,
             affiliate_account_hid,
             sid,
             provider_account_lid,
             provider_account_hid,
             feed_advertiser_id,
             advertiser_lid,
             advertiser_hid,
             campaign_id,
             adgroup_lid,
             adgroup_hid,
             said
                )
        SETTINGS index_granularity = 8192;


CREATE MATERIALIZED VIEW etl.ad_event_rollup_mat to addotnet.ad_event_daily
AS
select event_date,
       toYYYYMM(event_date)                                                   event_month_id,
       toString(formatDateTime(event_date, '%Y-%m'))                          event_month_name,
       event_year,
       coalesce(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END) advertiser_lid,
       coalesce(CASE WHEN advertiser_hid = -1 THEN 0 ELSE advertiser_hid END) advertiser_hid,
       coalesce(ad_event.campaign_id, 0)                                      campaign_id,
       coalesce(ad_event.adgroup_lid, 0)                                      adgroup_lid,
       coalesce(ad_event.adgroup_hid, 0)                                      adgroup_hid,
       coalesce(ad_event.provider_account_lid, 0)                             provider_account_lid,
       coalesce(ad_event.provider_account_hid, 0)                             provider_account_hid,
       coalesce(ad_event.feed_advertiser_id, 0)                               feed_advertiser_id,
       coalesce(ad_event.affiliate_account_lid, 0)                            affiliate_account_lid,
       coalesce(ad_event.affiliate_account_hid, 0)                            affiliate_account_hid,
       sid,
       said,
       requests,
       ad_returns,
       raw_clicks,
       paid_clicks,
       pub_payout,
       revenue,
       dollars_worth,
       actions_worth,
       case
           when actions_worth > 0 then 1
           when actions_worth < 0 then -1
           else 0 end                                                         event_fires_count
from addotnet.ad_event
where event_type <> 'feed_return';

insert into backfill.ad_event_daily
select event_date,
       toYYYYMM(event_date)                                                   event_month_id,
       toString(formatDateTime(event_date, '%Y-%m'))                          event_month_name,
       toUInt16(formatDateTime(event_date, '%Y'))                             event_year,
       coalesce(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END) advertiser_lid,
       coalesce(CASE WHEN advertiser_hid = -1 THEN 0 ELSE advertiser_hid END) advertiser_hid,
       coalesce(ad_event.campaign_id, 0)                                      campaign_id,
       coalesce(ad_event.adgroup_lid, 0)                                      adgroup_lid,
       coalesce(ad_event.adgroup_hid, 0)                                      adgroup_hid,
       coalesce(ad_event.provider_account_lid, 0)                             provider_account_lid,
       coalesce(ad_event.provider_account_hid, 0)                             provider_account_hid,
       coalesce(ad_event.feed_advertiser_id, 0)                               feed_advertiser_id,
       coalesce(ad_event.affiliate_account_lid, 0)                            affiliate_account_lid,
       coalesce(ad_event.affiliate_account_hid, 0)                            affiliate_account_hid,
       sid,
       said,
       requests,
       ad_returns,
       raw_clicks,
       paid_clicks,
       pub_payout,
       revenue,
       dollars_worth,
       actions_worth,
       case
           when actions_worth > 0 then 1
           when actions_worth < 0 then -1
           else 0 end                                                         event_fires_count
from ad_event
where dt >= '2020-05-17'
  and event_date between '2020-05-17' and '2020-05-26';



DROP TABLE IF EXISTS addotnet.stats_adjustment_daily;
CREATE VIEW addotnet.stats_adjustment_daily
AS
select event_date,
       toYYYYMM(event_date)                                                   event_month_id,
       toString(formatDateTime(event_date, '%Y-%m'))                          event_month_name,
       toUInt16(formatDateTime(event_date, '%Y'))                             event_year,
       coalesce(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END) advertiser_lid,
       coalesce(CASE WHEN advertiser_hid = -1 THEN 0 ELSE advertiser_hid END) advertiser_hid,
       coalesce(stats_adjustment.campaign_id, 0)                              campaign_id,
       coalesce(stats_adjustment.adgroup_lid, 0)                              adgroup_lid,
       coalesce(stats_adjustment.adgroup_hid, 0)                              adgroup_hid,
       coalesce(stats_adjustment.provider_account_lid, 0)                     provider_account_lid,
       coalesce(stats_adjustment.provider_account_hid, 0)                     provider_account_hid,
       coalesce(stats_adjustment.feed_advertiser_id, 0)                       feed_advertiser_id,
       coalesce(stats_adjustment.affiliate_account_lid, 0)                    affiliate_account_lid,
       coalesce(stats_adjustment.affiliate_account_hid, 0)                    affiliate_account_hid,
       sid,
       said,
       0                                                                      requests,
       0                                                                      ad_returns,
       raw_clicks_diff,
       paid_clicks_diff,
       pub_payout_diff,
       revenue_diff,
       dollars_worth_diff,
       actions_worth_diff,
       event_fires_count_diff,
       adjust_mb_stats
from addotnet.stats_adjustment;


drop table IF EXISTS addotnet.ad_event_daily_adjustment;

CREATE VIEW addotnet.ad_event_daily_adjustment
AS
SELECT event_date,
       event_month_id,
       event_month_name,
       event_year,
       advertiser_lid,
       advertiser_hid,
       campaign_id,
       adgroup_lid,
       adgroup_hid,
       provider_account_lid,
       provider_account_hid,
       feed_advertiser_id,
       affiliate_account_lid,
       affiliate_account_hid,
       sid,
       ''                                                                                AS said,
       0                                                                                 AS requests,
       0                                                                                 AS ad_returns,
       0                                                                                 AS raw_clicks,
       sum(paid_clicks)                                                                  AS paid_clicks,
       mb_cost                                                                           AS pub_payout,
       0                                                                                 AS revenue,
       0                                                                                 AS dollars_worth,
       0                                                                                 AS actions_worth,
       0                                                                                 AS event_fires_count,
       dictGet('addotnet.adgroup_dim', 'adgroup_name', toUInt64(ABS(adgroup_lid)))       AS adgroup_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_name', toUInt64(
               ABS(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END)))       AS advertiser_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_status',
               toUInt64(ABS(advertiser_lid)))                                            AS advertiser_status,
       dictGet('addotnet.affiliate_account_dim', 'name',
               toUInt64(ABS(affiliate_account_lid)))                                     AS affiliate_account_name,
       dictGet('addotnet.affiliate_account_dim', 'is_media_buy',
               toUInt64(ABS(affiliate_account_lid)))                                     AS is_media_buy,
       dictGet('addotnet.campaign_dim', 'name', toUInt64(ABS(campaign_id)))              AS campaign_name,
       dictGet('addotnet.provider_feed_dim', 'feed_advertiser_name',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS feed_advertiser_name,
       dictGet('addotnet.provider_feed_dim', 'feed_advertiser_status',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS feed_advertiser_status,
       dictGet('addotnet.provider_feed_dim', 'provider_account_name',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS provider_account_name,
       dictGet('addotnet.provider_feed_dim', 'provider_account_status',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS provider_account_status,
       dictGet('addotnet.io_cap_dim', 'io_cap_id',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_cap_id,
       dictGet('addotnet.io_cap_dim', 'io_autostart',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_autostart,
       CASE
           WHEN dictGet('addotnet.io_cap_dim', 'io_start_date', tuple(event_date, COALESCE(advertiser_lid, 0))) =
                '0000-00-00'
               THEN cast(NULL as Nullable(Date))
           else dictGet('addotnet.io_cap_dim', 'io_start_date',
                        tuple(event_date, COALESCE(advertiser_lid, 0))) end              AS io_start_date,
       CASE
           WHEN dictGet('addotnet.io_cap_dim', 'io_end_date', tuple(event_date, COALESCE(advertiser_lid, 0))) =
                '0000-00-00'
               THEN cast(NULL as Nullable(Date))
           else dictGet('addotnet.io_cap_dim', 'io_end_date',
                        tuple(event_date, COALESCE(advertiser_lid, 0))) END              AS io_end_date,
       dictGet('addotnet.io_cap_dim', 'io_revenue_cap',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_revenue_cap,
       dictGet('addotnet.io_cap_dim', 'io_elapsed_day',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_elapsed_day,
       dictGet('addotnet.io_cap_dim', 'io_total_day',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_total_day,
       dictGet('addotnet.traffic_source_dim', 'lid', toUInt64(ABS(sid)))                 AS traffic_source_lid,
       dictGet('addotnet.traffic_source_dim', 'hid', toUInt64(ABS(sid)))                 AS traffic_source_hid,
       dictGet('addotnet.traffic_source_dim', 'name', toUInt64(ABS(sid)))                AS traffic_source_name,
       dictGet('addotnet.traffic_source_dim', 'quality_bucket_name',
               toUInt64(ABS(sid)))                                                       AS quality_bucket_name,
       CAST(splitByChar('=',
                        splitByChar(',', COALESCE(dictGet('addotnet.adgroup_revenue_cap', 'source_daily_clicks_caps',
                                                          tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0))),
                                                  ''))[1])[2] AS Nullable(Float64))      AS source_daily_clicks_caps,
       CAST(splitByChar('=',
                        splitByChar(',', COALESCE(dictGet('addotnet.adgroup_revenue_cap', 'source_daily_revenue_caps',
                                                          tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0))),
                                                  ''))[1])[2] AS Nullable(Float64))      AS source_daily_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_revenue_cap', 'source_daily_sid_clicks_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String))),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_clicks_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_revenue_cap', 'source_daily_sid_revenue_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String))),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_said_revenue_cap', 'source_daily_sid_said_clicks_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String), said)),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_said_clicks_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_said_revenue_cap', 'source_daily_sid_said_revenue_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String), said)),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_said_revenue_caps,
       0                                                                                 AS impressions,
       0                                                                                 AS paid_clicks_diff,
       0                                                                                 AS revenue_diff,
       0                                                                                 AS pub_payout_diff,
       0                                                                                 AS feed_generated_revenue_diff,
       0                                                                                 AS feed_generated_pub_payout_diff,
       IF(affiliate_account_name = 'Facebook',
          CAST(dictGet('addotnet.facebook_dim', 'mb_clicks',
                       tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                             COALESCE(sid, 0), COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                             COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0),
                             COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                             COALESCE(adgroup_hid, 0))) AS Nullable(UInt64)),
          CAST(dictGet('addotnet.media_buyer_adjustment', 'mb_clicks',
                       tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                             COALESCE(sid, 0), COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                             COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0),
                             COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                             COALESCE(adgroup_hid, 0))) AS Nullable(UInt64)))            AS mb_clicks,
       IF(affiliate_account_name = 'Facebook',
          CAST(dictGet('addotnet.facebook_dim', 'mb_cost',
                       tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                             COALESCE(sid, 0), COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                             COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0),
                             COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                             COALESCE(adgroup_hid, 0))) AS Nullable(Float64)),
          CAST(dictGet('addotnet.media_buyer_adjustment', 'mb_cost',
                       tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                             COALESCE(sid, 0), COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                             COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0),
                             COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                             COALESCE(adgroup_hid, 0))) AS Nullable(Float64)))           AS mb_cost,
       IF(affiliate_account_name = 'Facebook',
          CAST(dictGet('addotnet.facebook_dim', 'mb_dollars_worth',
                       tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                             COALESCE(sid, 0), COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                             COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0),
                             COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                             COALESCE(adgroup_hid, 0))) AS Nullable(Float64)),
          CAST(dictGet('addotnet.media_buyer_adjustment', 'mb_dollars_worth',
                       tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                             COALESCE(sid, 0), COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                             COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0),
                             COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                             COALESCE(adgroup_hid, 0))) AS Nullable(Float64)))           AS mb_dollars_worth,
       mb_cost                                                                              click_cost,
       CAST(mb_clicks as UInt64)                                                            purchased_clicks,
       CASE
           WHEN event_date = toDate(NOW()) then revenue + revenue_diff
           else 0 end                                                                       today_revenue,
       CASE
           WHEN event_date = toDate(yesterday()) then revenue + revenue_diff
           else 0 end                                                                       yesterday_revenue,
       CASE
           WHEN event_date between toDate(addDays(now(), -7)) and toDate(yesterday()) then revenue
           else 0 end                                                                       last_7_completed_days_revenue,
       dictGet('addotnet.advertiser_budget_dim', 'budget_amount',
               tuple(COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0), COALESCE(provider_account_lid, 0),
                     COALESCE(provider_account_hid, 0),
                     toInt32(toYear(event_date) * 100 + toMonth(event_date))))           AS advertiser_budget_amount,
       toInt32(toYear(event_date) * 100 + toMonth(event_date))                           AS month_id,
       dictGet('addotnet.publisher_category_dim', 'publisher_category',
               toUInt64(ABS(sid)))                                                       AS publisher_category,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.advertiser_dim', 'daily_revenue_caps', toUInt64(ABS(advertiser_lid))),
               ''))[1])[2] AS Nullable(Float64))                                         AS advertiser_daily_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.advertiser_dim', 'monthly_revenue_caps', toUInt64(ABS(advertiser_lid))),
               ''))[1])[2] AS Nullable(Float64))                                         AS advertiser_monthly_revenue_caps,
       dictGet('addotnet.adgroup_dim', 'cpa_goal',
               toUInt64(ABS(adgroup_lid)))                                               AS cpa_goal,
       CAST(splitByChar('=', splitByChar(',', COALESCE(dictGet('addotnet.adgroup_dim', 'daily_revenue_caps',
                                                               toUInt64(ABS(CASE WHEN adgroup_lid = -1 THEN 0 ELSE adgroup_lid END))),
                                                       ''))[1])[2] AS Nullable(Float64)) AS adgroup_daily_revenue_caps
FROM addotnet.ad_event_daily
WHERE is_media_buy = 1
  AND sid != 11172614
GROUP BY event_date,
         event_month_id,
         event_month_name,
         event_year,
         affiliate_account_lid,
         affiliate_account_hid,
         sid,
         provider_account_lid,
         provider_account_hid,
         feed_advertiser_id,
         advertiser_lid,
         advertiser_hid,
         campaign_id,
         adgroup_lid,
         adgroup_hid
UNION ALL
SELECT event_date,
       event_month_id,
       event_month_name,
       event_year,
       advertiser_lid,
       advertiser_hid,
       campaign_id,
       adgroup_lid,
       adgroup_hid,
       provider_account_lid,
       provider_account_hid,
       feed_advertiser_id,
       affiliate_account_lid,
       affiliate_account_hid,
       sid,
       said,
       0                                                                                 AS requests,
       0                                                                                 AS ad_returns,
       0                                                                                 AS raw_clicks,
       sum(paid_clicks)                                                                  AS paid_clicks,
       mb_cost                                                                           AS pub_payout,
       0                                                                                 AS revenue,
       0                                                                                 AS dollars_worth,
       0                                                                                 AS actions_worth,
       0                                                                                 AS event_fires_count,
       dictGet('addotnet.adgroup_dim', 'adgroup_name', toUInt64(ABS(adgroup_lid)))       AS adgroup_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_name', toUInt64(
               ABS(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END)))       AS advertiser_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_status',
               toUInt64(ABS(advertiser_lid)))                                            AS advertiser_status,
       dictGet('addotnet.affiliate_account_dim', 'name',
               toUInt64(ABS(affiliate_account_lid)))                                     AS affiliate_account_name,
       dictGet('addotnet.affiliate_account_dim', 'is_media_buy',
               toUInt64(ABS(affiliate_account_lid)))                                     AS is_media_buy,
       dictGet('addotnet.campaign_dim', 'name', toUInt64(ABS(campaign_id)))              AS campaign_name,
       dictGet('addotnet.provider_feed_dim', 'feed_advertiser_name',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS feed_advertiser_name,
       dictGet('addotnet.provider_feed_dim', 'feed_advertiser_status',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS feed_advertiser_status,
       dictGet('addotnet.provider_feed_dim', 'provider_account_name',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS provider_account_name,
       dictGet('addotnet.provider_feed_dim', 'provider_account_status',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS provider_account_status,
       dictGet('addotnet.io_cap_dim', 'io_cap_id',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_cap_id,
       dictGet('addotnet.io_cap_dim', 'io_autostart',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_autostart,
       CASE
           WHEN dictGet('addotnet.io_cap_dim', 'io_start_date', tuple(event_date, COALESCE(advertiser_lid, 0))) =
                '0000-00-00'
               THEN cast(NULL as Nullable(Date))
           else dictGet('addotnet.io_cap_dim', 'io_start_date',
                        tuple(event_date, COALESCE(advertiser_lid, 0))) end              AS io_start_date,
       CASE
           WHEN dictGet('addotnet.io_cap_dim', 'io_end_date', tuple(event_date, COALESCE(advertiser_lid, 0))) =
                '0000-00-00'
               THEN cast(NULL as Nullable(Date))
           else dictGet('addotnet.io_cap_dim', 'io_end_date',
                        tuple(event_date, COALESCE(advertiser_lid, 0))) END              AS io_end_date,
       dictGet('addotnet.io_cap_dim', 'io_revenue_cap',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_revenue_cap,
       dictGet('addotnet.io_cap_dim', 'io_elapsed_day',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_elapsed_day,
       dictGet('addotnet.io_cap_dim', 'io_total_day',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_total_day,
       dictGet('addotnet.traffic_source_dim', 'lid', toUInt64(ABS(sid)))                 AS traffic_source_lid,
       dictGet('addotnet.traffic_source_dim', 'hid', toUInt64(ABS(sid)))                 AS traffic_source_hid,
       dictGet('addotnet.traffic_source_dim', 'name', toUInt64(ABS(sid)))                AS traffic_source_name,
       dictGet('addotnet.traffic_source_dim', 'quality_bucket_name',
               toUInt64(ABS(sid)))                                                       AS quality_bucket_name,
       CAST(splitByChar('=',
                        splitByChar(',', COALESCE(dictGet('addotnet.adgroup_revenue_cap', 'source_daily_clicks_caps',
                                                          tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0))),
                                                  ''))[1])[2] AS Nullable(Float64))      AS source_daily_clicks_caps,
       CAST(splitByChar('=',
                        splitByChar(',', COALESCE(dictGet('addotnet.adgroup_revenue_cap', 'source_daily_revenue_caps',
                                                          tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0))),
                                                  ''))[1])[2] AS Nullable(Float64))      AS source_daily_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_revenue_cap', 'source_daily_sid_clicks_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String))),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_clicks_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_revenue_cap', 'source_daily_sid_revenue_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String))),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_said_revenue_cap', 'source_daily_sid_said_clicks_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String), said)),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_said_clicks_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_said_revenue_cap', 'source_daily_sid_said_revenue_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String), said)),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_said_revenue_caps,
       0                                                                                 AS impressions,
       0                                                                                 AS paid_clicks_diff,
       0                                                                                 AS revenue_diff,
       0                                                                                 AS pub_payout_diff,
       0                                                                                 AS feed_generated_revenue_diff,
       0                                                                                 AS feed_generated_pub_payout_diff,
       CAST(dictGet('addotnet.bidwise_adjustment', 'mb_clicks',
                    tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                          COALESCE(sid, 0), COALESCE(said, ''), COALESCE(provider_account_lid, 0),
                          COALESCE(provider_account_hid, 0),
                          COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0),
                          COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                          COALESCE(adgroup_hid, 0))) AS Nullable(UInt64))                AS mb_clicks,
       CAST(dictGet('addotnet.bidwise_adjustment', 'mb_cost',
                    tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                          COALESCE(sid, 0), COALESCE(said, ''), COALESCE(provider_account_lid, 0),
                          COALESCE(provider_account_hid, 0),
                          COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0),
                          COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                          COALESCE(adgroup_hid, 0))) AS Nullable(Float64))               AS mb_cost,
       CAST(dictGet('addotnet.bidwise_adjustment', 'mb_dollars_worth',
                    tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                          COALESCE(sid, 0), COALESCE(said, ''), COALESCE(provider_account_lid, 0),
                          COALESCE(provider_account_hid, 0),
                          COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0),
                          COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                          COALESCE(adgroup_hid, 0))) AS Nullable(Float64))               AS mb_dollars_worth,
       mb_cost                                                                              click_cost,
       CAST(mb_clicks as UInt64)                                                            purchased_clicks,
       CASE
           WHEN event_date = toDate(NOW()) then revenue + revenue_diff
           else 0 end                                                                       today_revenue,
       CASE
           WHEN event_date = toDate(yesterday()) then revenue + revenue_diff
           else 0 end                                                                       yesterday_revenue,
       CASE
           WHEN event_date between toDate(addDays(now(), -7)) and toDate(yesterday()) then revenue
           else 0 end                                                                       last_7_completed_days_revenue,
       dictGet('addotnet.advertiser_budget_dim', 'budget_amount',
               tuple(COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0), COALESCE(provider_account_lid, 0),
                     COALESCE(provider_account_hid, 0),
                     toInt32(toYear(event_date) * 100 + toMonth(event_date))))           AS advertiser_budget_amount,
       toInt32(toYear(event_date) * 100 + toMonth(event_date))                           AS month_id,
       dictGet('addotnet.publisher_category_dim', 'publisher_category',
               toUInt64(ABS(sid)))                                                       AS publisher_category,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.advertiser_dim', 'daily_revenue_caps', toUInt64(ABS(advertiser_lid))),
               ''))[1])[2] AS Nullable(Float64))                                         AS advertiser_daily_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.advertiser_dim', 'monthly_revenue_caps', toUInt64(ABS(advertiser_lid))),
               ''))[1])[2] AS Nullable(Float64))                                         AS advertiser_monthly_revenue_caps,
       dictGet('addotnet.adgroup_dim', 'cpa_goal',
               toUInt64(ABS(adgroup_lid)))                                               AS cpa_goal,
       CAST(splitByChar('=', splitByChar(',', COALESCE(dictGet('addotnet.adgroup_dim', 'daily_revenue_caps',
                                                               toUInt64(ABS(CASE WHEN adgroup_lid = -1 THEN 0 ELSE adgroup_lid END))),
                                                       ''))[1])[2] AS Nullable(Float64)) AS adgroup_daily_revenue_caps
FROM addotnet.ad_event_daily
WHERE is_media_buy = 1
  AND sid = 11172614
GROUP BY event_date,
         event_month_id,
         event_month_name,
         event_year,
         affiliate_account_lid,
         affiliate_account_hid,
         sid,
         said,
         provider_account_lid,
         provider_account_hid,
         feed_advertiser_id,
         advertiser_lid,
         advertiser_hid,
         campaign_id,
         adgroup_lid,
         adgroup_hid
UNION ALL
SELECT event_date,
       event_month_id,
       event_month_name,
       event_year,
       advertiser_lid,
       advertiser_hid,
       campaign_id,
       adgroup_lid,
       adgroup_hid,
       provider_account_lid,
       provider_account_hid,
       feed_advertiser_id,
       affiliate_account_lid,
       affiliate_account_hid,
       sid,
       said,
       requests                                                                          AS requests,
       ad_returns                                                                        AS ad_returns,
       raw_clicks                                                                        AS raw_clicks,
       CASE WHEN is_media_buy = 0 THEN paid_clicks else 0 end                            AS paid_clicks,
       CASE WHEN is_media_buy = 0 THEN pub_payout else 0 end                             AS pub_payout,
       revenue                                                                           AS revenue,
       dollars_worth                                                                     AS dollars_worth,
       actions_worth                                                                     AS actions_worth,
       event_fires_count                                                                 AS event_fires_count,
       dictGet('addotnet.adgroup_dim', 'adgroup_name', toUInt64(ABS(adgroup_lid)))       AS adgroup_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_name', toUInt64(
               ABS(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END)))       AS advertiser_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_status',
               toUInt64(ABS(advertiser_lid)))                                            AS advertiser_status,
       dictGet('addotnet.affiliate_account_dim', 'name',
               toUInt64(ABS(affiliate_account_lid)))                                     AS affiliate_account_name,
       dictGet('addotnet.affiliate_account_dim', 'is_media_buy',
               toUInt64(ABS(affiliate_account_lid)))                                     AS is_media_buy,
       dictGet('addotnet.campaign_dim', 'name', toUInt64(ABS(campaign_id)))              AS campaign_name,
       dictGet('addotnet.provider_feed_dim', 'feed_advertiser_name',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS feed_advertiser_name,
       dictGet('addotnet.provider_feed_dim', 'feed_advertiser_status',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS feed_advertiser_status,
       dictGet('addotnet.provider_feed_dim', 'provider_account_name',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS provider_account_name,
       dictGet('addotnet.provider_feed_dim', 'provider_account_status',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS provider_account_status,
       dictGet('addotnet.io_cap_dim', 'io_cap_id',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_cap_id,
       dictGet('addotnet.io_cap_dim', 'io_autostart',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_autostart,
       CASE
           WHEN dictGet('addotnet.io_cap_dim', 'io_start_date', tuple(event_date, COALESCE(advertiser_lid, 0))) =
                '0000-00-00'
               THEN cast(NULL as Nullable(Date))
           else dictGet('addotnet.io_cap_dim', 'io_start_date',
                        tuple(event_date, COALESCE(advertiser_lid, 0))) end              AS io_start_date,
       CASE
           WHEN dictGet('addotnet.io_cap_dim', 'io_end_date', tuple(event_date, COALESCE(advertiser_lid, 0))) =
                '0000-00-00'
               THEN cast(NULL as Nullable(Date))
           else dictGet('addotnet.io_cap_dim', 'io_end_date',
                        tuple(event_date, COALESCE(advertiser_lid, 0))) END              AS io_end_date,
       dictGet('addotnet.io_cap_dim', 'io_revenue_cap',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_revenue_cap,
       dictGet('addotnet.io_cap_dim', 'io_elapsed_day',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_elapsed_day,
       dictGet('addotnet.io_cap_dim', 'io_total_day',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_total_day,
       dictGet('addotnet.traffic_source_dim', 'lid', toUInt64(ABS(sid)))                 AS traffic_source_lid,
       dictGet('addotnet.traffic_source_dim', 'hid', toUInt64(ABS(sid)))                 AS traffic_source_hid,
       dictGet('addotnet.traffic_source_dim', 'name', toUInt64(ABS(sid)))                AS traffic_source_name,
       dictGet('addotnet.traffic_source_dim', 'quality_bucket_name',
               toUInt64(ABS(sid)))                                                       AS quality_bucket_name,
       CAST(splitByChar('=',
                        splitByChar(',', COALESCE(dictGet('addotnet.adgroup_revenue_cap', 'source_daily_clicks_caps',
                                                          tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0))),
                                                  ''))[1])[2] AS Nullable(Float64))      AS source_daily_clicks_caps,
       CAST(splitByChar('=',
                        splitByChar(',', COALESCE(dictGet('addotnet.adgroup_revenue_cap', 'source_daily_revenue_caps',
                                                          tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0))),
                                                  ''))[1])[2] AS Nullable(Float64))      AS source_daily_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_revenue_cap', 'source_daily_sid_clicks_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String))),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_clicks_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_revenue_cap', 'source_daily_sid_revenue_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String))),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_said_revenue_cap', 'source_daily_sid_said_clicks_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String), said)),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_said_clicks_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_said_revenue_cap', 'source_daily_sid_said_revenue_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String), said)),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_said_revenue_caps,
       0                                                                                 AS impressions,
       dictGet('addotnet.feed_advertiser_adjustment', 'paid_clicks_diff',
               tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                     COALESCE(sid, 0), COALESCE(said, ''), COALESCE(provider_account_lid, 0),
                     COALESCE(provider_account_hid, 0), COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0),
                     COALESCE(advertiser_hid, 0), COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                     COALESCE(adgroup_hid, 0)))                                          AS paid_clicks_diff,
       dictGet('addotnet.feed_advertiser_adjustment', 'revenue_diff',
               tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                     COALESCE(sid, 0), COALESCE(said, ''), COALESCE(provider_account_lid, 0),
                     COALESCE(provider_account_hid, 0), COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0),
                     COALESCE(advertiser_hid, 0), COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                     COALESCE(adgroup_hid, 0)))                                          AS revenue_diff,
       dictGet('addotnet.feed_advertiser_adjustment', 'pub_payout_diff',
               tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                     COALESCE(sid, 0), COALESCE(said, ''), COALESCE(provider_account_lid, 0),
                     COALESCE(provider_account_hid, 0), COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0),
                     COALESCE(advertiser_hid, 0), COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                     COALESCE(adgroup_hid, 0)))                                          AS pub_payout_diff,
       dictGet('addotnet.feed_advertiser_adjustment', 'feed_generated_revenue_diff',
               tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                     COALESCE(sid, 0), COALESCE(said, ''), COALESCE(provider_account_lid, 0),
                     COALESCE(provider_account_hid, 0), COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0),
                     COALESCE(advertiser_hid, 0), COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                     COALESCE(adgroup_hid, 0)))                                          AS feed_generated_revenue_diff,
       dictGet('addotnet.feed_advertiser_adjustment', 'feed_generated_pub_payout_diff',
               tuple(event_date, COALESCE(affiliate_account_lid, 0), COALESCE(affiliate_account_hid, 0),
                     COALESCE(sid, 0), COALESCE(said, ''), COALESCE(provider_account_lid, 0),
                     COALESCE(provider_account_hid, 0), COALESCE(feed_advertiser_id, 0), COALESCE(advertiser_lid, 0),
                     COALESCE(advertiser_hid, 0), COALESCE(campaign_id, 0), COALESCE(adgroup_lid, 0),
                     COALESCE(adgroup_hid, 0)))                                          AS feed_generated_pub_payout_diff,
       0                                                                                 AS mb_clicks,
       0                                                                                 AS mb_cost,
       0                                                                                 AS mb_dollars_worth,
       CASE WHEN is_media_buy = 0 THEN pub_payout + pub_payout_diff else 0 end              click_cost,
       CASE WHEN is_media_buy = 0 THEN ad_returns else 0 end                                purchased_clicks,
       CASE
           WHEN event_date = toDate(NOW()) then revenue + revenue_diff
           else 0 end                                                                       today_revenue,
       CASE
           WHEN event_date = toDate(yesterday()) then revenue + revenue_diff
           else 0 end                                                                       yesterday_revenue,
       CASE
           WHEN event_date between toDate(addDays(now(), -7)) and toDate(yesterday()) then revenue
           else 0 end                                                                       last_7_completed_days_revenue,
       dictGet('addotnet.advertiser_budget_dim', 'budget_amount',
               tuple(COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0), COALESCE(provider_account_lid, 0),
                     COALESCE(provider_account_hid, 0),
                     toInt32(toYear(event_date) * 100 + toMonth(event_date))))           AS advertiser_budget_amount,
       toInt32(toYear(event_date) * 100 + toMonth(event_date))                           AS month_id,
       dictGet('addotnet.publisher_category_dim', 'publisher_category',
               toUInt64(ABS(sid)))                                                       AS publisher_category,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.advertiser_dim', 'daily_revenue_caps', toUInt64(ABS(advertiser_lid))),
               ''))[1])[2] AS Nullable(Float64))                                         AS advertiser_daily_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.advertiser_dim', 'monthly_revenue_caps', toUInt64(ABS(advertiser_lid))),
               ''))[1])[2] AS Nullable(Float64))                                         AS advertiser_monthly_revenue_caps,
       dictGet('addotnet.adgroup_dim', 'cpa_goal',
               toUInt64(ABS(adgroup_lid)))                                               AS cpa_goal,
       CAST(splitByChar('=', splitByChar(',', COALESCE(dictGet('addotnet.adgroup_dim', 'daily_revenue_caps',
                                                               toUInt64(ABS(CASE WHEN adgroup_lid = -1 THEN 0 ELSE adgroup_lid END))),
                                                       ''))[1])[2] AS Nullable(Float64)) AS adgroup_daily_revenue_caps
FROM addotnet.ad_event_daily
UNION ALL
SELECT event_date,
       event_month_id,
       event_month_name,
       event_year,
       advertiser_lid,
       advertiser_hid,
       campaign_id,
       adgroup_lid,
       adgroup_hid,
       provider_account_lid,
       provider_account_hid,
       feed_advertiser_id,
       affiliate_account_lid,
       affiliate_account_hid,
       sid,
       said,
       0                                                                                 AS requests,
       0                                                                                    ad_returns,
       0                                                                                    raw_clicks,
       0                                                                                    paid_clicks,
       0                                                                                    pub_payout,
       0                                                                                    revenue,
       0                                                                                    dollars_worth,
       0                                                                                    actions_worth,
       0                                                                                    event_fires_count,
       dictGet('addotnet.adgroup_dim', 'adgroup_name', toUInt64(ABS(adgroup_lid)))       AS adgroup_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_name', toUInt64(
               ABS(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END)))       AS advertiser_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_status',
               toUInt64(ABS(advertiser_lid)))                                            AS advertiser_status,
       dictGet('addotnet.affiliate_account_dim', 'name',
               toUInt64(ABS(affiliate_account_lid)))                                     AS affiliate_account_name,
       dictGet('addotnet.affiliate_account_dim', 'is_media_buy',
               toUInt64(ABS(affiliate_account_lid)))                                     AS is_media_buy,
       dictGet('addotnet.campaign_dim', 'name', toUInt64(ABS(campaign_id)))              AS campaign_name,
       dictGet('addotnet.provider_feed_dim', 'feed_advertiser_name',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS feed_advertiser_name,
       dictGet('addotnet.provider_feed_dim', 'feed_advertiser_status',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS feed_advertiser_status,
       dictGet('addotnet.provider_feed_dim', 'provider_account_name',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS provider_account_name,
       dictGet('addotnet.provider_feed_dim', 'provider_account_status',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0),
                     COALESCE(feed_advertiser_id, 0)))                                   AS provider_account_status,
       dictGet('addotnet.io_cap_dim', 'io_cap_id',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_cap_id,
       dictGet('addotnet.io_cap_dim', 'io_autostart',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_autostart,
       CASE
           WHEN dictGet('addotnet.io_cap_dim', 'io_start_date', tuple(event_date, COALESCE(advertiser_lid, 0))) =
                '0000-00-00'
               THEN cast(NULL as Nullable(Date))
           else dictGet('addotnet.io_cap_dim', 'io_start_date',
                        tuple(event_date, COALESCE(advertiser_lid, 0))) end              AS io_start_date,
       CASE
           WHEN dictGet('addotnet.io_cap_dim', 'io_end_date', tuple(event_date, COALESCE(advertiser_lid, 0))) =
                '0000-00-00'
               THEN cast(NULL as Nullable(Date))
           else dictGet('addotnet.io_cap_dim', 'io_end_date',
                        tuple(event_date, COALESCE(advertiser_lid, 0))) END              AS io_end_date,
       dictGet('addotnet.io_cap_dim', 'io_revenue_cap',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_revenue_cap,
       dictGet('addotnet.io_cap_dim', 'io_elapsed_day',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_elapsed_day,
       dictGet('addotnet.io_cap_dim', 'io_total_day',
               tuple(event_date, COALESCE(advertiser_lid, 0)))                           AS io_total_day,
       dictGet('addotnet.traffic_source_dim', 'lid', toUInt64(ABS(sid)))                 AS traffic_source_lid,
       dictGet('addotnet.traffic_source_dim', 'hid', toUInt64(ABS(sid)))                 AS traffic_source_hid,
       dictGet('addotnet.traffic_source_dim', 'name', toUInt64(ABS(sid)))                AS traffic_source_name,
       dictGet('addotnet.traffic_source_dim', 'quality_bucket_name',
               toUInt64(ABS(sid)))                                                       AS quality_bucket_name,
       CAST(splitByChar('=',
                        splitByChar(',', COALESCE(dictGet('addotnet.adgroup_revenue_cap', 'source_daily_clicks_caps',
                                                          tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0))),
                                                  ''))[1])[2] AS Nullable(Float64))      AS source_daily_clicks_caps,
       CAST(splitByChar('=',
                        splitByChar(',', COALESCE(dictGet('addotnet.adgroup_revenue_cap', 'source_daily_revenue_caps',
                                                          tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0))),
                                                  ''))[1])[2] AS Nullable(Float64))      AS source_daily_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_revenue_cap', 'source_daily_sid_clicks_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String))),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_clicks_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_revenue_cap', 'source_daily_sid_revenue_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String))),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_said_revenue_cap', 'source_daily_sid_said_clicks_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String), said)),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_said_clicks_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.adgroup_sid_said_revenue_cap', 'source_daily_sid_said_revenue_caps',
                       tuple(COALESCE(adgroup_lid, 0), COALESCE(adgroup_hid, 0), CAST(sid AS String), said)),
               ''))[1])[2] AS Nullable(Float64))                                         AS source_daily_sid_said_revenue_caps,
       0                                                                                    impressions,
       CASE
           WHEN is_media_buy = 0 or adjust_mb_stats = 1 THEN paid_clicks_diff
           ELSE 0 END                                                                       paid_clicks_diff,
       revenue_diff,
       CASE
           WHEN is_media_buy = 0 or adjust_mb_stats = 1 THEN pub_payout_diff
           ELSE 0 END                                                                       pub_payout_diff,
       0                                                                                    feed_generated_revenue_diff,
       0                                                                                    feed_generated_pub_payout_diff,
       0                                                                                 AS mb_clicks,
       0                                                                                 AS mb_cost,
       0                                                                                 AS mb_dollars_worth,
       CASE WHEN is_media_buy = 0 THEN pub_payout + pub_payout_diff else 0 end              click_cost,
       CASE WHEN is_media_buy = 0 THEN ad_returns else 0 end                                purchased_clicks,
       CASE
           WHEN event_date = toDate(NOW()) then revenue + revenue_diff
           else 0 end                                                                       today_revenue,
       CASE
           WHEN event_date = toDate(yesterday()) then revenue + revenue_diff
           else 0 end                                                                       yesterday_revenue,
       CASE
           WHEN event_date between toDate(addDays(now(), -7)) and toDate(yesterday()) then revenue
           else 0 end                                                                       last_7_completed_days_revenue,
       dictGet('addotnet.advertiser_budget_dim', 'budget_amount',
               tuple(COALESCE(advertiser_lid, 0), COALESCE(advertiser_hid, 0), COALESCE(provider_account_lid, 0),
                     COALESCE(provider_account_hid, 0),
                     toInt32(toYear(event_date) * 100 + toMonth(event_date))))           AS advertiser_budget_amount,
       toInt32(toYear(event_date) * 100 + toMonth(event_date))                           AS month_id,
       dictGet('addotnet.publisher_category_dim', 'publisher_category',
               toUInt64(ABS(sid)))                                                       AS publisher_category,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.advertiser_dim', 'daily_revenue_caps', toUInt64(ABS(advertiser_lid))),
               ''))[1])[2] AS Nullable(Float64))                                         AS advertiser_daily_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.advertiser_dim', 'monthly_revenue_caps', toUInt64(ABS(advertiser_lid))),
               ''))[1])[2] AS Nullable(Float64))                                         AS advertiser_monthly_revenue_caps,
       dictGet('addotnet.adgroup_dim', 'cpa_goal',
               toUInt64(ABS(adgroup_lid)))                                               AS cpa_goal,
       CAST(splitByChar('=', splitByChar(',', COALESCE(dictGet('addotnet.adgroup_dim', 'daily_revenue_caps',
                                                               toUInt64(ABS(CASE WHEN adgroup_lid = -1 THEN 0 ELSE adgroup_lid END))),
                                                       ''))[1])[2] AS Nullable(Float64)) AS adgroup_daily_revenue_caps
FROM addotnet.stats_adjustment_daily
UNION ALL
SELECT event_date,
       toYYYYMM(event_date)                                                              AS event_month_id,
       toString(formatDateTime(event_date, '%Y-%m'))                                     AS event_month_name,
       toYear(event_date),
       coalesce(advertiser_lid, 0)                                                       as advertiser_lid,
       coalesce(advertiser_hid, 0)                                                       as advertiser_hid,
       coalesce(campaign_id, 0)                                                          as campaign_id,
       coalesce(adgroup_lid, 0)                                                          as adgroup_lid,
       coalesce(adgroup_hid, 0)                                                          as adgroup_hid,
       CAST(0 AS Int64)                                                                  AS provider_account_lid,
       CAST(0 AS Int64)                                                                  AS provider_account_hid,
       CAST(0 AS Int64)                                                                  AS feed_advertiser_id,
       CAST(0 AS Int64)                                                                  AS affiliate_account_lid,
       CAST(0 AS Int64)                                                                  AS affiliate_account_hid,
       CAST(0 AS Int64)                                                                  AS sid,
       ''                                                                                AS said,
       0                                                                                 AS requests,
       0                                                                                 AS ad_returns,
       0                                                                                 AS raw_clicks,
       0                                                                                 AS paid_clicks,
       0                                                                                 AS pub_payout,
       0                                                                                 AS revenue,
       0                                                                                 AS dollars_worth,
       0                                                                                 AS actions_worth,
       0                                                                                 AS event_fires_count,
       dictGet('addotnet.adgroup_dim', 'adgroup_name', toUInt64(ABS(adgroup_lid)))       AS adgroup_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_name', toUInt64(
               ABS(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END)))       AS advertiser_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_status',
               toUInt64(ABS(advertiser_lid)))                                            AS advertiser_status,
       ''                                                                                AS affiliate_account_name,
       CAST(0 AS UInt8)                                                                  AS is_media_buy,
       dictGet('addotnet.campaign_dim', 'name', toUInt64(ABS(campaign_id)))              AS campaign_name,
       ''                                                                                AS feed_advertiser_name,
       ''                                                                                AS feed_advertiser_status,
       ''                                                                                AS provider_account_name,
       ''                                                                                AS provider_account_status,
       CAST(0 as Int64)                                                                  AS io_cap_id,
       CAST(0 as UInt8)                                                                  AS io_autostart,
       CAST(null AS Nullable(Date))                                                      AS io_start_date,
       CAST(null AS Nullable(Date))                                                      AS io_end_date,
       CAST(0 AS Float64)                                                                AS io_revenue_cap,
       CAST(0 AS Int32)                                                                  AS io_elapsed_day,
       CAST(0 AS Int32)                                                                  AS io_total_day,
       CAST(0 AS Int64)                                                                  AS traffic_source_lid,
       CAST(0 AS Int64)                                                                  AS traffic_source_hid,
       ''                                                                                AS traffic_source_name,
       ''                                                                                AS quality_bucket_name,
       CAST(null AS Nullable(Float64))                                                   AS source_daily_clicks_caps,
       CAST(null AS Nullable(Float64))                                                   AS source_daily_revenue_caps,
       CAST(null AS Nullable(Float64))                                                   AS source_daily_sid_clicks_caps,
       CAST(null AS Nullable(Float64))                                                   AS source_daily_sid_revenue_caps,
       CAST(null AS Nullable(Float64))                                                   AS source_daily_sid_said_clicks_caps,
       CAST(null AS Nullable(Float64))                                                   AS source_daily_sid_said_revenue_caps,
       impressions,
       CAST(0 AS Int64)                                                                  AS paid_clicks_diff,
       CAST(0 AS Float64)                                                                AS revenue_diff,
       CAST(0 AS Float64)                                                                AS pub_payout_diff,
       CAST(0 AS Float64)                                                                AS feed_generated_revenue_diff,
       CAST(0 AS Float64)                                                                AS feed_generated_pub_payout_diff,
       CAST(0 AS Nullable(UInt64))                                                       AS mb_clicks,
       CAST(0 AS Nullable(Float64))                                                      AS mb_cost,
       CAST(0 AS Nullable(Float64))                                                      AS mb_dollars_worth,
       CAST(0 AS Nullable(Float64))                                                      AS click_cost,
       CAST(0 AS Nullable(UInt64))                                                       AS purchased_clicks,
       CAST(0 AS Nullable(Float64))                                                      AS today_revenue,
       CAST(0 AS Nullable(Float64))                                                      AS yesterday_revenue,
       CAST(0 AS Nullable(Float64))                                                      AS last_7_completed_days_revenue,
       CAST(0 AS Float64)                                                                AS advertiser_budget_amount,
       toInt32(toYear(event_date) * 100 + toMonth(event_date))                           AS month_id,
       ''                                                                                AS publisher_category,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.advertiser_dim', 'daily_revenue_caps', toUInt64(ABS(advertiser_lid))),
               ''))[1])[2] AS Nullable(Float64))                                         AS advertiser_daily_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(
               dictGet('addotnet.advertiser_dim', 'monthly_revenue_caps', toUInt64(ABS(advertiser_lid))),
               ''))[1])[2] AS Nullable(Float64))                                         AS advertiser_monthly_revenue_caps,
       dictGet('addotnet.adgroup_dim', 'cpa_goal',
               toUInt64(ABS(adgroup_lid)))                                               AS cpa_goal,
       CAST(splitByChar('=', splitByChar(',', COALESCE(dictGet('addotnet.adgroup_dim', 'daily_revenue_caps',
                                                               toUInt64(ABS(CASE WHEN adgroup_lid = -1 THEN 0 ELSE adgroup_lid END))),
                                                       ''))[1])[2] AS Nullable(Float64)) AS adgroup_daily_revenue_caps
FROM addotnet.search_impression
;
-- this is needed since it would introduce 0 rows for other reporting
DROP TABLE IF EXISTS addotnet.ad_event_daily_adjustment_budget;
CREATE VIEW addotnet.ad_event_daily_adjustment_budget
as
SELECT *
FROM addotnet.ad_event_daily_adjustment
UNION ALL
select CAST(toDate(concat(substring(toString(month_id), 1, 4), '-', substring(toString(month_id), 5, 2),
                          '-01')) AS DATE)                                                     event_date,
       month_id                                                                                event_month_id,
       concat(substring(toString(month_id), 1, 4), '-', substring(toString(month_id), 5, 2))   event_month_name,
       CAST(substring(toString(month_id), 1, 4) as UInt16)                                     event_year,
       advertiser_budget.advertiser_lid,
       advertiser_budget.advertiser_hid,
       CAST(0 AS Int64)                                                                     AS campaign_id,
       CAST(0 AS Int64)                                                                     AS adgroup_lid,
       CAST(0 AS Int64)                                                                     AS adgroup_hid,
       advertiser_budget.provider_account_lid,
       advertiser_budget.provider_account_hid,
       CAST(0 AS Int64)                                                                     AS feed_advertiser_id,
       CAST(0 AS Int64)                                                                        affiliate_account_lid,
       CAST(0 AS Int64)                                                                        affiliate_account_hid,
       CAST(0 AS Int64)                                                                        sid,
       ''                                                                                   AS said,
       CAST(null AS Nullable(UInt64))                                                          requests,
       CAST(null AS Nullable(UInt64))                                                          ad_returns,
       CAST(null AS Nullable(UInt64))                                                          raw_clicks,
       CAST(null AS Nullable(UInt64))                                                          paid_clicks,
       CAST(null AS Nullable(Float64))                                                         pub_payout,
       CAST(null AS Nullable(Float64))                                                         revenue,
       CAST(null AS Nullable(Float64))                                                         dollars_worth,
       CAST(null AS Nullable(Float64))                                                         actions_worth,
       CAST(null AS Nullable(Int64))                                                           event_fires_count,
       ''                                                                                   AS adgroup_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_name',
               toUInt64(ABS(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END))) AS advertiser_name,
       ''                                                                                   AS advertiser_status,
       ''                                                                                   AS affiliate_account_name,
       CAST(0 as UInt8)                                                                     AS is_media_buy,
       ''                                                                                   AS campaign_name,
       ''                                                                                   AS feed_advertiser_name,
       ''                                                                                   AS feed_advertiser_status,
       dictGet('addotnet.provider_dim', 'provider_account_name',
               tuple(COALESCE(provider_account_lid, 0), COALESCE(provider_account_hid, 0))) AS provider_account_name,
       ''                                                                                   AS provider_account_status,
       CAST(0 as Int64)                                                                        io_cap_id,
       CAST(0 as UInt8)                                                                        io_autostart,
       CAST(null AS Nullable(Date))                                                            io_start_date,
       CAST(null AS Nullable(Date))                                                            io_end_date,
       CAST(0 AS Float64)                                                                      io_revenue_cap,
       CAST(0 AS Int32)                                                                     AS io_elapsed_day,
       CAST(0 AS Int32)                                                                     AS io_total_day,
       CAST(0 AS Int64)                                                                     AS traffic_source_lid,
       CAST(0 AS Int64)                                                                     AS traffic_source_hid,
       ''                                                                                   AS traffic_source_name,
       ''                                                                                   AS quality_bucket_name,
       CAST(null AS Nullable(Float64))                                                      AS source_daily_clicks_caps,
       CAST(null AS Nullable(Float64))                                                      AS source_daily_revenue_caps,
       CAST(null AS Nullable(Float64))                                                      AS source_daily_sid_clicks_caps,
       CAST(null AS Nullable(Float64))                                                      AS source_daily_sid_revenue_caps,
       CAST(null AS Nullable(Float64))                                                      AS source_daily_sid_said_clicks_caps,
       CAST(null AS Nullable(Float64))                                                      AS source_daily_sid_said_revenue_caps,
       CAST(0 AS Int32)                                                                     AS impressions,
       CAST(0 AS Int64)                                                                     AS paid_clicks_diff,
       CAST(0 AS Float64)                                                                      revenue_diff,
       CAST(0 AS Float64)                                                                      pub_payout_diff,
       CAST(0 AS Float64)                                                                      feed_generated_revenue_diff,
       CAST(0 AS Float64)                                                                      feed_generated_pub_payout_diff,
       CAST(0 AS Nullable(UInt64))                                                          AS mb_clicks,
       CAST(0 AS Nullable(Float64))                                                         AS mb_cost,
       CAST(0 AS Nullable(Float64))                                                         AS mb_dollars_worth,
       CAST(0 AS Nullable(Float64))                                                            click_cost,
       CAST(0 AS Nullable(UInt64))                                                             purchased_clicks,
       CAST(0 AS Nullable(Float64))                                                            today_revenue,
       CAST(0 AS Nullable(Float64))                                                            yesterday_revenue,
       CAST(0 AS Nullable(Float64))                                                            last_7_completed_days_revenue,
       coalesce(budget_amount, 0)                                                           AS advertiser_budget_amount,
       month_id                                                                             AS month_id,
       ''                                                                                   AS publisher_category,
       CAST(splitByChar('=', splitByChar(',', COALESCE(dictGet('addotnet.advertiser_dim', 'daily_revenue_caps',
                                                               toUInt64(
                                                                       ABS(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END))),
                                                       ''))[1])[2] AS Nullable(Float64))    AS advertiser_daily_revenue_caps,
       CAST(splitByChar('=', splitByChar(',', COALESCE(dictGet('addotnet.advertiser_dim', 'monthly_revenue_caps',
                                                               toUInt64(
                                                                       ABS(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END))),
                                                       ''))[1])[2] AS Nullable(Float64))    AS advertiser_monthly_revenue_caps,
       dictGet('addotnet.adgroup_dim', 'cpa_goal',
               toUInt64(ABS(adgroup_lid)))                                                  AS cpa_goal,
       CAST(splitByChar('=', splitByChar(',', COALESCE(dictGet('addotnet.adgroup_dim', 'daily_revenue_caps',
                                                               toUInt64(ABS(CASE WHEN adgroup_lid = -1 THEN 0 ELSE adgroup_lid END))),
                                                       ''))[1])[2] AS Nullable(Float64))    AS adgroup_daily_revenue_caps
FROM addotnet.advertiser_budget
WHERE ((advertiser_lid != 0) OR (provider_account_lid != 0));



INSERT INTO ad_event_daily
SELECT dt                                                                     event_date,
       toYYYYMM(event_date)                                                   event_month_id,
       toString(formatDateTime(event_date, '%Y-%m'))                          event_month_name,
       toUInt16(formatDateTime(event_date, '%Y'))                             event_year,
       coalesce(CASE WHEN advertiser_lid = -1 THEN 0 ELSE advertiser_lid END) advertiser_lid,
       coalesce(CASE WHEN advertiser_hid = -1 THEN 0 ELSE advertiser_hid END) advertiser_hid,
       coalesce(campaign_id, 0)                                               campaign_id,
       coalesce(adgroup_lid, 0)                                               adgroup_lid,
       coalesce(adgroup_hid, 0)                                               adgroup_hid,
       coalesce(provideraccount_lid, 0)                                       provider_account_lid,
       coalesce(provideraccount_hid, 0)                                       provider_account_hid,
       coalesce(feed_id, 0)                                                   feed_advertiser_id,
       coalesce(affiliateaccount_lid, 0)                                      affiliate_account_lid,
       coalesce(affiliateaccount_hid, 0)                                      affiliate_account_hid,
       sid,
       ''                                                                     said,
       0                                                                      requests,
       0                                                                      ad_returns,
       0                                                                      raw_clicks,
       0                                                                      paid_clicks,
       0                                                                      pub_payout,
       0                                                                      revenue,
       0                                                                      dollars_worth,
       0                                                                      actions_worth,
       0                                                                      event_fires_count
FROM media_buyer_adjustment
WHERE (dt >= '2020-05-01')
  AND ((mb_cost > 0) OR (mb_clicks > 0))
  AND ((dt, affiliateaccount_lid, sid, provideraccount_lid, feed_id, advertiser_lid, campaign_id, adgroup_lid) NOT IN
       (
           SELECT event_date,
                  affiliate_account_lid,
                  sid,
                  provider_account_lid,
                  feed_advertiser_id,
                  advertiser_lid,
                  campaign_id,
                  adgroup_lid
           FROM ad_event_daily
           WHERE dictGet('addotnet.affiliate_account_dim', 'is_media_buy', toUInt64(ABS(affiliate_account_lid))) = 1
             and event_date >= '2020-05-01'
       ));


DROP TABLE IF EXISTS addotnet.io_caps_and_budget_view;
CREATE VIEW addotnet.io_caps_and_budget_view
AS
SELECT case
           when io_caps_io_start_date is null then budget_advertiser_name
           else io_caps_advertiser_name end                          as advertiser_name,
       case
           when io_caps_io_start_date is null then budget_advertiser_lid
           else io_caps_advertiser_lid end                           as advertiser_lid,
       case
           when io_caps_io_start_date is null then budget_advertiser_hid
           else io_caps_advertiser_hid end                           as advertiser_hid,
       budget_advertiser_budget_amount                               as advertiser_budget,
       budget_revenue                                                as budget_revenue,
       coalesce(budget_yesterday_revenue, io_caps_yesterday_revenue) as yesterday_revenue,
       coalesce(budget_today_revenue, io_caps_today_revenue)         as today_revenue,
       io_caps_revenue                                               as io_caps_revenue,
       io_caps_revenue_diff                                          as io_caps_revenue_diff,
       budget_last_7_completed_days_revenue,
       io_caps_io_revenue_cap                                        as io_revenue_cap,
       io_caps_io_total_day                                          as io_total_day,
       io_caps_io_elapsed_day                                        as io_elapsed_day,
       io_caps_io_start_date                                         as io_start_date,
       io_caps_io_end_date                                           as io_end_date,
       io_caps_revenue_excluding_today                               as io_caps_revenue_excluding_today
FROM (SELECT case
                 WHEN advertiser_name is null OR advertiser_name = ''
                     then concat(provider_account_name, ' (Feed)')
                 else advertiser_name end              AS "budget_advertiser_name",
             budget_advertiser_lid,
             budget_advertiser_hid,
             max(budget_advertiser_budget_amount)      AS "budget_advertiser_budget_amount",
             sum(budget_revenue)                       AS "budget_revenue",
             sum(budget_yesterday_revenue)             AS "budget_yesterday_revenue",
             sum(budget_today_revenue)                 AS "budget_today_revenue",
             sum(budget_last_7_completed_days_revenue) AS "budget_last_7_completed_days_revenue"
      FROM ( SELECT budget."provider_account_name"                             AS "provider_account_name",
                    budget."provider_account_lid"                              AS "provider_account_lid",
                    budget."provider_account_hid"                              AS "provider_account_hid",
                    budget."advertiser_name"                                   AS "advertiser_name",
                    budget."advertiser_lid"                                    AS "budget_advertiser_lid",
                    budget."advertiser_hid"                                    AS "budget_advertiser_hid",
                    MAX(budget."advertiser_budget_amount")                     AS "budget_advertiser_budget_amount",
                    COALESCE(SUM(budget."revenue" + budget."revenue_diff"), 0) AS "budget_revenue",
                    COALESCE(SUM(budget."yesterday_revenue"), 0)               AS "budget_yesterday_revenue",
                    COALESCE(SUM(budget."today_revenue"), 0)                   AS "budget_today_revenue",
                    COALESCE(SUM(budget."last_7_completed_days_revenue"), 0)   AS "budget_last_7_completed_days_revenue"
             FROM addotnet.ad_event_daily_adjustment_budget AS budget
             WHERE toDate(budget."event_date") >= toDate(toStartOfMonth(now()))
               AND toDate(budget."event_date") < toDate(toStartOfMonth(addMonths(now(), 1)))
               AND ((LENGTH((budget."advertiser_name")) > 0) OR (LENGTH((budget."provider_account_name")) > 0))
             GROUP BY provider_account_name, provider_account_lid, provider_account_hid, advertiser_name,
                      advertiser_lid, advertiser_hid) a1
      group by budget_advertiser_name, budget_advertiser_lid, budget_advertiser_hid) a
         FULL OUTER JOIN
     (SELECT io_caps."advertiser_name"                     AS "io_caps_advertiser_name",
             io_caps."advertiser_lid"                      AS "io_caps_advertiser_lid",
             io_caps."advertiser_hid"                      AS "io_caps_advertiser_hid",
             io_caps."io_revenue_cap"                      AS "io_caps_io_revenue_cap",
             io_caps."io_total_day"                        AS "io_caps_io_total_day",
             io_caps."io_elapsed_day"                      AS "io_caps_io_elapsed_day",
             io_caps."io_start_date"                       AS "io_caps_io_start_date",
             io_caps."io_end_date"                         AS "io_caps_io_end_date",
             COALESCE(SUM(io_caps."yesterday_revenue"), 0) AS "io_caps_yesterday_revenue",
             COALESCE(SUM(io_caps."today_revenue"), 0)     AS "io_caps_today_revenue",
             COALESCE(sum(io_caps."revenue"), 0)           AS "io_caps_revenue",
             COALESCE(sum(io_caps."revenue_diff"), 0)      AS "io_caps_revenue_diff",
             (COALESCE(sum(io_caps."revenue"), 0) + COALESCE(sum(io_caps."revenue_diff"), 0) -
              COALESCE(SUM(io_caps."today_revenue"), 0))   AS "io_caps_revenue_excluding_today"
      FROM addotnet.ad_event_daily_adjustment_budget AS io_caps
      WHERE toDate(io_caps."io_start_date") <= toDate(now())
        AND (toDate(io_caps."io_end_date") IS NULL OR toDate(io_caps."io_end_date") >= toDate(now()))
        AND LENGTH(io_caps.advertiser_name) > 0
        AND toDate(io_caps."io_start_date") >= '2019-01-01'
      GROUP BY advertiser_name, advertiser_lid, advertiser_hid, io_revenue_cap, io_total_day, io_elapsed_day,
               io_start_date, io_end_date) b
     ON a.budget_advertiser_hid = b.io_caps_advertiser_hid and a.budget_advertiser_lid = b.io_caps_advertiser_lid;

CREATE TABLE IF NOT EXISTS addotnet.bidwise_sync
(
    source         String,
    category_ssaid String,
    category_id    String,
    pause          Nullable(Int32)
)
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/bidwise_sync/{shard}', '{replica}')
        PARTITION BY (source) PRIMARY KEY (source, category_ssaid, category_id)
        ORDER BY (source, category_ssaid, category_id)
        SETTINGS index_granularity = 8192;


DROP TABLE IF EXISTS addotnet.io_caps_and_budget_view;
CREATE VIEW addotnet.io_caps_and_budget_view
AS
SELECT case
           when io_caps_io_start_date is null then budget_advertiser_name
           else io_caps_advertiser_name end                          as advertiser_name,
       case
           when io_caps_io_start_date is null then budget_advertiser_lid
           else io_caps_advertiser_lid end                           as advertiser_lid,
       case
           when io_caps_io_start_date is null then budget_advertiser_hid
           else io_caps_advertiser_hid end                           as advertiser_hid,
       budget_advertiser_budget_amount                               as advertiser_budget,
       budget_revenue                                                as budget_revenue,
       coalesce(budget_yesterday_revenue, io_caps_yesterday_revenue) as yesterday_revenue,
       coalesce(budget_today_revenue, io_caps_today_revenue)         as today_revenue,
       io_caps_revenue                                               as io_caps_revenue,
       io_caps_revenue_diff                                          as io_caps_revenue_diff,
       budget_last_7_completed_days_revenue,
       io_caps_io_revenue_cap                                        as io_revenue_cap,
       io_caps_io_total_day                                          as io_total_day,
       io_caps_io_elapsed_day                                        as io_elapsed_day,
       io_caps_io_start_date                                         as io_start_date,
       io_caps_io_end_date                                           as io_end_date,
       io_caps_revenue_excluding_today                               as io_caps_revenue_excluding_today,
       io_caps_last_7_completed_days_revenue
FROM (SELECT case
                 WHEN advertiser_name is null OR advertiser_name = ''
                     then concat(provider_account_name, ' (Feed)')
                 else advertiser_name end              AS "budget_advertiser_name",
             budget_advertiser_lid,
             budget_advertiser_hid,
             max(budget_advertiser_budget_amount)      AS "budget_advertiser_budget_amount",
             sum(budget_revenue)                       AS "budget_revenue",
             sum(budget_yesterday_revenue)             AS "budget_yesterday_revenue",
             sum(budget_today_revenue)                 AS "budget_today_revenue",
             sum(budget_last_7_completed_days_revenue) AS "budget_last_7_completed_days_revenue"
      FROM ( SELECT budget."provider_account_name"                             AS "provider_account_name",
                    budget."provider_account_lid"                              AS "provider_account_lid",
                    budget."provider_account_hid"                              AS "provider_account_hid",
                    budget."advertiser_name"                                   AS "advertiser_name",
                    budget."advertiser_lid"                                    AS "budget_advertiser_lid",
                    budget."advertiser_hid"                                    AS "budget_advertiser_hid",
                    MAX(budget."advertiser_budget_amount")                     AS "budget_advertiser_budget_amount",
                    COALESCE(SUM(budget."revenue" + budget."revenue_diff"), 0) AS "budget_revenue",
                    COALESCE(SUM(budget."yesterday_revenue"), 0)               AS "budget_yesterday_revenue",
                    COALESCE(SUM(budget."today_revenue"), 0)                   AS "budget_today_revenue",
                    COALESCE(SUM(budget."last_7_completed_days_revenue"), 0)   AS "budget_last_7_completed_days_revenue"
             FROM addotnet.ad_event_daily_adjustment_budget AS budget
             WHERE toDate(budget."event_date") >= toDate(toStartOfMonth(now()))
               AND toDate(budget."event_date") < toDate(toStartOfMonth(addMonths(now(), 1)))
               AND ((LENGTH((budget."advertiser_name")) > 0) OR (LENGTH((budget."provider_account_name")) > 0))
               AND advertiser_lid <> -8759690492353553257
             GROUP BY provider_account_name, provider_account_lid, provider_account_hid, advertiser_name,
                      advertiser_lid, advertiser_hid) a1
      group by budget_advertiser_name, budget_advertiser_lid, budget_advertiser_hid) a
         FULL OUTER JOIN
     (SELECT io_caps."advertiser_name"                                 AS "io_caps_advertiser_name",
             io_caps."advertiser_lid"                                  AS "io_caps_advertiser_lid",
             io_caps."advertiser_hid"                                  AS "io_caps_advertiser_hid",
             io_caps."io_revenue_cap"                                  AS "io_caps_io_revenue_cap",
             io_caps."io_total_day"                                    AS "io_caps_io_total_day",
             io_caps."io_elapsed_day"                                  AS "io_caps_io_elapsed_day",
             io_caps."io_start_date"                                   AS "io_caps_io_start_date",
             io_caps."io_end_date"                                     AS "io_caps_io_end_date",
             COALESCE(SUM(io_caps."yesterday_revenue"), 0)             AS "io_caps_yesterday_revenue",
             COALESCE(SUM(io_caps."today_revenue"), 0)                 AS "io_caps_today_revenue",
             COALESCE(sum(io_caps."revenue"), 0)                       AS "io_caps_revenue",
             COALESCE(sum(io_caps."revenue_diff"), 0)                  AS "io_caps_revenue_diff",
             (COALESCE(sum(io_caps."revenue"), 0) + COALESCE(sum(io_caps."revenue_diff"), 0) -
              COALESCE(SUM(io_caps."today_revenue"), 0))               AS "io_caps_revenue_excluding_today",
             COALESCE(SUM(io_caps."last_7_completed_days_revenue"), 0) AS "io_caps_last_7_completed_days_revenue"
      FROM addotnet.ad_event_daily_adjustment_budget AS io_caps
      WHERE toDate(io_caps."io_start_date") <= toDate(now())
        AND (toDate(io_caps."io_end_date") IS NULL OR toDate(io_caps."io_end_date") >= toDate(now()))
        AND LENGTH(io_caps.advertiser_name) > 0
        AND toDate(io_caps."io_start_date") >= '2019-01-01'
        AND advertiser_lid <> -8759690492353553257
      GROUP BY advertiser_name, advertiser_lid, advertiser_hid, io_revenue_cap, io_total_day, io_elapsed_day,
               io_start_date, io_end_date) b
     ON a.budget_advertiser_hid = b.io_caps_advertiser_hid and a.budget_advertiser_lid = b.io_caps_advertiser_lid;

DROP table if exists addotnet.adgroup_sid_cap_view;
create view if not exists addotnet.adgroup_sid_cap_view as
SELECT event_date,
       adgroup_name,
       traffic_source_name,
       sid,
       coalesce(source_daily_clicks_caps, source_daily_sid_clicks_caps)   as click_cap,
       coalesce(source_daily_revenue_caps, source_daily_sid_revenue_caps) as revenue_cap,
       sum(CASE
               WHEN event_date = toDate(NOW()) then paid_clicks + paid_clicks_diff
               else 0 end)                                                as today_clicks,
       sum(CASE
               WHEN event_date = toDate(yesterday()) then paid_clicks + paid_clicks_diff
               else 0 end)                                                as yesterday_clicks,
       sum(today_revenue)                                                 as today_revenue,
       sum(yesterday_revenue)                                             as yesterday_revenue,
       CASE
           WHEN click_cap > 0 THEN round((today_clicks) * 100 / click_cap, 2)
           ELSE 0 END                                                        click_cap_percent,
       CASE
           WHEN revenue_cap > 0 THEN round((today_revenue) * 100 / revenue_cap, 2)
           ELSE 0 END                                                        revenue_cap_percent
FROM ad_event_daily_adjustment
WHERE (click_cap is not null OR revenue_cap is not null)
GROUP BY event_date,
         adgroup_name,
         traffic_source_name,
         sid,
         source_daily_revenue_caps,
         source_daily_sid_revenue_caps,
         source_daily_clicks_caps,
         source_daily_sid_clicks_caps;

DROP table if exists addotnet.adgroup_denied_traffic_providers_view;
create view if not exists addotnet.adgroup_denied_traffic_providers_view as
select sid,
       provider_id,
       CASE
           WHEN length(provider_id) > 0 THEN concat(CAST(sid as Nullable(String)), '-', provider_id)
           else CAST(sid as Nullable(String)) end                                     as sid_said,
       dictGet('addotnet.advertiser_by_adgroup_dim', 'adgroup_name', adgroup_id)      AS adgroup_name,
       dictGet('addotnet.advertiser_by_adgroup_dim', 'adgroup_status', adgroup_id)    AS adgroup_status,
       dictGet('addotnet.advertiser_by_adgroup_dim', 'campaign_status', adgroup_id)   AS campaign_status,
       dictGet('addotnet.advertiser_by_adgroup_dim', 'advertiser_status', adgroup_id) AS advertiser_status,
       dictGet('addotnet.advertiser_by_adgroup_dim', 'company_name', adgroup_id)      AS advertiser_name,
       dictGet('addotnet.publisher_by_source_dim', 'publisher_name', sid)             AS publisher_name,
       dr.last_90_days_revenue
FROM addotnet.adgroup_denied_traffic_providers adtp
         LEFT JOIN (select abs(adgroup_lid) adgroup_id, sid, said, sum(revenue) last_90_days_revenue
                    FROM addotnet.ad_event_daily
                    where event_date between toDate(addDays(now(), -90)) AND toDate(addDays(now(), -1))
                    group by adgroup_lid, sid, said) dr
                   on adtp.sid = CAST(dr.sid as Nullable(UInt64)) and adtp.provider_id = dr.said and
                      adtp.adgroup_id = dr.adgroup_id
;

drop table if exists addotnet.intel_ad_event_view;
create view addotnet.intel_ad_event_view as
SELECT *,
       CASE
           WHEN (eventpixel_name) = 'PCI' THEN
               CASE
                   WHEN (adgroup_name = 'Rally Generic' AND actions_worth < 0.01) THEN (actions_worth * 1000)
                   ELSE (actions_worth * 100) END
           END                                                                        AS pci_aw,
       CASE
           WHEN (eventpixel_name) = 'Scroll Rate' THEN (COALESCE((actions_worth), 0))
           END                                                                        AS scroll_rate_aw,
       CASE WHEN (eventpixel_name) = 'Visits' THEN (COALESCE((actions_worth), 0)) END AS visits_aw
FROM addotnet.ad_event_view
WHERE (advertiser_name) is not null
  and (advertiser_lid) <> -1;


-- speed optimization on the io caps and budget combined report.

drop table if exists addotnet.io_caps_report_dim_view;
create view addotnet.io_caps_report_dim_view as
SELECT io_caps."advertiser_name"                                       AS "advertiser_name",
       io_caps."advertiser_lid"                                        AS "advertiser_lid",
       io_caps."advertiser_hid"                                        AS "advertiser_hid",
       io_caps."io_revenue_cap"                                        AS "revenue_cap",
       io_caps."io_total_day"                                          AS "total_day",
       io_caps."io_elapsed_day"                                        AS "elapsed_day",
       coalesce(CAST(io_caps."io_start_date" as Nullable(String)), '') AS "start_date",
       CASE WHEN coalesce(CAST(io_caps."io_end_date" as Nullable(String)), '') = '0000-00-00'
           THEN cast(NULL as Nullable(String))  ELSE coalesce(CAST(io_caps."io_end_date" as Nullable(String)), '') end  AS "end_date",
       COALESCE(SUM(io_caps."yesterday_revenue"), 0)                   AS "yesterday_revenue",
       COALESCE(SUM(io_caps."today_revenue"), 0)                       AS "today_revenue",
       COALESCE(sum(io_caps."revenue"), 0)                             AS "revenue",
       COALESCE(sum(io_caps."revenue_diff"), 0)                        AS "revenue_diff",
       (COALESCE(sum(io_caps."revenue"), 0) + COALESCE(sum(io_caps."revenue_diff"), 0) -
        COALESCE(SUM(io_caps."today_revenue"), 0))                     AS "revenue_excluding_today",
       COALESCE(SUM(io_caps."last_7_completed_days_revenue"), 0)       AS "last_7_completed_days_revenue"
FROM addotnet.ad_event_daily_adjustment_budget AS io_caps
WHERE toDate(io_caps."io_start_date") is not null
  AND (toDate(io_caps."io_end_date") IS NULL OR toDate(io_caps."io_end_date") >= toDate(now()))
  AND LENGTH(io_caps.advertiser_name) > 0
  AND toDate(io_caps."io_start_date") >= '2019-01-01'
  AND advertiser_lid <> -8759690492353553257
GROUP BY advertiser_name, advertiser_lid, advertiser_hid, io_revenue_cap, io_total_day, io_elapsed_day,
         io_start_date, io_end_date
UNION ALL
SELECT dictGet('addotnet.advertiser_dim', 'advertiser_name',
               toUInt64(ABS(multiIf(advertiser_lid = -1, 0, advertiser_lid)))) AS advertiser_name,
       advertiser_lid                                                          AS advertiser_lid,
       0                                                                       AS advertiser_hid,
       io_revenue_cap                                                          AS revenue_cap,
       io_total_day                                                            AS total_day,
       io_elapsed_day                                                          AS elapsed_day,
       CAST(io_start_date, 'String')                                           AS start_date,
       CASE
           WHEN coalesce(CAST(io_end_date as String), '') = '0000-00-00'
               THEN cast('' as String)
           ELSE coalesce(CAST(io_end_date as String), '') END                  AS end_date,
       0                                                                       AS yesterday_revenue,
       0                                                                       AS today_revenue,
       0                                                                       AS revenue,
       0                                                                       AS revenue_diff,
       0                                                                       AS revenue_excluding_today,
       0                                                                       AS last_7_completed_days_revenue
FROM addotnet.io_cap_future_dim
WHERE io_start_date > now()
GROUP BY advertiser_lid, io_cap_id, io_autostart, io_start_date, io_end_date, io_revenue_cap, io_elapsed_day,
         io_total_day;


DROP DICTIONARY IF EXISTS addotnet.io_cap_report_dim;
CREATE DICTIONARY addotnet.io_cap_report_dim
(
    advertiser_name               String,
    advertiser_lid                Int64,
    advertiser_hid                Int64,
    revenue_cap                Float64,
    total_day                  Int32,
    elapsed_day                Int32,
    start_date                 String,
    end_date                   String,
    yesterday_revenue             Float64,
    today_revenue                 Float64,
    revenue                       Float64,
    revenue_diff                  Float64,
    revenue_excluding_today       Float64,
    last_7_completed_days_revenue Float64
)PRIMARY KEY advertiser_lid,advertiser_hid,start_date,end_date
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'io_caps_report_dim_view' PASSWORD '' DB 'addotnet'))
LIFETIME(MIN 300 MAX 360)
LAYOUT(COMPLEX_KEY_HASHED());

-- % IO Spent, %Days laps , %pacing, avg daily spend, target daily spend
DROP TABLE IF EXISTS addotnet.io_caps_and_budget_view_v1;
CREATE VIEW addotnet.io_caps_and_budget_view_v1
AS
SELECT case
           when io_caps_io_start_date is null then budget_advertiser_name
           else io_caps_advertiser_name end                          as advertiser_name,
       case
           when io_caps_io_start_date is null then budget_advertiser_lid
           else io_caps_advertiser_lid end                           as advertiser_lid,
       case
           when io_caps_io_start_date is null then budget_advertiser_hid
           else io_caps_advertiser_hid end                           as advertiser_hid,
       budget_advertiser_budget_amount                               as advertiser_budget,
       budget_revenue                                                as budget_revenue,
       coalesce(budget_yesterday_revenue, io_caps_yesterday_revenue) as yesterday_revenue,
       coalesce(budget_today_revenue, io_caps_today_revenue)         as today_revenue,
       io_caps_revenue                                               as io_caps_revenue,
       io_caps_revenue_diff                                          as io_caps_revenue_diff,
       budget_last_7_completed_days_revenue,
       io_caps_io_revenue_cap                                        as io_revenue_cap,
       io_caps_io_total_day                                          as io_total_day,
       io_caps_io_elapsed_day                                        as io_elapsed_day,
       io_caps_io_start_date                                         as io_start_date,
       io_caps_io_end_date                                           as io_end_date,
       io_caps_revenue_excluding_today                               as io_caps_revenue_excluding_today,
       io_caps_last_7_completed_days_revenue
FROM (SELECT case
                 WHEN advertiser_name is null OR advertiser_name = ''
                     then concat(provider_account_name, ' (Feed)')
                 else advertiser_name end              AS "budget_advertiser_name",
             budget_advertiser_lid,
             budget_advertiser_hid,
             max(budget_advertiser_budget_amount)      AS "budget_advertiser_budget_amount",
             sum(budget_revenue)                       AS "budget_revenue",
             sum(budget_yesterday_revenue)             AS "budget_yesterday_revenue",
             sum(budget_today_revenue)                 AS "budget_today_revenue",
             sum(budget_last_7_completed_days_revenue) AS "budget_last_7_completed_days_revenue"
      FROM ( SELECT budget."provider_account_name"                             AS "provider_account_name",
                    budget."provider_account_lid"                              AS "provider_account_lid",
                    budget."provider_account_hid"                              AS "provider_account_hid",
                    budget."advertiser_name"                                   AS "advertiser_name",
                    budget."advertiser_lid"                                    AS "budget_advertiser_lid",
                    budget."advertiser_hid"                                    AS "budget_advertiser_hid",
                    MAX(budget."advertiser_budget_amount")                     AS "budget_advertiser_budget_amount",
                    COALESCE(SUM(budget."revenue" + budget."revenue_diff"), 0) AS "budget_revenue",
                    COALESCE(SUM(budget."yesterday_revenue"), 0)               AS "budget_yesterday_revenue",
                    COALESCE(SUM(budget."today_revenue"), 0)                   AS "budget_today_revenue",
                    COALESCE(SUM(budget."last_7_completed_days_revenue"), 0)   AS "budget_last_7_completed_days_revenue"
             FROM addotnet.ad_event_daily_adjustment_budget AS budget
             WHERE toDate(budget."event_date") >= toDate(toStartOfMonth(now()))
               AND toDate(budget."event_date") < toDate(toStartOfMonth(addMonths(now(), 1)))
               AND ((LENGTH((budget."advertiser_name")) > 0) OR (LENGTH((budget."provider_account_name")) > 0))
               AND advertiser_lid <> -8759690492353553257
             GROUP BY provider_account_name, provider_account_lid, provider_account_hid, advertiser_name,
                      advertiser_lid, advertiser_hid) a1
      group by budget_advertiser_name, budget_advertiser_lid, budget_advertiser_hid) a
         FULL OUTER JOIN
     (SELECT advertiser_name                       AS io_caps_advertiser_name,
             advertiser_lid                        AS io_caps_advertiser_lid,
             advertiser_hid                        AS io_caps_advertiser_hid,
             revenue_cap                        AS io_caps_io_revenue_cap,
             total_day                          AS io_caps_io_total_day,
             elapsed_day                        AS io_caps_io_elapsed_day,
             CAST(start_date AS Nullable(Date)) AS io_caps_io_start_date,
             CAST(end_date AS Nullable(Date))   AS io_caps_io_end_date,
             yesterday_revenue                     AS io_caps_yesterday_revenue,
             today_revenue                         AS io_caps_today_revenue,
             revenue                               AS io_caps_revenue,
             revenue_diff                          AS io_caps_revenue_diff,
             revenue_excluding_today               AS io_caps_revenue_excluding_today,
             last_7_completed_days_revenue         AS io_caps_last_7_completed_days_revenue
      FROM addotnet.io_cap_report_dim) b
     ON a.budget_advertiser_hid = b.io_caps_advertiser_hid and a.budget_advertiser_lid = b.io_caps_advertiser_lid;


drop table if exists addotnet.io_caps_report_view;

create view addotnet.io_caps_report_view as
SELECT advertiser_name                       AS advertiser_name,
       advertiser_lid                        AS advertiser_lid,
       advertiser_hid                        AS advertiser_hid,
       revenue_cap                        AS io_revenue_cap,
       total_day                          AS io_total_day,
       elapsed_day                        AS io_elapsed_day,
       CAST(start_date AS Nullable(Date)) AS io_start_date,
       CAST(end_date AS Nullable(Date))   AS io_end_date,
       yesterday_revenue                     AS yesterday_revenue,
       today_revenue                         AS today_revenue,
       revenue                               AS revenue,
       revenue_diff                          AS revenue_diff,
       revenue_excluding_today               AS revenue_excluding_today,
       last_7_completed_days_revenue         AS last_7_completed_days_revenue
FROM addotnet.io_cap_report_dim;


DROP TABLE IF EXISTS addotnet.new_sources_view;
CREATE VIEW addotnet.new_sources_view
AS
SELECT m.event_date,
       m.sid,
       m.said,
       m.affiliate_account_name,
       m.affiliate_account_lid,
       m.traffic_source_name,
       m.advertiser_name,
       m.advertiser_lid,
       m.advertiser_hid,
       dictGet('addotnet.managers_by_publisher_dim', 'publisher_managers',
               toUInt64(ABS(affiliate_account_lid))) AS publisher_managers,
       min(m.event_date)                             as min_date,
       sum(m.requests)                               as requests,
       sum(m.ad_returns)                             as ad_returns,
       sum(m.raw_clicks)                             as raw_clicks,
       sum(m.paid_clicks)                            as paid_clicks,
       sum(m.revenue)                                as revenue,
       sum(m.pub_payout)                             as pub_payout,
       sum(m.actions_worth)                          as actions_worth,
       sum(m.mb_clicks)                              as mb_clicks
from addotnet.ad_event_daily_adjustment m
where (m.event_date > toDate(addDays(now(), -30))
    and m.sid not in (select m1.sid
                      from addotnet.ad_event_daily_adjustment m1
                      where m1.event_date between toDate(addDays(now(), -60)) and toDate(addDays(now(), -30))
                      group by m1.sid))
group by m.event_date,
         m.sid,
         m.said,
         m.affiliate_account_name,
         m.affiliate_account_lid,
         m.traffic_source_name,
         m.advertiser_name,
         m.advertiser_lid,
         m.advertiser_hid
order by min_date desc;


CREATE TABLE addotnet.bid_modifier_hourly
(
    `event_year`                          UInt16,
    `event_month`                         UInt8,
    `event_date`                          Date,
    `event_hour`                          UInt8,
    `event_minute`                        UInt8,
    `event_timestamp`                     Nullable(DateTime),
    `search_timestamp`                    Nullable(DateTime),
    `click_timestamp`                     Nullable(DateTime),
    `action_timestamp`                    Nullable(DateTime),
    `advertiser_lid`                      Int64 DEFAULT CAST(-1, 'Int64'),
    `advertiser_hid`                      Int64 DEFAULT CAST(-1, 'Int64'),
    `campaign_id`                         Nullable(Int64),
    `adgroup_lid`                         Nullable(Int64),
    `adgroup_hid`                         Nullable(Int64),
    `provider_account_lid`                Nullable(Int64),
    `provider_account_hid`                Nullable(Int64),
    `target_id`                           Nullable(Int64),
    `creative_id`                         Nullable(Int64),
    `feed_advertiser_id`                  Nullable(Int64),
    `faa_id1`                             Nullable(String),
    `faa_id2`                             Nullable(String),
    `faa_id3`                             Nullable(String),
    `affiliate_account_lid`               Nullable(Int64),
    `affiliate_account_hid`               Nullable(Int64),
    `traffic_source_lid`                  Nullable(Int64),
    `traffic_source_hid`                  Nullable(Int64),
    `adgroup_type`                        Nullable(String),
    `uuid`                                Nullable(String),
    `uuid_click`                          Nullable(String),
    `is_media_buy`                        Nullable(Int8),
    `browser_family`                      Nullable(String),
    `browser_version`                     Nullable(String),
    `os_family`                           Nullable(String),
    `os_version`                          Nullable(String),
    `device_name`                         Nullable(String),
    `is_mobile`                           Nullable(UInt8),
    `search_ip`                           Nullable(String),
    `search_user_agent`                   Nullable(String),
    `client_ip`                           Nullable(String),
    `user_agent`                          Nullable(String),
    `referer`                             Nullable(String),
    `fallback_url`                        Nullable(String),
    `mark_url`                            Nullable(String),
    `auto_redirect_next_hop_url`          Nullable(String),
    `publisher_referrer_domain`           Nullable(String),
    `country_code`                        Nullable(Int32),
    `zip_code`                            Nullable(String),
    `metro_code`                          Nullable(Int32),
    `region_code`                         Nullable(Int32),
    `reason_for_unpaid`                   Nullable(String),
    `findology_internal`                  Nullable(UInt8),
    `viewed_text`                         Nullable(String),
    `target_keyword`                      Nullable(String),
    `search_keyword`                      Nullable(String),
    `requests`                            Nullable(UInt64),
    `ad_returns`                          Nullable(UInt64),
    `ad_returned`                         Array(String),
    `search_net_bid_price`                Nullable(Float64),
    `search_gross_bid_price`              Nullable(Float64),
    `cpa_goal`                            Nullable(Float64),
    `raw_clicks`                          Nullable(UInt64),
    `paid_clicks`                         Nullable(UInt64),
    `pub_payout`                          Nullable(Float64),
    `revenue`                             Nullable(Float64),
    `eventpixel_id`                       Nullable(Int64),
    `eventpixel_name`                     Nullable(String),
    `eventpixel_type`                     Nullable(String),
    `dollars_worth`                       Nullable(Float64),
    `actions_worth`                       Nullable(Float64),
    `event_fires_count`                   Nullable(Int64),
    `eventpixel_margin`                   Nullable(Float64),
    `eventpixel_calculated_dollars_worth` Nullable(Float64),
    `bid_modifier_multiplier`             Nullable(Float64),
    `bid_modifier_details`                Nullable(String),
    `sid`                                 Int64,
    `said`                                String,
    `raw_said`                            String,
    `dt`                                  Date,
    `hour_id`                             UInt8
)
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/bid_modifier_hourly/{shard}', '{replica}')
        PARTITION BY (dt, hour_id)
        PRIMARY KEY (event_date, advertiser_lid, sid, said)
        ORDER BY (event_date, advertiser_lid, sid, said)
        SETTINGS index_granularity = 8192;


CREATE VIEW addotnet.bid_modifier_hourly_view
AS
SELECT *,
       dictGet('addotnet.adgroup_dim', 'adgroup_name', toUInt64(ABS(adgroup_lid)))             AS adgroup_name,
       dictGet('addotnet.adgroup_dim', 'adgroup_status', toUInt64(ABS(adgroup_lid)))           AS adgroup_status,
       dictGet('addotnet.advertiser_dim', 'advertiser_name', toUInt64(ABS(advertiser_lid)))    AS advertiser_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_status', toUInt64(ABS(advertiser_lid)))  AS advertiser_status,
       dictGet('addotnet.region_dim', 'state_name', tuple(COALESCE(region_code, 0)))           AS state_name,
       dictGet('addotnet.region_dim', 'state_short_name', tuple(COALESCE(region_code, 0)))     AS state_short_name,
       dictGet('addotnet.target_dim', 'keyword', toUInt64(ABS(target_id)))                     AS target_name,
       dictGet('addotnet.affiliate_account_dim', 'name', toUInt64(ABS(affiliate_account_lid))) AS affiliate_account_name
FROM addotnet.bid_modifier_hourly;


DROP TABLE IF EXISTS addotnet.advertiser_budget;
CREATE TABLE addotnet.advertiser_budget
(
    `advertiser_name`      String,
    `advertiser_lid`       Int64,
    `advertiser_hid`       Int64,
    `provider_account_lid` Int64,
    `provider_account_hid` Int64,
    `month_id`             Int32,
    `budget_amount`        Nullable(Float64)
)
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/advertiser_budget/{shard}', '{replica}')
        PARTITION BY month_id
        PRIMARY KEY (advertiser_lid, month_id)
        ORDER BY (advertiser_lid, month_id)
        SETTINGS index_granularity = 8192;


CREATE VIEW addotnet.cpa_goal_roas_history_by_adgroup
AS
SELECT dictGet('addotnet.adgroup_dim', 'adgroup_name', toUInt64(ABS(adgroup_lid)))          AS adgroup_name,
       dictGet('addotnet.advertiser_dim', 'advertiser_name', toUInt64(ABS(advertiser_lid))) AS advertiser_name,
       cpa_goal,
       multiIf(eventpixel_margin != 0, 1 / eventpixel_margin, 0)                            AS roas_history,
       event_date
FROM addotnet.ad_event
WHERE event_type IN ('click', 'action')
GROUP BY advertiser_name, adgroup_name, cpa_goal, roas_history, event_date;