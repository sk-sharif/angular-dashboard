CREATE TABLE IF NOT EXISTS addotnet.facebook_stats
(
    dt                    Date,
    event_date            Date,
    sid                   Int64,
    provider_id           Nullable(String),
    mb_name               Nullable(String),
    mb_campaign_id        Nullable(String),
    mb_adgroup_id         Nullable(Int64),
    adgroup_lid           Nullable(Int64),
    adgroup_hid           Nullable(Int64),
    feed_advertiser_id    Nullable(Int64),
    mb_clicks             Nullable(Int32),
    mb_cost               Nullable(Float64),
    mb_dollars_worth      Nullable(Float64),
    mb_ad_id              Nullable(String),
    mb_ad_name            Nullable(String),
    mb_ad_link            Nullable(String),
    mb_website_purchases  Nullable(Int64)
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/facebook_stats/{shard}',
                                 '{replica}') PARTITION BY (dt) PRIMARY KEY
(
    event_date
)
    ORDER BY
(
    event_date
)
    SETTINGS index_granularity = 8192;


CREATE VIEW IF NOT EXISTS addotnet.facebook_stats_view
AS
SELECT
    fb.event_date AS event_date,
    COALESCE(mb.affiliateaccount_lid, 0) AS affiliate_account_lid,
    COALESCE(mb.affiliateaccount_hid, 0) AS affiliate_account_hid,
    COALESCE(fb.sid, 0)                  AS sid,
    COALESCE(mb.provideraccount_lid, 0)  AS provider_account_lid,
    COALESCE(mb.provideraccount_hid, 0)  AS provider_account_hid,
    COALESCE(mb.feed_id, 0)              AS feed_advertiser_id,
    COALESCE(mb.advertiser_lid, 0)       AS advertiser_lid,
    COALESCE(mb.advertiser_hid, 0)       AS advertiser_hid,
    COALESCE(mb.campaign_id, 0)          AS campaign_id,
    COALESCE(fb.adgroup_lid, 0)          AS adgroup_lid,
    COALESCE(fb.adgroup_hid, 0)          AS adgroup_hid,
    COALESCE(fb.mb_clicks, 0)            AS mb_clicks,
    COALESCE(fb.mb_cost, 0)              AS mb_cost,
    COALESCE(fb.mb_dollars_worth,0) AS mb_dollars_worth  FROM (
                                                                  SELECT cc_part.event_date AS event_date, cc_part.sid AS sid, cc_part.adgroup_lid AS adgroup_lid, cc_part.adgroup_hid AS adgroup_hid, cc_part.mb_clicks AS mb_clicks, cc_part.mb_cost AS mb_cost, dw_part.mb_dollars_worth AS mb_dollars_worth  FROM (
                                                                                                                                                                                                                                                                                                                          SELECT event_date, sid, adgroup_lid, adgroup_hid, SUM(mb_clicks) AS mb_clicks, SUM(mb_cost) AS mb_cost FROM addotnet.facebook_stats WHERE dt = (SELECT max(partition) FROM system.parts WHERE table = 'facebook_stats' AND database = 'addotnet' AND rows > 0) AND event_date >= toStartOfMonth(now()) GROUP BY event_date, sid, adgroup_lid, adgroup_hid
                                                                  UNION ALL
                                                                  SELECT dt AS event_date, sid, adgroup_lid, adgroup_hid, mb_clicks, mb_cost FROM addotnet.media_buyer_adjustment WHERE affiliateaccount_lid = -7697216570435406790 AND affiliateaccount_hid = -7807186729539056298 AND event_date < toStartOfMonth(now())
                                                              ) cc_part
                                                                  INNER JOIN (
    SELECT dw_all_part.* FROM (
                                  SELECT dt, event_date, sid, adgroup_lid, adgroup_hid, SUM(mb_dollars_worth) AS mb_dollars_worth FROM addotnet.facebook_stats WHERE dt = (SELECT max(partition) FROM system.parts WHERE table = 'facebook_stats' AND database = 'addotnet' AND rows > 0) GROUP BY dt, event_date, sid, adgroup_lid, adgroup_hid
    UNION ALL
    SELECT dt, dt AS event_date, sid, adgroup_lid, adgroup_hid, mb_dollars_worth FROM addotnet.media_buyer_adjustment WHERE dt != (SELECT max(partition) FROM system.parts WHERE table = 'facebook_stats' AND database = 'addotnet' AND rows > 0) AND affiliateaccount_lid = -7697216570435406790 AND affiliateaccount_hid = -7807186729539056298
) dw_all_part
                                                                  INNER JOIN (
    SELECT MAX(dt) AS dt, event_date, sid, adgroup_lid, adgroup_hid FROM (
                                                                             SELECT dt, event_date, sid, adgroup_lid, adgroup_hid FROM addotnet.facebook_stats WHERE dt = (SELECT max(partition) FROM system.parts WHERE table = 'facebook_stats' AND database = 'addotnet' AND rows > 0) GROUP BY dt, event_date, sid, adgroup_lid, adgroup_hid
    UNION ALL
    SELECT dt, dt AS event_date, sid, adgroup_lid, adgroup_hid FROM addotnet.media_buyer_adjustment WHERE dt != (SELECT max(partition) FROM system.parts WHERE table = 'facebook_stats' AND database = 'addotnet' AND rows > 0) AND affiliateaccount_lid = -7697216570435406790 AND affiliateaccount_hid = -7807186729539056298
) dw_max_part GROUP BY event_date, sid, adgroup_lid, adgroup_hid
    ) dw_inner_part ON dw_all_part.dt = dw_inner_part.dt AND dw_all_part.event_date = dw_inner_part.event_date AND dw_all_part.sid = dw_inner_part.sid AND dw_all_part.adgroup_lid = dw_inner_part.adgroup_lid AND dw_all_part.adgroup_hid = dw_inner_part.adgroup_hid
    ) dw_part
    ON cc_part.event_date = dw_part.event_date
    AND cc_part.sid = dw_part.sid
    AND cc_part.adgroup_lid = dw_part.adgroup_lid
    AND cc_part.adgroup_hid = dw_part.adgroup_hid
    ) fb
    LEFT JOIN addotnet.media_buyer_adjustment mb
    ON fb.event_date = mb.dt AND fb.sid = mb.sid AND fb.adgroup_lid = mb.adgroup_lid AND fb.adgroup_hid = mb.adgroup_hid;


CREATE VIEW IF NOT EXISTS addotnet.facebook_stats_adjustment
AS
SELECT adjustment.event_date                         AS event_date,
       adjustment.advertiser_lid                     AS advertiser_lid,
       adjustment.advertiser_hid                     AS advertiser_hid,
       adjustment.advertiser_name                    AS advertiser_name,
       adjustment.advertiser_status                  AS advertiser_status,
       adjustment.campaign_id                        AS campaign_id,
       adjustment.campaign_name                      AS campaign_name,
       adjustment.adgroup_lid                        AS adgroup_lid,
       adjustment.adgroup_hid                        AS adgroup_hid,
       adjustment.adgroup_name                       AS adgroup_name,
       adjustment.sid                                AS sid,
       adjustment.cpa_goal                           AS cpa_goal,
       adjustment.requests                           AS requests,
       adjustment.ad_returns                         AS ad_returns,
       adjustment.raw_clicks                         AS raw_clicks,
       adjustment.paid_clicks                        AS paid_clicks,
       adjustment.pub_payout                         AS pub_payout,
       adjustment.revenue                            AS revenue,
       adjustment.dollars_worth                      AS dollars_worth,
       adjustment.actions_worth                      AS actions_worth,
       adjustment.event_fires_count                  AS event_fires_count,
       adjustment.impressions                        AS impressions,
       adjustment.paid_clicks_diff                   AS paid_clicks_diff,
       adjustment.revenue_diff                       AS revenue_diff,
       adjustment.pub_payout_diff                    AS pub_payout_diff,
       adjustment.feed_generated_revenue_diff        AS feed_generated_revenue_diff,
       adjustment.feed_generated_pub_payout_diff     AS feed_generated_pub_payout_diff,
       adjustment.mb_clicks                          AS mb_clicks,
       adjustment.mb_cost                            AS mb_cost,
       facebook.mb_dollars_worth                     AS mb_dollars_worth,
       facebook.mb_website_purchases                 AS mb_website_purchases,
       adjustment.click_cost                         AS click_cost,
       adjustment.purchased_clicks                   AS purchased_clicks,
       adjustment.today_revenue                      AS today_revenue,
       adjustment.yesterday_revenue                  AS yesterday_revenue,
       adjustment.last_7_completed_days_revenue      AS last_7_completed_days_revenue,
       adjustment.advertiser_budget_amount           AS advertiser_budget_amount,
       adjustment.advertiser_daily_revenue_caps      AS advertiser_daily_revenue_caps,
       adjustment.advertiser_monthly_revenue_caps    AS advertiser_monthly_revenue_caps,
       adjustment.adgroup_daily_revenue_caps         AS adgroup_daily_revenue_caps
FROM (
         SELECT complete_data.*
         FROM (
                  SELECT dt,
                         event_date,
                         sid,
                         adgroup_lid,
                         adgroup_hid,
                         SUM(mb_clicks)            AS mb_clicks,
                         SUM(mb_cost)              AS mb_cost,
                         SUM(mb_dollars_worth)     AS mb_dollars_worth,
                         SUM(mb_website_purchases) AS mb_website_purchases
                  FROM addotnet.facebook_stats
                  GROUP BY dt,
                           event_date,
                           sid,
                           adgroup_lid,
                           adgroup_hid
              ) AS complete_data
                  INNER JOIN (
             SELECT MAX(dt) AS dt,
                    event_date,
                    sid,
                    adgroup_lid,
                    adgroup_hid
             FROM addotnet.facebook_stats
             GROUP BY event_date,
                      sid,
                      adgroup_lid,
                      adgroup_hid
         ) AS max_date_data
                             ON complete_data.dt = max_date_data.dt
                                 AND complete_data.event_date = max_date_data.event_date
                                 AND complete_data.sid = max_date_data.sid
                                 AND complete_data.adgroup_lid = max_date_data.adgroup_lid
                                 AND complete_data.adgroup_hid = max_date_data.adgroup_hid
     ) AS facebook
         LEFT JOIN
     (
         SELECT event_date,
                advertiser_lid,
                advertiser_hid,
                advertiser_name,
                advertiser_status,
                campaign_id,
                campaign_name,
                adgroup_lid,
                adgroup_hid,
                adgroup_name,
                sid,
                cpa_goal,
                SUM(requests)                           AS requests,
                SUM(ad_returns)                         AS ad_returns,
                SUM(raw_clicks)                         AS raw_clicks,
                SUM(paid_clicks)                        AS paid_clicks,
                SUM(pub_payout)                         AS pub_payout,
                SUM(revenue)                            AS revenue,
                SUM(dollars_worth)                      AS dollars_worth,
                SUM(actions_worth)                      AS actions_worth,
                SUM(event_fires_count)                  AS event_fires_count,
                SUM(impressions)                        AS impressions,
                SUM(paid_clicks_diff)                   AS paid_clicks_diff,
                SUM(revenue_diff)                       AS revenue_diff,
                SUM(pub_payout_diff)                    AS pub_payout_diff,
                SUM(feed_generated_revenue_diff)        AS feed_generated_revenue_diff,
                SUM(feed_generated_pub_payout_diff)     AS feed_generated_pub_payout_diff,
                SUM(mb_clicks)                          AS mb_clicks,
                SUM(mb_cost)                            AS mb_cost,
                SUM(click_cost)                         AS click_cost,
                SUM(purchased_clicks)                   AS purchased_clicks,
                SUM(today_revenue)                      AS today_revenue,
                SUM(yesterday_revenue)                  AS yesterday_revenue,
                SUM(last_7_completed_days_revenue)      AS last_7_completed_days_revenue,
                SUM(advertiser_budget_amount)           AS advertiser_budget_amount,
                SUM(advertiser_daily_revenue_caps)      AS advertiser_daily_revenue_caps,
                SUM(advertiser_monthly_revenue_caps)    AS advertiser_monthly_revenue_caps,
                SUM(adgroup_daily_revenue_caps)         AS adgroup_daily_revenue_caps
         FROM addotnet.ad_event_daily_adjustment
         WHERE is_media_buy = 1
           AND affiliate_account_lid = -7697216570435406790
           AND affiliate_account_hid = -7807186729539056298
         GROUP BY event_date,
                  advertiser_lid,
                  advertiser_hid,
                  advertiser_name,
                  advertiser_status,
                  campaign_id,
                  campaign_name,
                  adgroup_lid,
                  adgroup_hid,
                  adgroup_name,
                  sid,
                  cpa_goal
     ) AS adjustment
     ON facebook.event_date = adjustment.event_date
         AND facebook.sid = adjustment.sid
         AND facebook.adgroup_lid = adjustment.adgroup_lid
         AND facebook.adgroup_hid = adjustment.adgroup_hid;

CREATE TABLE IF NOT EXISTS addotnet.facebook_raw_stats
(
    dt                    Date,
    event_date            Date,
    mb_name               Nullable(String),
    mb_campaign_id        Nullable(String),
    mb_adgroup_id         Nullable(Int64),
    mb_clicks             Nullable(Int32),
    mb_cost               Nullable(Float64),
    mb_dollars_worth      Nullable(Float64),
    mb_ad_id              Nullable(String),
    mb_ad_name            Nullable(String),
    mb_ad_link            Nullable(String),
    mb_website_purchases  Nullable(Int64)
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/facebook_raw_stats/{shard}',
                                 '{replica}') PARTITION BY (dt) PRIMARY KEY
(
    event_date
)
    ORDER BY
(
    event_date
)
    SETTINGS index_granularity = 8192;