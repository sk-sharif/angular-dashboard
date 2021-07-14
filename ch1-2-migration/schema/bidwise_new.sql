drop table if exists addotnet.bidwise_mb_stats;
CREATE TABLE addotnet.bidwise_mb_stats
(
    `source`          String,
    `category`        String,
    `clicks`          Int64,
    `spend`           Float64,
    `cpc`             Float64,
    `referrer_domain` Nullable(String),
    `category_id`     UInt64 DEFAULT toInt64(0),
    `event_date`      String,
    `advertiser_lid`   Int64
) ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/bidwise_mb_stats/{shard}',
           '{replica}') PARTITION BY (event_date,advertiser_lid) PRIMARY KEY event_date ORDER BY event_date SETTINGS index_granularity = 8192;


DROP TABLE IF EXISTS addotnet.bidwise_ad_event_daily_with_category_new;
CREATE VIEW addotnet.bidwise_ad_event_daily_with_category_new
as
select m.event_date                  AS event_date,
       m.said_internal               AS said,
       m.advertiser_lid              AS advertiser_lid,
       sum(m.requests_internal)      AS requests,
       sum(m.raw_clicks_internal)    AS raw_clicks,
       sum(m.paid_clicks_internal)   AS paid_clicks,
       sum(m.revenue_internal)       AS revenue,
       sum(m.actions_worth_internal) AS actions_worth
FROM (
         (SELECT 0                as raw_clicks_internal,
                 c.paid_clicks    as paid_clicks_internal,
                 c.revenue        as revenue_internal,
                 a.actions_worth  AS actions_worth_internal,
                 0                as requests_internal,
                 c.raw_said       as said_internal,
                 c.event_date     as event_date,
                 c.advertiser_lid as advertiser_lid
          FROM addotnet.ad_event c
                   left join (select uuid_click, raw_said, actions_worth
                              FROM addotnet.ad_event
                              where affiliate_account_lid = -8280958097931867273
                                and event_date >= '2021-04-08'
                                and event_type = 'action') a
                             on c.uuid_click = a.uuid_click
          where c.affiliate_account_lid = -8280958097931867273
            and c.event_date >= '2021-04-08'
            and c.event_type = 'click'
            and mc_node_identifier is not null)
         UNION ALL
         SELECT rc.raw_clicks    as raw_clicks_internal,
                0                as paid_clicks_internal,
                0                as revenue_internal,
                0                AS actions_worth_internal,
                0                as requests_internal,
                rc.raw_said       as said_internal,
                rc.event_date     as event_date,
                rc.advertiser_lid as advertiser_lid
         FROM addotnet.ad_event rc
         where rc.affiliate_account_lid = -8280958097931867273
           and rc.event_date >= '2021-04-08'
           and rc.event_type = 'click'
           and mc_node_identifier is null
         UNION ALL
         (SELECT 0                                                               as raw_clicks_internal,
                 0                                                               as paid_clicks_internal,
                 0                                                               as revenue_internal,
                 0                                                               AS actions_worth_internal,
                 requests                                                        as requests_internal,
                 raw_said                                                        as said_internal,
                 r.event_date                                                    as event_date,
                 dictGet('addotnet.aid_to_advertiser_dim', 'advertiser_lid',
                         tuple(COALESCE(extractURLParameter(r.url, 'aid'), ''))) as advertiser_lid
          FROM addotnet.ad_event r
          where r.affiliate_account_lid = -8280958097931867273
            and r.event_date >= '2021-04-08'
            and r.event_type = 'request'
         )
         ) m
group by m.event_date, m.said_internal, m.advertiser_lid;


DROP TABLE IF EXISTS addotnet.bidwise_sid_mb_discrepancy_category;
CREATE VIEW addotnet.bidwise_sid_mb_discrepancy_category
as
select CASE
           WHEN a.event_date != null and a.event_date != '0000-00-00' THEN a.event_date
           ELSE b.mb_event_date END                                            AS event_date,
       CASE
           WHEN a.said != null THEN a.said
           ELSE b.said END                                                     AS said,
       CASE
           WHEN a.advertiser_lid != null then a.advertiser_lid
           ELSE b.advertiser_lid END                                           AS advertiser_lid,
       dictGet('addotnet.advertiser_dim', 'advertiser_name',
               toUInt64(ABS(advertiser_lid)))                                  AS advertiser_name,
       if(b.category_id != 0, dictGet('addotnet.bidwise_category_dim', 'category', toUInt64(b.category_id)), '')
                                                                               AS category_details,
       b.referrer_domain                                                       AS referrer_domain,
       splitByChar('_', coalesce(said, ''))[1]                                 AS provider_id,
       splitByChar('_', coalesce(said, ''))[2]                                 AS ssaid,
       b.category_id                                                           AS cat_id,
       a.requests                                                              AS requests,
       a.raw_clicks                                                            AS raw_clicks,
       a.paid_clicks                                                           AS paid_clicks,
       a.revenue                                                               AS revenue,
       a.actions_worth                                                         AS actions_worth,
       b.actual_clicks                                                         AS actual_clicks,
       b.actual_revenue                                                        AS actual_cost,
       multiIf(actions_worth > 0, round(actual_cost / actions_worth, 6), NULL) AS cpa
from addotnet.bidwise_ad_event_daily_with_category_new a
         full outer join
     (select toDate(event_date)                                                       as mb_event_date,
             concat(source, '_', COALESCE(CAST(category_id as Nullable(String)), '')) as said,
             advertiser_lid,
             category_id,
             referrer_domain,
             sum(clicks)                                                                 actual_clicks,
             sum(spend)                                                                  actual_revenue
      from addotnet.bidwise_mb_stats
      where event_date >= '2021-04-08'
      group by mb_event_date, said, advertiser_lid, category_id, referrer_domain ) b on
             b.mb_event_date = a.event_date and lower(b.said) = lower(a.said) and b.advertiser_lid = a.advertiser_lid;
