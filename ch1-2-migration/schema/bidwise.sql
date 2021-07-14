DROP TABLE IF EXISTS test.bidwise_ad_event_daily;

CREATE VIEW test.bidwise_ad_event_daily
AS
SELECT m.event_date                  AS event_date,
       m.said_internal               AS said,
       m.advertiser_lid              AS advertiser_lid,
       SUM(m.requests_internal)      AS requests,
       SUM(m.raw_clicks_internal)    AS raw_clicks,
       SUM(m.paid_clicks_internal)   AS paid_clicks,
       SUM(m.revenue_internal)       AS revenue,
       sum(m.actions_worth_internal) AS actions_worth
FROM (SELECT c.raw_clicks                                        AS raw_clicks_internal,
             c.paid_clicks                                       AS paid_clicks_internal,
             c.revenue                                           AS revenue_internal,
             c.actions_worth                                     AS actions_worth_internal,
             0                                                   AS requests_internal,
             a.url,
             substring(extractURLParameter(a.url, 'q'), 4)       AS query_internal,
             COALESCE(concat(said, '_', query_internal), c.said) AS said_internal,
             COALESCE(c.event_date, a.event_date)                AS event_date,
             c.advertiser_lid                                    AS advertiser_lid
      FROM addotnet.ad_event AS c
               LEFT JOIN (SELECT *
                          FROM addotnet.ad_event
                          WHERE (affiliate_account_lid = -8280958097931867273)
                            AND ((event_date >= '2020-08-31') AND (event_date <= '2020-09-14'))
                            AND (event_type = 'request')) AS a ON c.uuid = a.uuid
      WHERE (c.affiliate_account_lid = -8280958097931867273)
        AND ((c.event_date >= '2020-08-31') AND (c.event_date <= '2020-09-14'))
        AND (c.event_type IN ('click', 'action'))
      UNION ALL
      SELECT 0                                                            AS raw_clicks_internal,
             0                                                            AS paid_clicks_internal,
             0                                                            AS revenue_internal,
             0                                                            AS actions_worth_internal,
             requests                                                     AS requests_internal,
             r.url                                                        AS url,
             substring(extractURLParameter(r.url, 'q'), 4)                AS query_internal,
             concat(said, '_', query_internal)                            AS said_internal,
             r.event_date                                                 AS event_date,
             dictGet('addotnet.aid_to_advertiser_dim', 'advertiser_lid',
                     tuple(COALESCE(extractURLParameter(r.url, 'aid'),''))) as advertiser_lid
      FROM addotnet.ad_event AS r
      WHERE (r.affiliate_account_lid = -8280958097931867273)
        AND ((r.event_date >= '2020-08-31') AND (r.event_date <= '2020-09-14'))
        AND (r.event_type = 'request')) AS m
GROUP BY m.event_date, m.said_internal, m.advertiser_lid;

DROP TABLE IF EXISTS test.bidwise_ad_event_daily_with_category;
CREATE VIEW test.bidwise_ad_event_daily_with_category
as
select m.event_date                  AS event_date,
       m.said_internal               AS said,
       m.category_internal           AS category_id,
       m.advertiser_lid              AS advertiser_lid,
       sum(m.requests_internal)      AS requests,
       sum(m.raw_clicks_internal)    AS raw_clicks,
       sum(m.paid_clicks_internal)   AS paid_clicks,
       sum(m.revenue_internal)       AS revenue,
       sum(m.actions_worth_internal) AS actions_worth
FROM (
         (SELECT c.raw_clicks                         as raw_clicks_internal,
                 c.paid_clicks                        as paid_clicks_internal,
                 c.revenue                            as revenue_internal,
                 c.actions_worth                      AS actions_worth_internal,
                 0                                    as requests_internal,
                 a.cat,
                 CAST(CASE WHEN a.cat is null or a.cat = '' or not match(cat, '^[0-9]*$') THEN '0' else a.cat end  as UInt64) as category_internal,
                 said                                 as said_internal,
                 COALESCE(c.event_date, a.event_date) as event_date,
                 c.advertiser_lid                     as advertiser_lid
          FROM addotnet.ad_event c
                   LEFT OUTER JOIN
               (SELECT event_date, url, uuid, extractURLParameter(url, 'cat') as cat
                FROM addotnet.ad_event
                where affiliate_account_lid = -8280958097931867273
                  and event_date >= '2020-10-08'
                  and event_type = 'request') a
               ON (c.uuid = a.uuid)
          where c.affiliate_account_lid = -8280958097931867273
            and c.event_date >= '2020-10-08'
            and c.event_type in ('click', 'action'))
         UNION ALL
         (SELECT 0                                  as raw_clicks_internal,
                 0                                  as paid_clicks_internal,
                 0                                  as revenue_internal,
                 0                                  AS actions_worth_internal,
                 requests                           as requests_internal,
                 extractURLParameter(r.url, 'cat')  as cat,
                 CAST(CASE WHEN cat is null or cat = '' or not match(cat, '^[0-9]*$') THEN '0' else cat end  as UInt64) as category_internal,
                 said                               as said_internal,
                 r.event_date                       as event_date,
                 dictGet('addotnet.aid_to_advertiser_dim', 'advertiser_lid',
                         tuple(COALESCE(extractURLParameter(r.url, 'aid'),''))) as advertiser_lid
          FROM addotnet.ad_event r
          where r.affiliate_account_lid = -8280958097931867273
            and r.event_date >= '2020-10-08'
            and r.event_type = 'request'
         )
         ) m
group by m.event_date, m.said_internal, m.category_internal,m.advertiser_lid;


drop table if exists test.bidwise_sid_mb_discrepancy_v1;
CREATE VIEW test.bidwise_sid_mb_discrepancy_v1
AS
SELECT *
FROM (
         SELECT event_date,
                said,
                coalesce(sum(requests), 0)    AS requests,
                coalesce(sum(raw_clicks), 0)  AS raw_clicks,
                coalesce(sum(paid_clicks), 0) AS paid_clicks,
                coalesce(sum(revenue), 0)     AS revenue
         FROM test.bidwise_ad_event_daily
         WHERE (event_date >= '2020-08-31')
           AND (event_date <= '2020-09-14')
         GROUP BY event_date, said
         ORDER BY said ASC
         union all
         SELECT event_date,
                said,
                coalesce(sum(requests), 0)    AS requests,
                coalesce(sum(raw_clicks), 0)  AS raw_clicks,
                coalesce(sum(paid_clicks), 0) AS paid_clicks,
                coalesce(sum(revenue), 0)     AS revenue
         FROM addotnet.ad_event_daily
         WHERE (affiliate_account_lid = -8280958097931867273)
           AND (event_date >= '2020-09-15')
           AND (event_date <= '2020-09-17')
         GROUP BY event_date, said
         ORDER BY said ASC) AS temp1
         FULL OUTER JOIN (SELECT CAST(event_date, 'date')     AS event_date,
                                 concat(lower(source), '_',
                                        replaceRegexpAll(trimBoth(splitByChar('>', lower(category))[1]), '[^a-zA-Z0-9]',
                                                         '')) AS said,
                                 coalesce(sum(clicks), 0)     AS mb_clicks,
                                 coalesce(sum(spend), 0)      AS mb_cost
                          FROM test.bidwise_mb_stats
                          GROUP BY event_date, said) AS temp2 USING (event_date, said)
ORDER BY event_date ASC, said ASC;

-- Below is the harris's view which is to be expose on looker
DROP TABLE IF EXISTS addotnet.bidwise_sid_mb_discrepancy_v2;

CREATE VIEW addotnet.bidwise_sid_mb_discrepancy_v2
AS
SELECT event_date,
       said,
       advertiser_lid,
       referrer_domain,
       category_id,
       splitByChar('_', coalesce(said, ''))[1]                             AS provider_id,
       splitByChar('_', coalesce(said, ''))[2]                             AS ssaid,
       mb_cost                                                             AS actual_cost,
       mb_clicks                                                           AS actual_clicks,
       requests,
       raw_clicks,
       paid_clicks,
       revenue,
       actions_worth,
       multiIf(actions_worth > 0, round(mb_cost / actions_worth, 6), NULL) AS cpa
FROM (SELECT event_date,
             said,
             advertiser_lid,
             COALESCE(SUM(requests), 0)                AS requests,
             COALESCE(SUM(raw_clicks), 0)              AS raw_clicks,
             COALESCE(SUM(paid_clicks), 0)             AS paid_clicks,
             COALESCE(SUM(revenue), 0)                 AS revenue,
             COALESCE(ROUND(SUM(actions_worth), 2), 0) AS actions_worth
      FROM test.bidwise_ad_event_daily
      WHERE (event_date >= '2020-08-31')
        AND (event_date <= '2020-09-14')
      GROUP BY event_date, said,advertiser_lid
      ORDER BY said ASC
      UNION ALL
      SELECT event_date,
             said,
             advertiser_lid,
             COALESCE(SUM(requests), 0)      AS requests,
             COALESCE(SUM(raw_clicks), 0)    AS raw_clicks,
             COALESCE(SUM(paid_clicks), 0)   AS paid_clicks,
             COALESCE(SUM(revenue), 0)       AS revenue,
             COALESCE(SUM(actions_worth), 0) AS actions_worth
      FROM addotnet.ad_event_daily
      WHERE (affiliate_account_lid = -8280958097931867273)
        AND (event_date >= '2020-09-15')
      GROUP BY event_date, said,advertiser_lid
      ORDER BY said ASC) AS temp1
         FULL OUTER JOIN (SELECT CAST(event_date, 'date')     AS event_date,
                                 concat(LOWER(source), '_',
                                        replaceRegexpAll(trimBoth(splitByChar('>', LOWER(category))[1]), '[^a-zA-Z0-9]',
                                                         '')) AS said,
                                 COALESCE(SUM(clicks), 0)     AS mb_clicks,
                                 COALESCE(SUM(spend), 0)      AS mb_cost,
                                 referrer_domain,
                                 category_id,
                                 advertiser_lid
                          FROM test.bidwise_mb_stats
                          GROUP BY event_date, said, referrer_domain, category_id,advertiser_lid) AS temp2 USING (event_date, said,advertiser_lid)
ORDER BY event_date ASC, said ASC;

drop table if exists test.bidwise_mb_stats;
CREATE TABLE test.bidwise_mb_stats
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
) ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/qa/tables/bidwise_mb_stats_1/{shard}',
           '{replica}') PARTITION BY (event_date,advertiser_lid) PRIMARY KEY event_date ORDER BY event_date SETTINGS index_granularity = 8192;

drop table if exists test.bidwise_advertisers;
CREATE TABLE test.bidwise_advertisers
(
    `advertiser_name`  String,
    `advertiser_lid`   Int64
) ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/qa/tables/bidwise_advertisers/{shard}',
           '{replica}') ORDER BY tuple() SETTINGS index_granularity = 8192;

--insert into test.bidwise_mb_stats (source, category, clicks, spend, cpc, event_date)
--select source, category, clicks, spend, cpc, event_date
--from test.bidwise_mb_stats_backup1;


DROP TABLE IF EXISTS addotnet.bidwise_sid_mb_discrepancy_category;
CREATE VIEW addotnet.bidwise_sid_mb_discrepancy_category as
SELECT event_date,
       said,
       referrer_domain,
       CAST(coalesce(temp1.category_id, 0) as UInt64)                      as cat_id,
       advertiser_lid,
       dictGet('addotnet.advertiser_dim', 'advertiser_name',
               toUInt64(ABS(advertiser_lid)))                               AS      advertiser_name,
       if(cat_id != 0, dictGet('addotnet.bidwise_category_dim', 'category', toUInt64(cat_id)), '')
                                                                           AS category_details,
       splitByChar('_', coalesce(said, ''))[1]                             AS provider_id,
       splitByChar('_', coalesce(said, ''))[2]                             AS ssaid,
       mb_cost                                                             AS actual_cost,
       mb_clicks                                                           AS actual_clicks,
       requests,
       raw_clicks,
       paid_clicks,
       revenue,
       actions_worth,
       multiIf(actions_worth > 0, round(mb_cost / actions_worth, 6), NULL) AS cpa
FROM (SELECT event_date,
             said,
             CAST(0 as UInt64)                         as category_id,
             advertiser_lid,
             COALESCE(SUM(requests), 0)                AS requests,
             COALESCE(SUM(raw_clicks), 0)              AS raw_clicks,
             COALESCE(SUM(paid_clicks), 0)             AS paid_clicks,
             COALESCE(SUM(revenue), 0)                 AS revenue,
             COALESCE(ROUND(SUM(actions_worth), 2), 0) AS actions_worth
      FROM test.bidwise_ad_event_daily
      WHERE (event_date >= '2020-08-31')
        AND (event_date <= '2020-09-14')
      GROUP BY event_date, said,category_id,advertiser_lid
      ORDER BY said ASC
      UNION ALL
      SELECT event_date,
             said,
             CAST(0 as UInt64)               as category_id,
             dictGet('addotnet.aid_to_advertiser_dim', 'advertiser_lid',tuple(COALESCE(extractURLParameter(url, 'aid'), ''))) as advertiser_lid,
             COALESCE(SUM(requests), 0)      AS requests,
             COALESCE(SUM(raw_clicks), 0)    AS raw_clicks,
             COALESCE(SUM(paid_clicks), 0)   AS paid_clicks,
             COALESCE(SUM(revenue), 0)       AS revenue,
             COALESCE(SUM(actions_worth), 0) AS actions_worth
      FROM addotnet.ad_event
      WHERE (affiliate_account_lid = -8280958097931867273)
        AND (event_date >= '2020-09-15')
        AND (event_date <= '2020-10-07')
      GROUP BY event_date, said,category_id,advertiser_lid
      ORDER BY said ASC
      UNION ALL
      SELECT event_date,
             said,
             category_id,
             advertiser_lid,
             COALESCE(SUM(requests), 0)                AS requests,
             COALESCE(SUM(raw_clicks), 0)              AS raw_clicks,
             COALESCE(SUM(paid_clicks), 0)             AS paid_clicks,
             COALESCE(SUM(revenue), 0)                 AS revenue,
             COALESCE(ROUND(SUM(actions_worth), 2), 0) AS actions_worth
      FROM test.bidwise_ad_event_daily_with_category
      WHERE (event_date >= '2020-10-08')
      GROUP BY event_date, said, category_id,advertiser_lid
      ORDER BY said ASC) AS temp1
         FULL OUTER JOIN (SELECT CAST(event_date, 'date')     AS event_date,
                                 concat(LOWER(source), '_',
                                        replaceRegexpAll(trimBoth(splitByChar('>', LOWER(category))[1]), '[^a-zA-Z0-9]',
                                                         '')) AS said,
                                 COALESCE(SUM(clicks), 0)     AS mb_clicks,
                                 COALESCE(SUM(spend), 0)      AS mb_cost,
                                 referrer_domain,
                                 category_id,
                                 advertiser_lid
                          FROM test.bidwise_mb_stats
                          GROUP BY event_date, said, referrer_domain, category_id,advertiser_lid) AS temp2
                         USING (event_date, said, category_id,advertiser_lid)
ORDER BY event_date ASC, said ASC;


-- dictionary
drop table if EXISTS test.bidwise_category_view;
create view test.bidwise_category_view as
select category_id, category
from test.bidwise_mb_stats
where category_id is not null and category_id<>0
group by category_id, category;


-- OUTGOING data

DROP TABLE IF EXISTS bidwise.bidwise_category_status_log;
CREATE TABLE bidwise.bidwise_category_status_log
(
    `source`       String,
    `ssaid`        String,
    `category_id`  Nullable(UInt64),
    `is_paused`    Int8,
    `aaa_username` String,
    `timestamp`    Int64,
    `event_date`   Date
) ENGINE = MergeTree
      PARTITION BY event_date PRIMARY KEY (source, ssaid) ORDER BY (source, ssaid) SETTINGS index_granularity = 8192;

INSERT INTO bidwise.bidwise_category_status_log (source, ssaid, category_id, is_paused, aaa_username, timestamp,
                                                 event_date)
values ('testsource3', 'electronics', 123, 1, 'archan', now(), toDate(now()));

--ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/qa/tables/bidwise_mb_stats/{shard}',
--         '{replica}') PARTITION BY (event_date) PRIMARY KEY event_date ORDER BY event_date SETTINGS index_granularity = 8192;

drop table if exists bidwise.bidwise_category_status;
create view bidwise.bidwise_category_status
as
select source, ssaid, category_id, b.is_paused
from (select source, ssaid, category_id, max(timestamp) as timestamp
      from bidwise.bidwise_category_status_log
      group by source, ssaid, category_id) a
         inner join bidwise.bidwise_category_status_log b
                    on b.timestamp = a.timestamp
order by timestamp,source, ssaid, category_id;



