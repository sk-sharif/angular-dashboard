drop table if exists test.sid_said_daily_performance_2;
CREATE VIEW test.sid_said_daily_performance_2 as
SELECT event_date,
       sid,
       said,
       final_raw_keyword           AS final_raw_keyword,
       matched_keyword,
       country_code,
       SUM(intarnal_requests)      AS total_requests,
       SUM(intarnal_raw_clicks)    AS total_raw_clicks,
       SUM(intarnal_paid_clicks)   AS total_paid_clicks,
       SUM(intarnal_revenue)       AS total_revenue,
       SUM(intarnal_actions_worth) AS total_actions_worth
FROM (
      select r.event_date,
             r.sid,
             r.said,
             r.intarnal_raw_keyword AS final_raw_keyword,
             c.matched_keyword,
             r.country_code,
             r.intarnal_requests    AS intarnal_requests,
             0                      AS intarnal_raw_clicks,
             0                      AS intarnal_paid_clicks,
             0                      AS intarnal_revenue,
             0                      AS intarnal_actions_worth
      FROM (SELECT event_date,
                   sid,
                   said,
                   raw_keyword AS intarnal_raw_keyword,
                   country_code,
                   requests    as intarnal_requests,
                   uuid
            FROM addotnet.ad_event
            WHERE ((event_date >= '2020-11-20') AND (event_date <= '2020-11-30'))
              AND (sid IN (11164440, 11165151, 11165150, 11168258, 11170798, 11165399))
              AND (event_type IN ('request')) ) r
               LEFT OUTER JOIN
           (select matched_keyword, uuid
            FROM addotnet.ad_event
            WHERE ((event_date >= '2020-11-20') AND (event_date <= '2020-11-30'))
              AND (sid IN (11164440, 11165151, 11165150, 11168258, 11170798, 11165399))
              AND (event_type IN ('click'))
            group by matched_keyword, uuid) c on c.uuid = r.uuid

      UNION ALL

      SELECT event_date,
             sid,
             said,
             search_keyword AS final_raw_keyword,
             matched_keyword,
             country_code,
             0              AS requests,
             raw_clicks     AS intarnal_raw_clicks,
             paid_clicks    AS intarnal_paid_clicks,
             revenue        AS intarnal_revenue,
             actions_worth  AS intarnal_actions_worth
      FROM addotnet.ad_event
      WHERE ((event_date >= '2020-11-20') AND (event_date <= '2020-11-30'))
        AND (sid IN (11164440, 11165151, 11165150, 11168258, 11170798, 11165399))
        AND (event_type IN ('click', 'action'))
         )
GROUP BY event_date, said, sid, final_raw_keyword, matched_keyword, country_code;


drop table test.sid_said_daily_performance_parquet;
CREATE TABLE test.sid_said_daily_performance_parquet
(
    `event_date`          String,
    `sid`                 Int64,
    `said`                String,
    `raw_keyword`         Nullable(String),
    `matched_keyword`     Nullable(String),
    `country_code`        Nullable(String),
    `total_requests`      Nullable(Int64),
    `total_raw_clicks`    Nullable(Int64),
    `total_paid_clicks`   Nullable(Int64),
    `total_revenue`       Nullable(Float64),
    `total_actions_worth` Nullable(Float64)
) ENGINE = HDFS(
           'hdfs://nn1.data.int.dc1.ad.net:8020/user/hive/warehouse/test.db/sid_said_keyword_requests/data_nov_26_30',
           'Parquet');


insert into test.sid_said_daily_performance_parquet
select event_date,
       sid,
       said,
       final_raw_keyword,
       matched_keyword,
       country_code,
       total_requests,
       total_raw_clicks,
       total_paid_clicks,
       total_revenue,
       total_actions_worth
from test.sid_said_daily_performance_2
where event_date between '2020-11-26' and '2020-11-30';

--hdfs dfs -mkdir /user/hive/warehouse/test.db/sid_said_keyword_request


CREATE EXTERNAL
    TABLE test.sid_said_keyword_requests
(
    event_date          STRING COMMENT 'Inferred from Parquet file.',
    sid                 BIGINT COMMENT 'Inferred from Parquet file.',
    said                STRING COMMENT 'Inferred from Parquet file.',
    raw_keyword         STRING COMMENT 'Inferred from Parquet file.',
    matched_keyword     STRING,
    country_code        STRING,
    total_requests      BIGINT COMMENT 'Inferred from Parquet file.',
    total_raw_clicks    BIGINT COMMENT 'Inferred from Parquet file.',
    total_paid_clicks   BIGINT COMMENT 'Inferred from Parquet file.',
    total_revenue       DOUBLE COMMENT 'Inferred from Parquet file.',
    total_actions_worth DOUBLE COMMENT 'Inferred from Parquet file.'
) STORED AS PARQUET LOCATION 'hdfs://nn1.data.int.dc1.ad.net:8020/user/hive/warehouse/test.db/sid_said_keyword_requests' TBLPROPERTIES ('COLUMN_STATS_ACCURATE'='false',  'numRows'='-1', 'rawDataSize'='-1');
