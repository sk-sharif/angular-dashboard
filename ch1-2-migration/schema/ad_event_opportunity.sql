--clickhouse
DROP TABLE IF EXISTS addotnet.ad_event_opportunity_daily;

CREATE TABLE addotnet.ad_event_opportunity_daily
(
    event_date            Date   DEFAULT toDate('1975-01-01'),
    affiliate_account_lid Int64  DEFAULT toInt64(0),
    affiliate_account_hid Int64  DEFAULT toInt64(0),
    search_keyword        String DEFAULT '',
    sid                   Int64  DEFAULT toInt64(0),
    requests              Nullable(UInt64)
)
    ENGINE = ReplicatedSummingMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/ad_event_opportunity_daily/{shard}',
             '{replica}') PARTITION BY (event_date) PRIMARY KEY
        (event_date, affiliate_account_lid, affiliate_account_hid, search_keyword, sid)
        ORDER BY (event_date, affiliate_account_lid, affiliate_account_hid, search_keyword,
                  sid) SETTINGS index_granularity = 8192;


DROP TABLE IF EXISTS etl.ad_event_opportunity_daily_mat;
CREATE MATERIALIZED VIEW etl.ad_event_opportunity_daily_mat to addotnet.ad_event_opportunity_daily
AS
select event_date,
       coalesce(ad_event.affiliate_account_lid, 0) affiliate_account_lid,
       coalesce(ad_event.affiliate_account_hid, 0) affiliate_account_hid,
       coalesce(ad_event.search_keyword, '')       search_keyword,
       sid,
       requests
from addotnet.ad_event
where event_type = 'request';

DROP TABLE IF EXISTS addotnet.ad_event_opportunity_view;

CREATE VIEW addotnet.ad_event_opportunity_view
AS
SELECT event_date,
       affiliate_account_lid,
       affiliate_account_hid,
       search_keyword,
       sid,
       requests,
       dictGet('addotnet.affiliate_account_dim', 'name',
               toUInt64(ABS(affiliate_account_lid))) AS affiliate_account_name,
       dictGet('addotnet.publisher_category_dim', 'publisher_category',
               toUInt64(ABS(sid)))                   AS publisher_category
FROM addotnet.ad_event_opportunity_daily;


--postgres

DROP FOREIGN TABLE addotnet.ad_event_opportunity_view;

CREATE FOREIGN TABLE addotnet.ad_event_opportunity_view
(
    event_date             date,
    affiliate_account_lid  bigint,
    affiliate_account_hid  bigint,
    search_keyword         text,
    sid                    bigint,
    requests               bigint,
    affiliate_account_name text,
    publisher_category text
) SERVER clickhouse_svr;

