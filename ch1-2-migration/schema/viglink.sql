CREATE TABLE
    addotnet.ad_event_viglink
(
    event_date               Date,
    domain                   Nullable(String),
    rtb_partner_id           Nullable(String),
    old_convert_partner_id   Nullable(String),
    rtb_ads_returned         UInt64,
    old_convert_ads_returned UInt64,
    rtb_bid_floor            Nullable(Float64),
    old_convert_requests     UInt64,
    max_bid_returned         Nullable(Float64),
    bid_floor                Nullable(Float64),
    uuid                     Nullable(String),
    rtb_max_bid_returned     Nullable(Float64),
    sid                      Int64,
    raw_clicks               Nullable(Int64),
    rtb_said                 Nullable(String),
    convert_said             Nullable(String)
)
    ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/addotnet/prod/tables/ad_event_viglink/{shard}',
             '{replica}')
        PARTITION BY (event_date)
        PRIMARY KEY (event_date, sid)
        ORDER BY (event_date, sid)
        SETTINGS index_granularity = 8192;


ALTER TABLE addotnet.ad_event_viglink
    DROP PARTITION ('2020-11-16');
INSERT INTO addotnet.ad_event_viglink
SELECT event_date,
       domain,
       rtb_partner_id,
       old_convert_partner_id,
       rtb_ads_returned,
       old_convert_ads_returned,
       rtb_bid_floor,
       old_convert_requests,
       max_bid_returned,
       bid_floor,
       a.uuid,
       rtb_max_bid_returned,
       sid,
       raw_clicks,
       rtb_said,
       convert_said

FROM (
         SELECT coalesce(CASE
                             WHEN rtb_requests.event_date = '0000-00-00' THEN old_requests.event_date
                             else rtb_requests.event_date END) event_date,
                rtb_requests.domain           AS               domain,
                rtb_requests.partner_id       AS               rtb_partner_id,
                old_requests.partner_id       AS               old_convert_partner_id,
                rtb_requests.num_ads_returned AS               rtb_ads_returned,
                old_requests.num_ads_returned AS               old_convert_ads_returned,
                rtb_requests.bid_floor        AS               rtb_bid_floor,
                old_requests.num_ads_returned AS               old_convert_requests,
                old_requests.max_bid_returned AS               max_bid_returned,
                rtb_requests.bid_floor        AS               bid_floor,
                rtb_requests.uuid             AS               uuid,
                rtb_requests.rtb_max_bid_returned,
                old_requests.sid              AS               sid,
                rtb_requests.raw_said         as               rtb_said,
                old_requests.raw_said         as               convert_said
         FROM (
                  SELECT event_date,
                         partner_id,
                         raw_said,
                         sid,
                         uuid,
                         substr(search_keyword, position(search_keyword, '_') + 1, length(search_keyword)) AS domain,
                         max(CASE
                                 WHEN length(ad_returned[1]) > 0 and event_type = 'ad_return' THEN 1
                                 else 0 END)                                                               AS num_ads_returned,
                         max(CAST(extract(url_parameters_json, '.*"convert_bid_floor":"([0-9.]+)".*'),
                                  'Nullable(Float64)'))                                                    AS bid_floor,
                         max(if(isNull(ad_returns), NULL, max_internal_net_bid))                           AS rtb_max_bid_returned
                  FROM addotnet.ad_event
                  WHERE event_date = '2020-11-16'
                    AND (sid = 11172415)
                    AND (affiliate_account_lid = -7992873158829125019)
                    AND (affiliate_account_hid = -6635096376100370648)
                    AND (url LIKE '/openrtb%')
                  GROUP BY event_date,
                           partner_id,
                           uuid,
                           raw_said,
                           sid,
                           substr(search_keyword, position(search_keyword, '_') + 1, length(search_keyword))
                  ) AS rtb_requests
                  FULL OUTER JOIN
              (
                  SELECT event_date,
                         raw_said,
                         sid,
                         substr(partner_id, 1, 16) AS partner_id,
                         max(CASE
                                 WHEN length(ad_returned[1]) > 0 and event_type = 'ad_return' THEN 1
                                 else 0 END)       AS num_ads_returned,
                         max(max_internal_net_bid) AS max_bid_returned
                  FROM addotnet.ad_event
                  WHERE event_date = '2020-11-16'
                    AND (sid != 11172415)
                    AND (affiliate_account_lid = -7992873158829125019)
                    AND (affiliate_account_hid = -6635096376100370648)
                    AND ((url LIKE '/ads%') OR (url LIKE '/xml-search%') OR (url LIKE '/direct%'))
                  GROUP BY event_date,
                           raw_said,
                           sid,
                           partner_id
                  ) AS old_requests ON (rtb_requests.partner_id = old_requests.partner_id) AND
                                       (rtb_requests.event_date = old_requests.event_date)
         ) a
         LEFT JOIN
     (SELECT uuid,
             max(if(uuid_click IS NOT NULL, 1, 0)) AS raw_clicks
      FROM addotnet.ad_event
      WHERE event_date = '2020-11-16'
        AND sid = 11172415
      GROUP BY uuid) rtb_clicks ON a.uuid = rtb_clicks.uuid;



CREATE TABLE addotnet.viglink_optimize_direct_advertiser
(
    `name` String
) ENGINE = ReplicatedMergeTree(
           '/clickhouse/{cluster}/addotnet/prod/tables/viglink_optimize_direct_advertiser/{shard}',
           '{replica}') PRIMARY KEY name ORDER BY name SETTINGS index_granularity = 8192;

CREATE TABLE addotnet.viglink_optimize_oos_url
(
    `url` String
) ENGINE = ReplicatedMergeTree(
           '/clickhouse/{cluster}/addotnet/prod/tables/viglink_optimize_oos_url/{shard}',
           '{replica}') PRIMARY KEY url ORDER BY url SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS addotnet.viglink_optimize_request_ad_returns;
CREATE VIEW addotnet.viglink_optimize_request_ad_returns as
SELECT event_date,
       search_uuid,
       mark_url,
       firstSignificantSubdomain(mark_url)                                                                       AS domain,
       CASE
           WHEN replaceAll(arrayJoin(JSONExtractArrayRaw(COALESCE(viglink_alt_mark_urls, '[""]'))), '"', '') = ''
               then null
           else replaceAll(arrayJoin(JSONExtractArrayRaw(COALESCE(viglink_alt_mark_urls, '[""]'))), '"',
                           '') end                                                                               AS viglink_alt_mark_url,
       firstSignificantSubdomain(viglink_alt_mark_url)                                                           AS viglink_alt_domain,
       null                                                                                                         addotnet_alt_mark_url,
       null                                                                                                         addotnet_alt_domain,
       CASE
           WHEN domain IN ('walmart', 'amazon') and
                viglink_alt_domain IN (select name from viglink_optimize_direct_advertiser) THEN search_uuid
           else null end                                                                                            viglink_amzwal_direct_adv_alt_search_uuid,
       null                                                                                                         addotnet_amzwal_direct_adv_alt_search_uuid,
       winning_mark_url                                                                                             ad_return_mark_url,
       firstSignificantSubdomain(winning_mark_url)                                                                  ad_return_domain,
       winning_search_uuid                                                                                          ad_return_search_uuid,
       optimize_search_uuids,
       CASE
           WHEN viglink_alt_mark_url IN (select url from viglink_optimize_oos_url) THEN search_uuid
           else null end                                                                                            viglink_oos_search_uuid,
       null                                                                                                         addotnet_oos_search_uuid
FROM viglink_optimize_info
UNION ALL
SELECT event_date,
       search_uuid,
       mark_url,
       firstSignificantSubdomain(mark_url) AS           domain,
       null                                as           viglink_alt_mark_url,
       null                                AS           viglink_alt_domain,
       CASE
           WHEN replaceAll(arrayJoin(JSONExtractArrayRaw(COALESCE(our_alt_mark_urls, '[""]'))), '"', '') = '' then null
           else replaceAll(arrayJoin(JSONExtractArrayRaw(COALESCE(our_alt_mark_urls, '[""]'))), '"',
                           '') end                      addotnet_alt_mark_url,
       firstSignificantSubdomain(addotnet_alt_mark_url) addotnet_alt_domain,
       null                                as           viglink_amzwal_direct_adv_alt_search_uuid,
       CASE
           WHEN domain IN ('walmart', 'amazon') and
                addotnet_alt_domain IN (select name from viglink_optimize_direct_advertiser) THEN search_uuid
           else null end                                addotnet_amzwal_direct_adv_alt_search_uuid,
       winning_mark_url                                 ad_return_mark_url,
       firstSignificantSubdomain(winning_mark_url)      ad_return_domain,
       winning_search_uuid                              ad_return_search_uuid,
       optimize_search_uuids,
       null                                             viglink_oos_search_uuid,
       CASE
           WHEN addotnet_alt_mark_url IN (select url from viglink_optimize_oos_url) THEN search_uuid
           else null end                                addotnet_oos_search_uuid
FROM viglink_optimize_info;

DROP TABLE test.product_mapping_raw_url_to_identifiers_impala;

CREATE TABLE test.product_mapping_raw_url_to_identifiers_impala
(
    dt      String,
    url    String,
    asin   Nullable(String),
    upc    Nullable(String),
    volume Nullable(String)
) ENGINE = HDFS(
           'hdfs://nn1.data.int.dc1.ad.net:8020/tmp/impala_to_ch/product_mapping_raw_url_to_identifiers/*',
           'Parquet');


CREATE TABLE addotnet.product_mapping_raw_url_to_identifiers
(
    dt     Date,
    url    String,
    asin   Nullable(String),
    upc    Nullable(String),
    volume Nullable(String)
) ENGINE = ReplicatedMergeTree(
           '/clickhouse/{cluster}/addotnet/prod/tables/product_mapping_raw_url_to_identifiers/{shard}',
           '{replica}') PARTITION BY dt PRIMARY KEY url ORDER BY url SETTINGS index_granularity = 8192;

insert into addotnet.product_mapping_raw_url_to_identifiers select dt,url,asin,upc,volume from test.product_mapping_raw_url_to_identifiers_impala where dt='2020-12-07';

DROP TABLE IF EXISTS test.product_mapping_offers_impala;
CREATE TABLE test.product_mapping_offers_impala
(
    dt                String,
    upc               String,
    title             String,
    merchant          Nullable(String),
    sub_merchant      Nullable(String),
    price             Nullable(Float64),
    raw_offer_url     Nullable(String),
    offer_url         Nullable(String),
    cleaned_offer_url Nullable(String),
    whitelisted       Nullable(Int8)
)ENGINE = HDFS(
           'hdfs://nn1.data.int.dc1.ad.net:8020/tmp/impala_to_ch/product_mapping_offers/*',
           'Parquet');


DROP TABLE IF EXISTS addotnet.product_mapping_offers;
CREATE TABLE addotnet.product_mapping_offers
(
    dt                Date,
    upc               String,
    title             String,
    merchant          Nullable(String),
    sub_merchant      Nullable(String),
    price             Nullable(Float64),
    raw_offer_url     Nullable(String),
    offer_url         Nullable(String),
    cleaned_offer_url Nullable(String),
    whitelisted       Nullable(Int8)
)ENGINE = ReplicatedMergeTree(
          '/clickhouse/{cluster}/addotnet/prod/tables/product_mapping_offers/{shard}',
          '{replica}') PARTITION BY dt PRIMARY KEY (upc,title) ORDER BY (upc,title) SETTINGS index_granularity = 8192;


CREATE VIEW addotnet.viglink_min_bid_report AS
SELECT event_date,
       advertiser_name,
       advertiser_lid,
       advertiser_hid,
       adgroup_name,
       said,
       JSONExtractString(url_parameters_json, 'convert_pub_name') AS  pub_name,
       JSONExtractString(url_parameters_json, 'convert_bid_floor') AS bid_floor,
       search_net_bid_price,
       sum(ad_returns) AS                                             total_ad_returns,
       sum(multiIf(search_net_bid_price >= CAST(bid_floor, 'Nullable(Float64)'), ad_returns,
                   0)) AS                                             ad_returns_above_floor,
       sum(multiIf(search_net_bid_price < CAST(bid_floor, 'Nullable(Float64)'), ad_returns,
                   0)) AS                                             ad_returns_below_floor,
       sum(raw_clicks) AS                                             raw_clicks,
       sum(paid_clicks) AS                                            paid_clicks,
       sum(round(revenue, 2)) AS                                      total_revenue,
       sum(actions_worth) AS                                          total_actions_worth
FROM addotnet.ad_event_view
WHERE (sid = 11172415)
  AND (advertiser_name != 'OdessaGaming.com')
GROUP BY event_date, advertiser_name, advertiser_lid, advertiser_hid, adgroup_name, said, pub_name, bid_floor,
         search_net_bid_price;