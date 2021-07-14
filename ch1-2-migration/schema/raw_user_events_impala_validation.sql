drop table if exists test.raw_user_events_json;
CREATE EXTERNAL TABLE test.raw_user_events_json (
 json_data string)
PARTITIONED BY (   dt STRING,    hour_id STRING )
ROW FORMAT DELIMITED
STORED AS TEXTFILE LOCATION 'hdfs://nn1.data.int.dc1.ad.net:8020/topics/data_aug_user_profile_json_logs';

MSCK REPAIR TABLE test.raw_user_events_json;

drop view if exists test.raw_user_event_view;

create view test.raw_user_event_view AS
SELECT get_json_object(json_data, '$.timeStamp')                        as `timeStamp`,
       get_json_object(json_data, '$.eventName')                        as `eventName`,
       get_json_object(json_data, '$.userId')                           as `userId`,
       get_json_object(json_data, '$.src_url')                          as `src_url`,
       get_json_object(json_data, '$.ip')                               as `ip`,
       get_json_object(json_data, '$.ua')                               as `ua`,
       get_json_object(json_data, '$.refererDomain')                    as `refererDomain`,
       get_json_object(json_data, '$.eventParams.sid')                  as `eventParams.sid`,
       get_json_object(json_data, '$.eventParams.said')                 as `eventParams.said`,
       get_json_object(json_data, '$.eventParams.kw')                   as `eventParams.kw`,
       get_json_object(json_data, '$.eventParams.adgroupId')            as `eventParams.adgroupId`,
       get_json_object(json_data, '$.eventParams.actionWorth')          as `eventParams.actionWorth`,
       get_json_object(json_data, '$.eventParams.adnetClickId')         as `eventParams.adnetClickId`,
       get_json_object(json_data, '$.eventParams.external_referrer')    as `eventParams.external_referrer`,
       get_json_object(json_data, '$.eventParams.push_user_id')         as `eventParams.push_user_id`,
       get_json_object(json_data, '$.eventParams.impression_id')        as `eventParams.impression_id`,
       get_json_object(json_data, '$.eventParams.kw_rank')              as `eventParams.kw_rank`,
       get_json_object(json_data, '$.eventParams.ad_unit_name')         as `eventParams.ad_unit_name`,
       get_json_object(json_data, '$.eventParams.source_url')           as `eventParams.source_url`,
       get_json_object(json_data, '$.eventParams.event_uuid')           as `eventParams.event_uuid`,
       get_json_object(json_data, '$.eventParams.country')              as `eventParams.country`,
       get_json_object(json_data, '$.eventParams.dst_url')              as `eventParams.dst_url`,
       get_json_object(json_data, '$.eventParams.error')                as `eventParams.error`,
       get_json_object(json_data, '$.eventParams.js_version')           as `eventParams.js_version`,
       get_json_object(json_data, '$.eventParams.ext_version')          as `eventParams.ext_version`,
       get_json_object(json_data, '$.eventParams.ad_unit_updated_type') as `eventParams.ad_unit_updated_type`,
       get_json_object(json_data, '$.eventParams.ad_unit_updated_by')   as `eventParams.ad_unit_updated_by`,
       get_json_object(json_data, '$.eventParams.ad_unit_old_version')  as `eventParams.ad_unit_old_version`,
       get_json_object(json_data, '$.eventParams.ad_unit_new_version')  as `eventParams.ad_unit_new_version`,
       get_json_object(json_data, '$.eventParams.affiliate_id')         as `eventParams.affiliate_id`,
       get_json_object(json_data, '$.eventParams.sub_affiliate_id')     as `eventParams.sub_affiliate_id`,
       get_json_object(json_data, '$.eventParams.pre_event_uuid')       as `eventParams.pre_event_uuid`,
       get_json_object(json_data, '$.eventParams.display_url')          as `eventParams.display_url`,
       get_json_object(json_data, '$.eventParams.ad_unit_version')      as `eventParams.ad_unit_version`,
       get_json_object(json_data, '$.eventParams.uuid')                 as `eventParams.uuid`,
       dt,
       hour_id
from test.raw_user_events_json
union all
SELECT get_json_object(json_data, '$.timeStamp')                        AS `timeStamp`,
       'presearchKeywordImpression'                        AS `eventName`,
       get_json_object(json_data, '$.userId')                           AS `userId`,
       get_json_object(json_data, '$.src_url')                          as `src_url`,
       get_json_object(json_data, '$.ip')                               AS `ip`,
       get_json_object(json_data, '$.ua')                               AS `ua`,
       get_json_object(json_data, '$.refererDomain')                    AS `refererDomain`,
       get_json_object(json_data, '$.eventParams.sid')                  AS `eventParams.sid`,
       get_json_object(json_data, '$.eventParams.said')                 AS `eventParams.said`,
       temp.extracted_kw                                                AS `eventParams.kw`,
       get_json_object(json_data, '$.eventParams.adgroupId')            as `eventParams.adgroupId`,
       get_json_object(json_data, '$.eventParams.actionWorth')          AS `eventParams.actionWorth`,
       get_json_object(json_data, '$.eventParams.adnetClickId')         AS `eventParams.adnetClickId`,
       get_json_object(json_data, '$.eventParams.external_referrer')    AS `eventParams.external_referrer`,
       get_json_object(json_data, '$.eventParams.push_user_id')         AS `eventParams.push_user_id`,
       get_json_object(json_data, '$.eventParams.impression_id')        AS `eventParams.impression_id`,
       (temp.kw_rank + 1)                                               AS `eventParams.kw_rank`,
       get_json_object(json_data, '$.eventParams.ad_unit_name')         AS `eventParams.ad_unit_name`,
       get_json_object(json_data, '$.eventParams.source_url')           AS `eventParams.source_url`,
       get_json_object(json_data, '$.eventParams.event_uuid')           AS `eventParams.event_uuid`,
       get_json_object(json_data, '$.eventParams.country')              AS `eventParams.country`,
       get_json_object(json_data, '$.eventParams.dst_url')              AS `eventParams.dst_url`,
       get_json_object(json_data, '$.eventParams.error')                AS `eventParams.error`,
       get_json_object(json_data, '$.eventParams.js_version')           AS `eventParams.js_version`,
       get_json_object(json_data, '$.eventParams.ext_version')          AS `eventParams.ext_version`,
       get_json_object(json_data, '$.eventParams.ad_unit_updated_type') AS `eventParams.ad_unit_updated_type`,
       get_json_object(json_data, '$.eventParams.ad_unit_updated_by')   AS `eventParams.ad_unit_updated_by`,
       get_json_object(json_data, '$.eventParams.ad_unit_old_version')  AS `eventParams.ad_unit_old_version`,
       get_json_object(json_data, '$.eventParams.ad_unit_new_version')  AS `eventParams.ad_unit_new_version`,
       get_json_object(json_data, '$.eventParams.affiliate_id')         AS `eventParams.affiliate_id`,
       get_json_object(json_data, '$.eventParams.sub_affiliate_id')     AS `eventParams.sub_affiliate_id`,
       get_json_object(json_data, '$.eventParams.pre_event_uuid')       AS `eventParams.pre_event_uuid`,
       get_json_object(json_data, '$.eventParams.display_url')          AS `eventParams.display_url`,
       get_json_object(json_data, '$.eventParams.ad_unit_version')      AS `eventParams.ad_unit_version`,
       get_json_object(json_data, '$.eventParams.uuid')                 AS `eventParams.uuid`,
       dt,
       hour_id
FROM test.raw_user_events_json LATERAL VIEW POSEXPLODE(split(regexp_extract(get_json_object(json_data, '$.eventParams.kw'),'^\\["(.*)\\"]$',1),'","')) temp AS kw_rank,extracted_kw
WHERE get_json_object(json_data, '$.eventName') = 'presearchShown';


drop table if exists test.raw_user_events_json_raw_logs;
CREATE EXTERNAL TABLE test.raw_user_events_json_raw_logs (
 json_data string)
PARTITIONED BY (   dt STRING,    hour_id STRING )
ROW FORMAT DELIMITED
STORED AS TEXTFILE LOCATION 'hdfs://nn1.data.int.dc1.ad.net:8020/topics/raw/data_aug_user_profile_json_logs';

MSCK REPAIR TABLE test.raw_user_events_json_raw_logs;

drop view if exists test.raw_user_event_raw_logs_view;
create view test.raw_user_event_raw_logs_view AS
SELECT get_json_object(json_data, '$.timeStamp')                        as `timeStamp`,
       get_json_object(json_data, '$.eventName')                        as `eventName`,
       get_json_object(json_data, '$.userId')                           as `userId`,
       get_json_object(json_data, '$.src_url')                          as `src_url`,
       get_json_object(json_data, '$.ip')                               as `ip`,
       get_json_object(json_data, '$.ua')                               as `ua`,
       get_json_object(json_data, '$.refererDomain')                    as `refererDomain`,
       get_json_object(json_data, '$.eventParams.sid')                  as `eventParams.sid`,
       get_json_object(json_data, '$.eventParams.said')                 as `eventParams.said`,
       get_json_object(json_data, '$.eventParams.kw')                   as `eventParams.kw`,
       get_json_object(json_data, '$.eventParams.adgroupId')            as `eventParams.adgroupId`,
       get_json_object(json_data, '$.eventParams.actionWorth')          as `eventParams.actionWorth`,
       get_json_object(json_data, '$.eventParams.adnetClickId')         as `eventParams.adnetClickId`,
       get_json_object(json_data, '$.eventParams.external_referrer')    as `eventParams.external_referrer`,
       get_json_object(json_data, '$.eventParams.push_user_id')         as `eventParams.push_user_id`,
       get_json_object(json_data, '$.eventParams.impression_id')        as `eventParams.impression_id`,
       get_json_object(json_data, '$.eventParams.kw_rank')              as `eventParams.kw_rank`,
       get_json_object(json_data, '$.eventParams.ad_unit_name')         as `eventParams.ad_unit_name`,
       get_json_object(json_data, '$.eventParams.source_url')           as `eventParams.source_url`,
       get_json_object(json_data, '$.eventParams.event_uuid')           as `eventParams.event_uuid`,
       get_json_object(json_data, '$.eventParams.country')              as `eventParams.country`,
       get_json_object(json_data, '$.eventParams.dst_url')              as `eventParams.dst_url`,
       get_json_object(json_data, '$.eventParams.error')                as `eventParams.error`,
       get_json_object(json_data, '$.eventParams.js_version')           as `eventParams.js_version`,
       get_json_object(json_data, '$.eventParams.ext_version')          as `eventParams.ext_version`,
       get_json_object(json_data, '$.eventParams.ad_unit_updated_type') as `eventParams.ad_unit_updated_type`,
       get_json_object(json_data, '$.eventParams.ad_unit_updated_by')   as `eventParams.ad_unit_updated_by`,
       get_json_object(json_data, '$.eventParams.ad_unit_old_version')  as `eventParams.ad_unit_old_version`,
       get_json_object(json_data, '$.eventParams.ad_unit_new_version')  as `eventParams.ad_unit_new_version`,
       get_json_object(json_data, '$.eventParams.affiliate_id')         as `eventParams.affiliate_id`,
       get_json_object(json_data, '$.eventParams.sub_affiliate_id')     as `eventParams.sub_affiliate_id`,
       get_json_object(json_data, '$.eventParams.pre_event_uuid')       as `eventParams.pre_event_uuid`,
       get_json_object(json_data, '$.eventParams.display_url')          as `eventParams.display_url`,
       get_json_object(json_data, '$.eventParams.ad_unit_version')      as `eventParams.ad_unit_version`,
       get_json_object(json_data, '$.eventParams.uuid')                 as `eventParams.uuid`,
       dt,
       hour_id
from test.raw_user_events_json_raw_logs
union all
SELECT get_json_object(json_data, '$.timeStamp')                        AS `timeStamp`,
       'presearchKeywordImpression'                        AS `eventName`,
       get_json_object(json_data, '$.userId')                           AS `userId`,
       get_json_object(json_data, '$.src_url')                          as `src_url`,
       get_json_object(json_data, '$.ip')                               AS `ip`,
       get_json_object(json_data, '$.ua')                               AS `ua`,
       get_json_object(json_data, '$.refererDomain')                    AS `refererDomain`,
       get_json_object(json_data, '$.eventParams.sid')                  AS `eventParams.sid`,
       get_json_object(json_data, '$.eventParams.said')                 AS `eventParams.said`,
       temp.extracted_kw                                                AS `eventParams.kw`,
       get_json_object(json_data, '$.eventParams.adgroupId')            as `eventParams.adgroupId`,
       get_json_object(json_data, '$.eventParams.actionWorth')          AS `eventParams.actionWorth`,
       get_json_object(json_data, '$.eventParams.adnetClickId')         AS `eventParams.adnetClickId`,
       get_json_object(json_data, '$.eventParams.external_referrer')    AS `eventParams.external_referrer`,
       get_json_object(json_data, '$.eventParams.push_user_id')         AS `eventParams.push_user_id`,
       get_json_object(json_data, '$.eventParams.impression_id')        AS `eventParams.impression_id`,
       (temp.kw_rank + 1)                                               AS `eventParams.kw_rank`,
       get_json_object(json_data, '$.eventParams.ad_unit_name')         AS `eventParams.ad_unit_name`,
       get_json_object(json_data, '$.eventParams.source_url')           AS `eventParams.source_url`,
       get_json_object(json_data, '$.eventParams.event_uuid')           AS `eventParams.event_uuid`,
       get_json_object(json_data, '$.eventParams.country')              AS `eventParams.country`,
       get_json_object(json_data, '$.eventParams.dst_url')              AS `eventParams.dst_url`,
       get_json_object(json_data, '$.eventParams.error')                AS `eventParams.error`,
       get_json_object(json_data, '$.eventParams.js_version')           AS `eventParams.js_version`,
       get_json_object(json_data, '$.eventParams.ext_version')          AS `eventParams.ext_version`,
       get_json_object(json_data, '$.eventParams.ad_unit_updated_type') AS `eventParams.ad_unit_updated_type`,
       get_json_object(json_data, '$.eventParams.ad_unit_updated_by')   AS `eventParams.ad_unit_updated_by`,
       get_json_object(json_data, '$.eventParams.ad_unit_old_version')  AS `eventParams.ad_unit_old_version`,
       get_json_object(json_data, '$.eventParams.ad_unit_new_version')  AS `eventParams.ad_unit_new_version`,
       get_json_object(json_data, '$.eventParams.affiliate_id')         AS `eventParams.affiliate_id`,
       get_json_object(json_data, '$.eventParams.sub_affiliate_id')     AS `eventParams.sub_affiliate_id`,
       get_json_object(json_data, '$.eventParams.pre_event_uuid')       AS `eventParams.pre_event_uuid`,
       get_json_object(json_data, '$.eventParams.display_url')          AS `eventParams.display_url`,
       get_json_object(json_data, '$.eventParams.ad_unit_version')      AS `eventParams.ad_unit_version`,
       get_json_object(json_data, '$.eventParams.uuid')                 AS `eventParams.uuid`,
       dt,
       hour_id
FROM test.raw_user_events_json_raw_logs LATERAL VIEW POSEXPLODE(split(regexp_extract(get_json_object(json_data, '$.eventParams.kw'),'^\\["(.*)\\"]$',1),'","')) temp AS kw_rank,extracted_kw
WHERE get_json_object(json_data, '$.eventName') = 'presearchShown';


drop table if exists test.siq_raw_user_events_json_raw_logs;
CREATE EXTERNAL TABLE test.siq_raw_user_events_json_raw_logs (
 json_data string)
PARTITIONED BY (   dt STRING,    hour_id STRING )
ROW FORMAT DELIMITED
STORED AS TEXTFILE LOCATION 'hdfs://nn1.data.int.dc1.ad.net:8020/topics/raw/siq_user_profile_json_logs';
MSCK REPAIR TABLE test.siq_raw_user_events_json_raw_logs;

drop table if exists test.siq_raw_user_events_json_logs;
CREATE EXTERNAL TABLE test.siq_raw_user_events_json_logs (
 json_data string)
PARTITIONED BY (   dt STRING,    hour_id STRING )
ROW FORMAT DELIMITED
STORED AS TEXTFILE LOCATION 'hdfs://nn1.data.int.dc1.ad.net:8020/topics/siq_user_profile_json_logs';


MSCK REPAIR TABLE test.siq_raw_user_events_json_logs;

SELECT get_json_object(json_data, '$.timeStamp')                        as `timeStamp`,
       get_json_object(json_data, '$.eventName')                        as `eventName`,
       get_json_object(json_data, '$.userId')                           as `userId`,
       get_json_object(json_data, '$.src_url')                          as `src_url`,
       get_json_object(json_data, '$.ip')                               as `ip`,
       get_json_object(json_data, '$.ua')                               as `ua`,
       get_json_object(json_data, '$.refererDomain')                    as `refererDomain`,
       get_json_object(json_data, '$.eventParams.sid')                  as `eventParams.sid`,
       get_json_object(json_data, '$.eventParams.said')                 as `eventParams.said`,
       get_json_object(json_data, '$.eventParams.kw')                   as `eventParams.kw`,
       get_json_object(json_data, '$.eventParams.adgroupId')            as `eventParams.adgroupId`,
       get_json_object(json_data, '$.eventParams.actionWorth')          as `eventParams.actionWorth`,
       get_json_object(json_data, '$.eventParams.adnetClickId')         as `eventParams.adnetClickId`,
       get_json_object(json_data, '$.eventParams.external_referrer')    as `eventParams.external_referrer`,
       get_json_object(json_data, '$.eventParams.push_user_id')         as `eventParams.push_user_id`,
       get_json_object(json_data, '$.eventParams.impression_id')        as `eventParams.impression_id`,
       get_json_object(json_data, '$.eventParams.kw_rank')              as `eventParams.kw_rank`,
       get_json_object(json_data, '$.eventParams.ad_unit_name')         as `eventParams.ad_unit_name`,
       get_json_object(json_data, '$.eventParams.source_url')           as `eventParams.source_url`,
       get_json_object(json_data, '$.eventParams.event_uuid')           as `eventParams.event_uuid`,
       get_json_object(json_data, '$.eventParams.country')              as `eventParams.country`,
       get_json_object(json_data, '$.eventParams.dst_url')              as `eventParams.dst_url`,
       get_json_object(json_data, '$.eventParams.error')                as `eventParams.error`,
       get_json_object(json_data, '$.eventParams.js_version')           as `eventParams.js_version`,
       get_json_object(json_data, '$.eventParams.ext_version')          as `eventParams.ext_version`,
       get_json_object(json_data, '$.eventParams.ad_unit_updated_type') as `eventParams.ad_unit_updated_type`,
       get_json_object(json_data, '$.eventParams.ad_unit_updated_by')   as `eventParams.ad_unit_updated_by`,
       get_json_object(json_data, '$.eventParams.ad_unit_old_version')  as `eventParams.ad_unit_old_version`,
       get_json_object(json_data, '$.eventParams.ad_unit_new_version')  as `eventParams.ad_unit_new_version`,
       get_json_object(json_data, '$.eventParams.affiliate_id')         as `eventParams.affiliate_id`,
       get_json_object(json_data, '$.eventParams.sub_affiliate_id')     as `eventParams.sub_affiliate_id`,
       get_json_object(json_data, '$.eventParams.pre_event_uuid')       as `eventParams.pre_event_uuid`,
       get_json_object(json_data, '$.eventParams.display_url')          as `eventParams.display_url`,
       get_json_object(json_data, '$.eventParams.ad_unit_version')      as `eventParams.ad_unit_version`,
       get_json_object(json_data, '$.eventParams.uuid')                 as `eventParams.uuid`,
       dt,
       hour_id
FROM test.siq_raw_user_events_json_raw_logs;
select toDate(toDateTime(_timestamp, 'America/Los_Angeles'))                       AS kafka_dt,
       toHour(toDateTime(_timestamp, 'America/Los_Angeles'))                       AS kafka_hour,
       toDateTime(CAST(timeStamp / 1000 as BIGINT), 'UTC')                         AS dt_utc,
       toDateTime(CAST(timeStamp / 1000 as BIGINT), 'UTC')                         AS dt,
       toDate(toDateTime(CAST(timeStamp / 1000 as BIGINT), 'America/Los_Angeles')) AS `date`,
       toDateTime(CAST(timeStamp / 1000 as BIGINT), 'America/Los_Angeles')         AS event_time,
       timeStamp                                                                   AS epoch,
       eventName                                                                   AS event_name,
       userId                                                                      AS user_id,
       coalesce(`eventParams.source_url`, srcUrl)                                  AS source_url,
       coalesce(`eventParams.sid`, '')                                             AS sid,
       coalesce(`eventParams.said`, '')                                            AS said,
       `eventParams.kw`                                                            AS keyword,
       ''                                                                          AS adgroup_id,
       CAST(0.0 as Nullable(DOUBLE))                                               AS action_worth,
       ip                                                                          AS ip,
       ua                                                                          AS ua,
       refererDomain                                                               AS referrer_domain,
       `eventParams.adsReturned`                                                   AS ads_returned,
       CAST('SEARCHIQ' AS Nullable(String))                                        AS data_source,
       CAST(_timestamp as Nullable(BIGINT))                                        AS partition_time,
       CAST(_timestamp as Nullable(BIGINT))                                        AS receivetimestamp
from etl.siq_raw_user_events_consumer