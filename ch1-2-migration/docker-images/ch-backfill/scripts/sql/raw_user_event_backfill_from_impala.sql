alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',00);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',01);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',02);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',03);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',04);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',05);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',06);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',07);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',08);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',09);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',10);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',11);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',12);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',13);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',14);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',15);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',16);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',17);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',18);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',19);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',20);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',21);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',22);
alter table addotnet.raw_user_events drop partition ('PROCESS_DATE',23);

DROP TABLE IF EXISTS test.raw_user_events_live_hdfs_parquet;
CREATE TABLE test.raw_user_events_live_hdfs_parquet
(
    dt_utc               Nullable(String),
    `date`               Nullable(String),
    event_time           Nullable(String),
    epoch                Nullable(BIGINT),
    event_name           Nullable(String),
    user_id              Nullable(String),
    source_url           Nullable(String),
    sid                  Nullable(String),
    said                 Nullable(String),
    keyword              Nullable(String),
    adgroup_id           Nullable(String),
    action_worth         Nullable(DOUBLE),
    ip                   Nullable(String),
    ua                   Nullable(String),
    referrer_domain      Nullable(String),
    ads_returned         Nullable(DOUBLE),
    country_name         Nullable(String),
    country_iso_code     Nullable(String),
    click_id             Nullable(String),
    data_source          Nullable(String),
    external_referrer    Nullable(String),
    push_user_id         Nullable(String),
    impression_id        Nullable(String),
    keyword_rank         Nullable(DOUBLE),
    ad_unit_name         Nullable(String),
    event_uuid           Nullable(String),
    dst_url              Nullable(String),
    error_message        Nullable(String),
    js_version           Nullable(String),
    ext_version          Nullable(String),
    ad_unit_updated_type Nullable(String),
    ad_unit_updated_by   Nullable(String),
    ad_unit_old_version  Nullable(String),
    ad_unit_new_version  Nullable(String),
    partition_time       Nullable(BIGINT),
    ad_unit_version      Nullable(String),
    affiliate_id         Nullable(String),
    display_url          Nullable(String),
    pre_event_uuid       Nullable(String),
    receivetimestamp     Nullable(BIGINT),
    sub_affiliate_id     Nullable(String),
    uuid                 Nullable(String),
    dt_val               Nullable(String)
) ENGINE = HDFS(
           'hdfs://nn1.data.int.dc1.ad.net:8020/user/hive/warehouse/backfill.db/raw_user_events_new_tmp2/dt=PROCESS_DATE/*',
           'Parquet');


insert into addotnet.raw_user_events (kafka_dt,kafka_hour,dt_utc,dt,
                                      `date`,
                                      event_time,
                                      epoch,
                                      event_name,
                                      user_id,
                                      source_url,
                                      sid,
                                      said,
                                      keyword,
                                      adgroup_id,
                                      action_worth,
                                      ip,
                                      ua,
                                      referrer_domain,
                                      ads_returned,
                                      country_name,
                                      country_iso_code,
                                      click_id,
                                      data_source,
                                      external_referrer,
                                      push_user_id,
                                      impression_id,
                                      keyword_rank,
                                      ad_unit_name,
                                      event_uuid,
                                      dst_url,
                                      error_message,
                                      js_version,
                                      ext_version,
                                      ad_unit_updated_type,
                                      ad_unit_updated_by,
                                      ad_unit_old_version,
                                      ad_unit_new_version,
                                      partition_time,
                                      ad_unit_version,
                                      affiliate_id,
                                      display_url,
                                      pre_event_uuid,
                                      receivetimestamp,
                                      sub_affiliate_id,
                                      uuid)
select dt_val , toHour(toDateTime(receivetimestamp / 1000000)),dt_utc,
       dt_val as dt,
       `date`,
       event_time,
       epoch,
       event_name,
       user_id,
       source_url,
       coalesce(sid, ''),
       coalesce(said, ''),
       keyword,
       adgroup_id,
       action_worth,
       ip,
       ua,
       referrer_domain,
       ads_returned,
       country_name,
       country_iso_code,
       click_id,
       data_source,
       external_referrer,
       push_user_id,
       impression_id,
       keyword_rank,
       ad_unit_name,
       event_uuid,
       dst_url,
       error_message,
       js_version,
       ext_version,
       ad_unit_updated_type,
       ad_unit_updated_by,
       ad_unit_old_version,
       ad_unit_new_version,
       partition_time,
       ad_unit_version,
       affiliate_id,
       display_url,
       pre_event_uuid,
       receivetimestamp,
       sub_affiliate_id,
       uuid
from test.raw_user_events_live_hdfs_parquet;