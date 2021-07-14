alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 00);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 01);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 02);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 03);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 04);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 05);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 06);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 07);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 08);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 09);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 10);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 11);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 12);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 13);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 14);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 15);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 16);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 17);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 18);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 19);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 20);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 21);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 22);
alter table addotnet.raw_user_events_new
    drop partition ('PROCESS_DATE', 23);


insert into addotnet.raw_user_events_new (kafka_dt,
                                          kafka_hour,
                                          dt_utc,
                                          dt,
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
select kafka_dt,
       CASE
           when receivetimestamp is not null then toHour(toDateTime(receivetimestamp / 1000000))
           ELSE 00 END as kafka_hour,
       dt_utc,
       dt,
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
       uuid
from addotnet.raw_user_events
where kafka_dt = 'PROCESS_DATE';