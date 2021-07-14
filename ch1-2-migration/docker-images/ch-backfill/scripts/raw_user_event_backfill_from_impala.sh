#! /bin/bash

set -e

CLICKHOUSE_HOSTNAME1=$1
CLICKHOUSE_DATABASE1=$2
DATE=$3

if [[ -z "${CLICKHOUSE_HOSTNAME1}" ]]
then
  echo "Click-House hostname1 is not defined"
  exit 1
fi

if [[ -z "${CLICKHOUSE_DATABASE1}" ]]
then
  echo "Click-House database1 is not defined"
  exit 1
fi

if [[ -z "${DATE}" ]]
then
  echo "DATE is not defined"
  exit 1
fi


CH_SSH_USER_HOSTNAME1="root@${CLICKHOUSE_HOSTNAME1}"
IMPALA_DATA_NODE="d10.data.int.dc1.ad.net"
IMPALA_CLI_BACKFILL="impala-shell -i ${IMPALA_DATA_NODE} -d backfill"
IMPALA_SSH="ssh -o StrictHostKeyChecking=no data@nn1.data.int.dc1.ad.net"

d=$(date -d $DATE +%s)
temp_date=`date -d @$d +%Y-%m-%d`
dy=$(( $d - 86400 ))
yesterday=`date -d @$dy +%Y-%m-%d`
echo "Processing date $temp_date "
echo "Writing data to backfill impala table raw_user_events_new_tmp2"
IMPALA_QUERY="INSERT OVERWRITE backfill.raw_user_events_new_tmp2 (dt_utc,
                                                    dt,
                                                    \\\`date\\\`,
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
                                                    uuid, dt_val)
select dt_utc,
       dt,
       \\\`date\\\`,
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
       uuid,
       dt as dt_val
FROM addotnet.raw_user_events_new
where dt = '${temp_date}'"

${IMPALA_SSH} "${IMPALA_CLI_BACKFILL} --delimited -q \"${IMPALA_QUERY}\""
echo "|--------Back-filling using [ `cat ./sql/raw_user_event_backfill_from_impala.sql  | sed "s/PROCESS_DATE/$temp_date/g"` ]"
cat ./sql/raw_user_event_backfill_from_impala.sql  | sed "s/PROCESS_DATE/$temp_date/g"| ssh -o StrictHostKeyChecking=no "${CH_SSH_USER_HOSTNAME1}" -T "cat | clickhouse-client -h \"${CLICKHOUSE_HOSTNAME1}\" -d \"${CLICKHOUSE_DATABASE1}\" --multiline --multiquery"