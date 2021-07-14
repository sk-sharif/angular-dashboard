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

d=$(date -d $DATE +%s)
temp_date=`date -d @$d +%Y-%m-%d`
dy=$(( $d - 86400 ))
yesterday=`date -d @$dy +%Y-%m-%d`
echo "Processing date $temp_date "
echo "|--------Back-filling using [ `cat ./sql/ad_event_to_ad_event_daily_backfill.sql  | sed "s/PROCESS_DATE/$temp_date/g" | sed "s/PREV_DATE/$yesterday/g"` ]"
cat ./sql/ad_event_to_ad_event_daily_backfill.sql  | sed "s/PROCESS_DATE/$temp_date/g"| sed "s/PREV_DATE/$yesterday/g" | ssh -o StrictHostKeyChecking=no "${CH_SSH_USER_HOSTNAME1}" -T "cat | clickhouse-client -h \"${CLICKHOUSE_HOSTNAME1}\" -d \"${CLICKHOUSE_DATABASE1}\" --multiline --multiquery"