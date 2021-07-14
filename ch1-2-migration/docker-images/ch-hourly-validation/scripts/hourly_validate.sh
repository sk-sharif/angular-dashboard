#! /bin/bash

set -e

CLICKHOUSE_HOSTNAME1=$1
CLICKHOUSE_DATABASE1=$2
CLICKHOUSE_HOSTNAME2=$3
CLICKHOUSE_DATABASE2=$4
DATE=$5
HOUR=$6

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

if [[ -z "${CLICKHOUSE_HOSTNAME2}" ]]
then
  echo "Click-House hostname2 is not defined"
  exit 1
fi

if [[ -z "${CLICKHOUSE_DATABASE2}" ]]
then
  echo "Click-House database2 is not defined"
  exit 1
fi

if [[ -z "${DATE}" ]]
then
  echo "DATE is not defined"
  exit 1
fi

if [[ -z "${HOUR}" ]]
then
  echo "HOUR is not defined"
  exit 1
fi

CH_SSH_USER_HOSTNAME1="root@${CLICKHOUSE_HOSTNAME1}"
CH_SSH_USER_HOSTNAME2="root@${CLICKHOUSE_HOSTNAME2}"

d=$(date -d $DATE +%s)
temp_date=`date -d @$d +%Y-%m-%d`
echo "Processing date $temp_date at `date +'%T'` and Hour $HOUR"

declare -a colarr=("requests" "raw_clicks" "paid_clicks" "revenue" "pub_payout" "actions_worth" "dollars_worth")
return_code=0;
for col in "${colarr[@]}"
do
    result_from_ad_event_db1=`echo "select coalesce(sum(${col}),0) from ad_event where dt = 'PROCESS_DATE' and hour_id = HOURID" | sed "s/PROCESS_DATE/$temp_date/g" | sed "s/HOURID/$HOUR/g"|ssh -o StrictHostKeyChecking=no "${CH_SSH_USER_HOSTNAME1}" -T "cat | clickhouse-client -h \"${CLICKHOUSE_HOSTNAME1}\" -d \"${CLICKHOUSE_DATABASE1}\" --multiline --multiquery"`
    result_from_ad_event_db2=`echo "select coalesce(sum(${col}),0) from ad_event where dt = 'PROCESS_DATE' and hour_id = HOURID" | sed "s/PROCESS_DATE/$temp_date/g" | sed "s/HOURID/$HOUR/g"|ssh -o StrictHostKeyChecking=no "${CH_SSH_USER_HOSTNAME2}" -T "cat | clickhouse-client -h \"${CLICKHOUSE_HOSTNAME2}\" -d \"${CLICKHOUSE_DATABASE2}\" --multiline --multiquery"`
    if [[ "$result_from_ad_event_db1" == "$result_from_ad_event_db2" ]]
    then
        echo "Checksum pass for ${col} [$result_from_ad_event_db1 == $result_from_ad_event_db2]"
    else
        val_diff=`echo ${result_from_ad_event_db1}-${result_from_ad_event_db2}| bc`
        #converting to int
        int_1=${result_from_ad_event_db1%.*}
        int_2=${result_from_ad_event_db2%.*}
        if [[ "$int_1" == "$int_2" ]]
        then
            echo "Checksum passed (with marginal difference) for ${col} with difference [$val_diff] , where ${CLICKHOUSE_HOSTNAME1} has ${result_from_ad_event_db1} ${col} and ${CLICKHOUSE_HOSTNAME2} has ${result_from_ad_event_db2} ${col}"
        else
            echo "Checksum failed for ${col} with difference [$val_diff] , where ${CLICKHOUSE_HOSTNAME1} has ${result_from_ad_event_db1} ${col} and ${CLICKHOUSE_HOSTNAME2} has ${result_from_ad_event_db2} ${col}"
            return_code = 1;
        fi
    fi
done
echo "Completed the process"
if [[ "$return_code" == 1 ]]
then
    exit 1;
fi