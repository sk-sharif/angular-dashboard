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
echo "Processing date $temp_date "

sql="select hour_id, event_type from addotnet.ad_event where dt = '${temp_date}' and event_type in ('request','ad_return')  group by hour_id, event_type having count(*)>0 order by event_type,hour_id"
partitions_to_process=`echo "${sql}" |ssh -o StrictHostKeyChecking=no "${CH_SSH_USER_HOSTNAME1}" -T "cat | clickhouse-client -h \"${CLICKHOUSE_HOSTNAME1}\" -d \"${CLICKHOUSE_DATABASE1}\" --multiline --multiquery"`

echo "Partitions to process are ${partitions_to_process}"
counter=1
while read row_data; do
    IFS=$'\t' read -ra row <<< "$row_data"
    echo "Processing partition : ${row}"

    echo "Executing the drop partition command [ ALTER TABLE addotnet.ad_event DROP PARTITION ('${temp_date}',${row[0]},'${row[1]}');]"
    echo "ALTER TABLE addotnet.ad_event DROP PARTITION ('${temp_date}',${row[0]},'${row[1]}')" |ssh -o StrictHostKeyChecking=no "${CH_SSH_USER_HOSTNAME1}" -T "cat | clickhouse-client -h \"${CLICKHOUSE_HOSTNAME1}\" -d \"${CLICKHOUSE_DATABASE1}\" --multiline --multiquery"

    if [ $(echo "$counter % 4" | bc) -eq 0 ]; then
      echo "Wait for 10 seconds"
      sleep 10s
    fi
    counter=$((counter+1))
done <<< "${partitions_to_process}"


#echo "|--------Cleaning using [ `cat ./sql/ad_event_cleanup.sql  | sed "s/PROCESS_DATE/$temp_date/g"` ]"
#cat ./sql/ad_event_cleanup.sql  | sed "s/PROCESS_DATE/$temp_date/g"| ssh -o StrictHostKeyChecking=no "${CH_SSH_USER_HOSTNAME1}" -T "cat | clickhouse-client -h \"${CLICKHOUSE_HOSTNAME1}\" -d \"${CLICKHOUSE_DATABASE1}\" --multiline --multiquery"