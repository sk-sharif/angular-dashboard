#! /bin/bash

set -e

CLICKHOUSE_HOSTNAME=$1
CLICKHOUSE_DATABASE=$2
DATE=$3
HOUR=$4
VALIDATE_VS_RAW_LOGS=$5

if [[ -z "${CLICKHOUSE_HOSTNAME}" ]]; then
  echo "Click-House hostname is not defined"
  exit 1
fi

if [[ -z "${CLICKHOUSE_DATABASE}" ]]; then
  echo "Click-House database is not defined"
  exit 1
fi

if [[ -z "${DATE}" ]]; then
  echo "DATE is not defined"
  exit 1
fi

if [[ -z "${HOUR}" ]]; then
  echo "HOUR is not defined"
  exit 1
fi

THRESHOLD=0.01

CH_SSH_USER_HOSTNAME="root@${CLICKHOUSE_HOSTNAME}"
CLICKHOUSE_SSH="ssh -o StrictHostKeyChecking=no ${CH_SSH_USER_HOSTNAME}"
HIVE_SSH="ssh -o StrictHostKeyChecking=no data@nn1.data.int.dc1.ad.net"
CLICKHOUSE_TABLE="addotnet.raw_user_events"
HIVE_TABLE="test.raw_user_event_view"
HIVE_JSON_TABLE="test.raw_user_events_json"

if [[ "${VALIDATE_VS_RAW_LOGS}" == "1" ]]; then
  HIVE_TABLE="test.raw_user_event_raw_logs_view"
  HIVE_JSON_TABLE="test.raw_user_events_json_raw_logs"
fi
HIVE_CLI="beeline -u jdbc:hive2://hiveserver.data.int.dc1.ad.net:10000/addotnet -n hdfs"

d=$(date -d $DATE +%s)
PROCESS_DATE=$(date -d @$d +%Y-%m-%d)
echo "Processing date: $PROCESS_DATE and HOUR: $HOUR"
echo "Start validation to check number of records in Hive and ClickHouse"

#clickHouseData=$(${CLICKHOUSE_SSH} "clickhouse-client --database=${CLICKHOUSE_DATABASE} -q \"SELECT COUNT(*) AS records FROM ${CLICKHOUSE_TABLE} WHERE kafka_dt  = '${PROCESS_DATE}' AND hour_id = ${HOUR}\"")
echo "clickhoyse sql: SELECT COUNT(*) AS records FROM ${CLICKHOUSE_TABLE} WHERE kafka_dt  = '${PROCESS_DATE}' and kafka_hour = $HOUR and data_source='DATA_AUG' and event_name <>'presearchKeywordImpression'"
clickHouseData=$(${CLICKHOUSE_SSH} "clickhouse-client --database=${CLICKHOUSE_DATABASE} -q \"SELECT COUNT(*) AS records FROM ${CLICKHOUSE_TABLE} WHERE kafka_dt  = '${PROCESS_DATE}' and kafka_hour = $HOUR and data_source='DATA_AUG' and event_name <>'presearchKeywordImpression'\"")
clickHouseRecords=$(echo ${clickHouseData} | awk '{print $1}')
echo "${clickHouseRecords} records in ClickHouse table [${CLICKHOUSE_TABLE}]"
echo "updaging the hive metadata"
${HIVE_SSH} "${HIVE_CLI} -e \"msck repair table ${HIVE_JSON_TABLE}\""

hiveData=$(${HIVE_SSH} "${HIVE_CLI} --showHeader=false --outputformat=tsv2  -e \"SELECT COUNT(*) from ${HIVE_JSON_TABLE} WHERE dt='${PROCESS_DATE}' and hour_id=$HOUR\"")
hiveRecords=$(echo ${hiveData} | awk '{print $1}')

echo "${hiveRecords} records in Hive table [${HIVE_TABLE}]"
function abs() {
  [[ $(($@)) -lt 0 ]] && echo "$((($@) * -1))" || echo "$(($@))"
}
if (($(bc <<<"${hiveRecords} == ${clickHouseRecords}"))); then
  echo "For Date='$PROCESS_DATE' and HOUR=$HOUR the number of records in Hive(${hiveRecords}) and ClickHouse(${clickHouseRecords}) are equal "
else
  count_diff=$((hiveRecords - clickHouseRecords))
  count_diff=$(abs "$count_diff")
  if [[ ${clickHouseRecords} != 0 ]]; then
    diff=$(bc <<<"scale=2; ((${count_diff})*100)/${clickHouseRecords}")
  else
    diff=$(bc <<<"scale=2; (${count_diff})")
  fi
  if (($(bc <<<"${diff} >= ${THRESHOLD}"))); then
    echo "Count mismatch for Date='$PROCESS_DATE' and HOUR=$HOUR. The number of records in Hive(${hiveRecords}) and ClickHouse(${clickHouseRecords})."
    exit 1
  else
    echo "For Date='$PROCESS_DATE' and HOUR=$HOUR there is minor diff ${diff}% (<= ${THRESHOLD}%) with absolute diff $count_diff. <br> Number of records in Hive = ${hiveRecords} and ClickHouse = ${clickHouseRecords} "
  fi
fi
