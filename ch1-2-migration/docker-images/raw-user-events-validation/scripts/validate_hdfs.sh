#! /bin/bash

set -e

DATE=$1
HOUR=$2

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
THRESHOLD=0.01

HIVE_SSH="ssh -o StrictHostKeyChecking=no data@nn1.data.int.dc1.ad.net"
HIVE_JSON_TABLE="test.raw_user_events_json"
HIVE_JSON_TABLE_RAW="test.raw_user_events_json_raw_logs"

HIVE_CLI="beeline -u jdbc:hive2://hiveserver.data.int.dc1.ad.net:10000/addotnet -n hdfs"

d=$(date -d $DATE +%s)
PROCESS_DATE=`date -d @$d +%Y-%m-%d`
echo "Processing date: $PROCESS_DATE and HOUR: $HOUR"
echo "Start validation to check number of records in Hive"

echo "Updaging the hive metadata"
${HIVE_SSH} "${HIVE_CLI} -e \"msck repair table ${HIVE_JSON_TABLE}\""
${HIVE_SSH} "${HIVE_CLI} -e \"msck repair table ${HIVE_JSON_TABLE_RAW}\""

hiveData=$(${HIVE_SSH} "${HIVE_CLI} --showHeader=false --outputformat=tsv2  -e \"SELECT COUNT(*) from ${HIVE_JSON_TABLE} WHERE dt='${PROCESS_DATE}' and hour_id=$HOUR\"")
hiveRecords=$(echo ${hiveData} | awk '{print $1}')
echo "${hiveRecords} records in Hive table [${HIVE_JSON_TABLE}]"

hiveDataRaw=$(${HIVE_SSH} "${HIVE_CLI} --showHeader=false --outputformat=tsv2  -e \"SELECT COUNT(*) from ${HIVE_JSON_TABLE_RAW} WHERE dt='${PROCESS_DATE}' and hour_id=$HOUR\"")
hiveRecordsRaw=$(echo ${hiveDataRaw} | awk '{print $1}')
function abs() {
  [[ $(($@)) -lt 0 ]] && echo "$((($@) * -1))" || echo "$(($@))"
}
echo "${hiveRecordsRaw} records in Hive RAW table [${HIVE_JSON_TABLE_RAW}]"

if (( $(bc <<< "${hiveRecords} == ${hiveRecordsRaw}") )); then
    echo "For Date='$PROCESS_DATE' and HOUR=$HOUR the number of records in Hive(${hiveRecords}) and Hive RAW(${hiveRecordsRaw}) are equal "
else
  count_diff=$((hiveRecords - hiveRecordsRaw))
  count_diff=$(abs "$count_diff")
  if [[ ${hiveRecordsRaw} != 0 ]]; then
    diff=$(bc <<<"scale=2; ((${count_diff})*100)/${hiveRecordsRaw}")
  else
    diff=$(bc <<<"scale=2; (${count_diff})")
  fi
  if (($(bc <<<"${diff} >= ${THRESHOLD}"))); then
    echo "Count mismatch for Date='$PROCESS_DATE' and HOUR=$HOUR. The number of records in Hive(${hiveRecords}) and Hive RAW(${hiveRecordsRaw})."
    exit 1
  else
    echo "For Date='$PROCESS_DATE' and HOUR=$HOUR there is minor diff ${diff}% (<= ${THRESHOLD}%) with absolute diff $count_diff. <br> Number of records in Hive = ${hiveRecords} and Hive RAW = ${hiveRecordsRaw} "
  fi
fi