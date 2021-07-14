#!/bin/bash

set -e

PROCESS_DATE=${PROCESS_DATE:-${1:-$(date +"%Y-%m-%d")}} #take environment variable, if null, take parameter 1, if null, take today's date

HDFS_SERVER="data@nn1.data.int.dc1.ad.net"
REPORT_NAME="Ad.Net_${PROCESS_DATE}.csv"
REPORT_LOCAL_PATH_NAME="/data/${REPORT_NAME}"
REPORT_REMOTE_PATH_NAME="/tmp/${REPORT_NAME}"
ADSBRAIN_RAW_TABLE="addotnet.adsbrain_yahoo_feed_report"

if [ -z "${REPORT_LOCAL_PATH_NAME}" ]; then

  echo "Unable to find report: ${REPORT_LOCAL_PATH_NAME}"
  exit 1

else

  echo "Copying file[${REPORT_LOCAL_PATH_NAME}] from local to hdfs server[${HDFS_SERVER}]"
  scp -o StrictHostKeyChecking=no ${REPORT_LOCAL_PATH_NAME} ${HDFS_SERVER}:${REPORT_REMOTE_PATH_NAME}

  echo "Pushing file[${REPORT_REMOTE_PATH_NAME}] to hdfs"
  ssh -o StrictHostKeyChecking=no ${HDFS_SERVER} "hdfs dfs -mkdir -p /user/hive/warehouse/addotnet.db/adsbrain_yahoo_feed_report/dt=${PROCESS_DATE}/"
  ssh -o StrictHostKeyChecking=no ${HDFS_SERVER} "hdfs dfs -chown -R impala:hive /user/hive/warehouse/addotnet.db/adsbrain_yahoo_feed_report/dt=${PROCESS_DATE}/"
  ssh -o StrictHostKeyChecking=no ${HDFS_SERVER} "hdfs dfs -copyFromLocal -f ${REPORT_REMOTE_PATH_NAME} /user/hive/warehouse/addotnet.db/adsbrain_yahoo_feed_report/dt=${PROCESS_DATE}/"

  ssh -o StrictHostKeyChecking=no ${HDFS_SERVER} "rm -r \"${REPORT_REMOTE_PATH_NAME}\""
  ssh -o StrictHostKeyChecking=no ${HDFS_SERVER} "impala-shell -i d10.data.int.dc1.ad.net -d amazon -q \"ALTER TABLE ${ADSBRAIN_RAW_TABLE} ADD IF NOT EXISTS PARTITION  (dt = '${PROCESS_DATE}')\""
  ssh -o StrictHostKeyChecking=no ${HDFS_SERVER} "impala-shell -i d10.data.int.dc1.ad.net -d amazon -q \"INVALIDATE METADATA ${ADSBRAIN_RAW_TABLE}\""
  
  echo "Validating number of records between file and database..."
  impala_data=$(ssh -o StrictHostKeyChecking=no ${HDFS_SERVER} "impala-shell -i d10.data.int.dc1.ad.net -d addotnet --delimited -q \"SELECT COUNT(*) FROM ${ADSBRAIN_RAW_TABLE} WHERE dt = '${PROCESS_DATE}'\"")
  db_records=$(echo "${impala_data}" | awk -F' ' '{print $1}')
  file_records=$(awk 'FNR > 1 {print $0}' "${REPORT_LOCAL_PATH_NAME}" | wc -l)

  echo "Number of records in database: ${db_records}"
  echo "Number of records in file: ${file_records}"
  if [[ "${db_records}" -eq "${file_records}" ]]; then
    echo "Number of records between file and database are equal"
  else
    echo "Number of records between file and database are not equal"
    exit 1
  fi
  
  echo "Getting revenue amount from table"
  impala_data=$(ssh -o StrictHostKeyChecking=no ${HDFS_SERVER} "impala-shell -i d10.data.int.dc1.ad.net -d addotnet --delimited -q \"SELECT SUM(amount) FROM ${ADSBRAIN_RAW_TABLE} WHERE dt = '${PROCESS_DATE}'\"")
  table_amount=$(echo "${impala_data}" | awk -F' ' '{print $1}')
  
  echo "Getting revenue amount from file"
  file_amount=$(awk -F ',' '{sum += $6}END{print sum}' ${REPORT_LOCAL_PATH_NAME})
  
  echo "Total amount in table: ${table_amount}"
  echo "Total amount in file: ${file_amount}"
  
  table_amount=$(bc <<< "scale=2; ${table_amount}/1")
  file_amount=$(bc <<< "scale=2; ${file_amount}/1")
  echo "Total amount in table after scale: ${table_amount}"
  echo "Total amount in file after scale: ${file_amount}"
  
  echo "Validating amount between file and impala table..."
  if (( $(bc <<< "${table_amount} == ${file_amount}") )); then
    echo "Amount revenue is the same in impala table and file"
  else
    echo "ERROR: Amount revenue is not the same in impala table and file"
    exit 1
  fi
fi