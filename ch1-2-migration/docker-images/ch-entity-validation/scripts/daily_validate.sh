#! /bin/bash

set -e

CLICKHOUSE_HOSTNAME1=$1
CLICKHOUSE_DATABASE1=$2
CLICKHOUSE_HOSTNAME2=$3
CLICKHOUSE_DATABASE2=$4
DATE=$5
ENTITYCOLUMN=$6
COLUMNS=$7
THRESHOLD=$8
TABLE=$9
DATE_COLUMN=${10}

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

if [[ -z "${ENTITYCOLUMN}" ]]
then
  echo "ENTITYCOLUMN is not defined"
  exit 1
fi

if [[ -z "${COLUMNS}" ]]
then
  echo "COLUMNS are not defined"
  exit 1
fi

if [[ -z "${THRESHOLD}" ]]
then
  echo "THRESHOLD is not defined"
  exit 1
fi

if [[ -z "${TABLE}" ]]
then
  TABLE="ad_event"
fi

if [[ -z "${DATE_COLUMN}" ]]
then
  DATE_COLUMN="dt"
fi

CH_SSH_USER_HOSTNAME1="root@${CLICKHOUSE_HOSTNAME1}"
CH_SSH_USER_HOSTNAME2="root@${CLICKHOUSE_HOSTNAME2}"

d=$(date -d $DATE +%s)
temp_date=`date -d @$d +%Y-%m-%d`
echo "Processing date $temp_date"
OUTPUT_MESSAGE="See airflow log for full details. \n"
EXIT_CODE=0

IFS=',' read -ra colarr <<< "$COLUMNS"

return_code=0;
sql="SELECT ${ENTITYCOLUMN},"
for col in "${colarr[@]}"
do
    sql="${sql} COALESCE(SUM(${col}),0) ,"
done
sql=${sql::-1}
sql="${sql} FROM ${TABLE} where ${DATE_COLUMN} = 'PROCESS_DATE' GROUP BY ${ENTITYCOLUMN}"

echo "SQL: ${sql}"
OUTPUT_MESSAGE="${OUTPUT_MESSAGE} SQL: ${sql} \n"
result_from_ad_event_db1=`echo "${sql}" | sed "s/PROCESS_DATE/$temp_date/g" |ssh -o StrictHostKeyChecking=no "${CH_SSH_USER_HOSTNAME1}" -T "cat | clickhouse-client -h \"${CLICKHOUSE_HOSTNAME1}\" -d \"${CLICKHOUSE_DATABASE1}\" --multiline --multiquery"`
result_from_ad_event_db2=`echo "${sql}" | sed "s/PROCESS_DATE/$temp_date/g" |ssh -o StrictHostKeyChecking=no "${CH_SSH_USER_HOSTNAME2}" -T "cat | clickhouse-client -h \"${CLICKHOUSE_HOSTNAME2}\" -d \"${CLICKHOUSE_DATABASE2}\" --multiline --multiquery"`

db1Rows=`echo "${result_from_ad_event_db1}" | wc -l`
db2Rows=`echo "${result_from_ad_event_db2}" | wc -l`

if [[ "${db1Rows}" -ne "${db2Rows}" ]]; then
    OUTPUT_MESSAGE="${OUTPUT_MESSAGE}The number of ${ENTITYCOLUMN} in ${CLICKHOUSE_HOSTNAME1} and ${CLICKHOUSE_HOSTNAME2} are not equal. ${CLICKHOUSE_HOSTNAME1}[${db1Rows}] ${CLICKHOUSE_HOSTNAME2}[${db2Rows}] \n"
else
    echo "Row counts match [${db1Rows} == ${db2Rows}]"
fi
OUTPUT_MESSAGE="${OUTPUT_MESSAGE} ${ENTITYCOLUMN}[Entity Key].         ${CLICKHOUSE_HOSTNAME1}  |  ${CLICKHOUSE_HOSTNAME2} \n"
while read row_data; do
    IFS=$'\t' read -ra row <<< "$row_data"
    echo "Verifying ${CLICKHOUSE_HOSTNAME1} data: ${ENTITYCOLUMN}[${row}]"
    row2_data=`echo "${result_from_ad_event_db2}" | awk -v entity=${row[0]} '$1 == entity'`
    if [[ -n "${row2_data}" ]]; then
        echo "${CLICKHOUSE_HOSTNAME2} data: ${ENTITYCOLUMN}[${row2_data}]"
        arrlength=$((${#row[@]}-1))
        IFS=$'\t' read -ra row2 <<< "$row2_data"

        for i in $(seq 1 ${arrlength}); do
            val1=${row[${i}]}
            val2=${row2[${i}]}
            if [[ ${val1} != 0 ]]; then
                diff=`bc <<< "scale=2; ((${val2}-${val1})*100)/${val1}"`
            else
               diff=`bc <<< "scale=2; (${val2}-${val1})"`
            fi

            echo "diff in ${colarr[$i-1]}: ${diff}%"
            if (( $(bc <<< "${diff} < 0") ))
            then
                diff=`bc <<< "scale=2; ${diff}*-1"`
            fi

            if (( $(bc <<< "${diff} >= ${THRESHOLD}") ))
            then
                OUTPUT_MESSAGE="${OUTPUT_MESSAGE} ${ENTITYCOLUMN}[${row[0]}]. ${colarr[$i-1]}: ${val1} != ${val2} \n"
                if [[ ${row[0]} == 0 ]]; then
                  echo "Ignoring row with id 0 in comparission";
                else
                  EXIT_CODE=1
                fi
            fi
        done
    elif [[ "${row[0]}" != "N" ]]; then
            arrlength=$((${#row[@]}-1))
            for i in $(seq 1 ${arrlength}); do
                 val=${row[${i}]}
                 if [[ ${val} != 0 ]]; then
                    echo "${ENTITYCOLUMN}[${row[0]}]. ${CLICKHOUSE_HOSTNAME1} | Unable to find ${ENTITYCOLUMN} in ${CLICKHOUSE_HOSTNAME2}"
                    OUTPUT_MESSAGE="${OUTPUT_MESSAGE}${ENTITYCOLUMN}[${row[0]}]. ${CLICKHOUSE_HOSTNAME1} | Unable to find ${ENTITYCOLUMN} in ${CLICKHOUSE_HOSTNAME2} \n"
                    EXIT_CODE=1
                 fi
            done
    fi
done <<< "${result_from_ad_event_db1}"

echo "Completed the process"
echo -e "${OUTPUT_MESSAGE}"
echo "${OUTPUT_MESSAGE}"
exit ${EXIT_CODE}