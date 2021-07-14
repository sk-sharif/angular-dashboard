#!/bin/bash

set -e

if [ "$#" -lt 3 ]; then
    echo -e "\nusage: ./download_adsbrain_report.sh <process_date> <sftp_username> <sftp_password>"
    exit 1
fi

PROCESS_DATE=${PROCESS_DATE:-${1:-$(date +"%Y-%m-%d")}} #take environment variable, if null, take parameter 1, if null, take today's date
SFTP_USERNAME=$2
SFTP_PASSWORD=$3
SFTP_DIRECTORY="/incoming"
echo "PROCESS_DATE: ${PROCESS_DATE}"
echo "SFTP_USERNAME: ${SFTP_USERNAME}"
echo "SFTP_PASSWORD: ****************"
echo "SFTP_DIRECTORY: ${SFTP_DIRECTORY}"

file_downloaded=false
SFTP_PARAMS="open -e 'set sftp:auto-confirm yes;' -u ${SFTP_USERNAME},'${SFTP_PASSWORD}' sftp://adsbrain.sftp.ad.net:2222;"
FILE_NAME="Ad.Net - ${PROCESS_DATE}.xlsx"
DOWNLOADED_FILE_PATHNAME="/data/${FILE_NAME}"
FINAL_FILE_PATHNAME="/data/Ad.Net_${PROCESS_DATE}.csv"

echo "${SFTP_PARAMS}"

echo "Listing files from sftp server..."
lftp -c "${SFTP_PARAMS} ls -l ${SFTP_DIRECTORY}"

if [ -f "${FINAL_FILE_PATHNAME}" ]; then
  echo "Removing old file[${FINAL_FILE_PATHNAME}]..."
  rm -r "${FINAL_FILE_PATHNAME}"
fi

if [ -f "${DOWNLOADED_FILE_PATHNAME}" ]; then
  echo "Removing old file[${DOWNLOADED_FILE_PATHNAME}]..."
  rm -r "${DOWNLOADED_FILE_PATHNAME}"
fi

echo "Checking if file[${FILE_NAME}] exists in sftp..."
files_found=$(lftp -c "${SFTP_PARAMS} ls ${SFTP_DIRECTORY}" | grep "${FILE_NAME}" | wc -l)

if [ "${files_found}" -ne "1" ]; then
  echo "WARN: File[${FILE_NAME}] does not exist in sftp"
  echo "Trying to download file with date format: YYYY-M-D"
  
  FILE_NAME="Ad.Net - $(date +%Y-%-m-%-d -d "${PROCESS_DATE}").xlsx"
  DOWNLOADED_FILE_PATHNAME="/data/${FILE_NAME}"
  
  if [ -f "${DOWNLOADED_FILE_PATHNAME}" ]; then
    echo "Removing old file[${DOWNLOADED_FILE_PATHNAME}]..."
    rm -r "${DOWNLOADED_FILE_PATHNAME}"
  fi
  
  echo "Checking if file[${FILE_NAME}] exists in sftp..."
  files_found=$(lftp -c "${SFTP_PARAMS} ls ${SFTP_DIRECTORY}" | grep "${FILE_NAME}" | wc -l)
  if [ "${files_found}" -ne "1" ]; then
    echo "ERROR: File[${FILE_NAME}] does not exist in sftp"
    exit 1
  else
    echo "File[${FILE_NAME}] exist in sftp"
  fi
else
  echo "File[${FILE_NAME}] exist in sftp"
fi

echo "Downloading file[${FILE_NAME}]..."
lftp -c "${SFTP_PARAMS} get '${SFTP_DIRECTORY}/${FILE_NAME}' -o '${DOWNLOADED_FILE_PATHNAME}'"
  
if [ ! -f "${DOWNLOADED_FILE_PATHNAME}" ]; then
  echo "ERROR: File[${DOWNLOADED_FILE_PATHNAME}] does not exist in local"
  exit 1
else
  echo "File[${FILE_NAME}] downloaded from sftp server!"
fi

echo "Converting XLSX file to CSV..."
xlsx2csv --dateformat=%F "${DOWNLOADED_FILE_PATHNAME}" "${FINAL_FILE_PATHNAME}"

if [ -f "${FINAL_FILE_PATHNAME}" ]; then
  echo "CSV file created[${FINAL_FILE_PATHNAME}]..."
else
  echo "ERROR: Unable to generate CSV file[${FINAL_FILE_PATHNAME}]..."
  exit 1
fi

echo "Process completed successfully!"