#!/bin/bash

# -------- LOAD LIB --------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/libs/db_lib.sh"

echo "[+] Reading latest PENDING job..."

JOB_DATA=$(read_latest_job_request)

if [ -z "$JOB_DATA" ]; then
  echo "[!] No PENDING jobs found"
  exit 0
fi

# -------- SAFE PARSING (TAB-BASED) --------
IFS=$'\t' read -r JOB_ID FILE_NAME CONTENT_TYPE FILE_B64 <<< "$JOB_DATA"

echo "[+] Job ID: $JOB_ID"
insert_report "$JOB_ID" "STARTED" "Job started"

# -------- DECODE FILE --------
TMP_FILE="tmp_$FILE_NAME"

echo "$FILE_B64" | base64 -d > "$TMP_FILE"

echo "[+] File decoded"
insert_report "$JOB_ID" "PROCESSING" "File decoded from database"

# -------- SIMULATE PROCESSING --------
echo "[+] Processing file..."
sleep 2

insert_report "$JOB_ID" "PROCESSING" "File processed (simulated)"

# -------- SEND TO KAFKA --------
echo "[+] Sending to Kafka..."

docker exec -i dev-kafka-1 kafka-console-producer \
--bootstrap-server localhost:9092 \
--topic sanitizer_in <<EOF
$(cat "$TMP_FILE")
EOF

if [ $? -eq 0 ]; then
  echo "[+] Kafka send successful"
  insert_report "$JOB_ID" "PROCESSING" "Sent to Kafka topic sanitizer_in"
else
  echo "[!] Kafka send failed"
  insert_report "$JOB_ID" "FAILED" "Kafka send failed"
  exit 1
fi

# -------- UPDATE STATUS --------
update_job_request_status "$JOB_ID" "COMPLETED"
insert_report "$JOB_ID" "COMPLETED" "Job completed successfully"

echo "[+] Job completed"

# -------- CLEANUP --------
rm -f "$TMP_FILE"

echo "[+] Done"
