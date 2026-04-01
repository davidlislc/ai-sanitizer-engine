import mysql.connector
from kafka import KafkaProducer
import json

# -------- CONFIG --------
DB_CONFIG = {
    "host": "localhost",
    "user": "user",
    "password": "password",
    "database": "sanitizer_db"
}

KAFKA_BROKER = "localhost:9092"
TOPIC = "sanitizer_in"

CSV_FILE = "/home/luca/net2300/sanitizer-engine/samplefiles/2-csv-20260316221533.csv"

# -------- STEP 1: READ CSV --------
with open(CSV_FILE, "rb") as f:   # ⚠️ binary for blob
    file_content = f.read()

print("[+] CSV file loaded")

# -------- STEP 2: INSERT INTO DB --------
conn = mysql.connector.connect(**DB_CONFIG)
cur = conn.cursor()

cur.execute("""
INSERT INTO job_request (
    file_content,
    file_content_content_type,
    status,
    file_type,
    request_type,
    priority,
    file_name
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
""", (
    file_content,
    "text/csv",
    "PENDING",
    "csv",
    "sanitize",
    "high",
    CSV_FILE.split("/")[-1]
))

conn.commit()
job_id = cur.lastrowid

print(f"[+] Inserted into job_request with ID: {job_id}")

# -------- STEP 3: READ BACK --------
cur.execute("""
SELECT file_name, file_content
FROM job_request
WHERE id = %s
""", (job_id,))

row = cur.fetchone()
file_name, content = row

print("[+] Retrieved record from DB")

# -------- STEP 4: BUILD JSON --------
data = {
    "payload": content.decode(errors="ignore"),
    "metadata": {
        "file_name": file_name,
        "source": "job_request"
    }
}

print("[+] JSON constructed")

# -------- STEP 5: SEND TO KAFKA --------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send(TOPIC, data)
producer.flush()

print("[+] Sent to Kafka topic:", TOPIC)

# -------- CLEANUP --------
cur.close()
conn.close()
