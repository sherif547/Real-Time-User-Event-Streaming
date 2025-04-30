#!/usr/bin/env python3
import time, os
import psycopg2
from psycopg2 import sql

# ← CHANGED: wait for Postgres, then ensure our table exists
host = os.getenv("POSTGRES_HOST", "postgres")
port = os.getenv("POSTGRES_PORT", 5432)
user = os.getenv("POSTGRES_USER", "stream")
password = os.getenv("POSTGRES_PASSWORD", "stream")
dbname = os.getenv("POSTGRES_DB", "streamdb")

# wait until Postgres is up
while True:
    try:
        conn = psycopg2.connect(host=host, port=port,
                                user=user, password=password,
                                dbname=dbname)
        break
    except psycopg2.OperationalError:
        time.sleep(1)

cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS create_users (
        id            INT PRIMARY KEY,
        name          TEXT,
        username      TEXT,
        email         TEXT,
        address       TEXT,
        post_code     TEXT,
        phone_number  TEXT,
        company_name  TEXT
    );
""")
conn.commit()
cursor.close()
conn.close()
print("✅ Table create_users is ready in Postgres")
