#!/usr/bin/env python3
import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType
)

# ─────────────────────────────────────────────────────────────────────────────
# 1) Postgres connection & table-creation helpers
# ─────────────────────────────────────────────────────────────────────────────
PG_HOST     = "postgres"
PG_PORT     = 5432
PG_DB       = "streamdb"
PG_USER     = "stream"
PG_PASS     = "stream"
TABLE_NAME  = "create_users"

def get_pg_conn():
    """Open a psycopg2 connection to Postgres."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )

def init_postgres_table():
    """Create the target table if it doesn't already exist."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id            INTEGER ,
        name          TEXT,
        username      TEXT,
        email         TEXT,
        address       TEXT,
        post_code     TEXT,
        phone_number  TEXT,
        company_name  TEXT
    );
    """
    conn = get_pg_conn()
    cur  = conn.cursor()
    cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"✅ Table `{TABLE_NAME}` is ready in Postgres.")

# ─────────────────────────────────────────────────────────────────────────────
# 2) Per-batch writer
# ─────────────────────────────────────────────────────────────────────────────
def foreach_batch(batch_df, batch_id):
    """
    Called on each micro-batch of data. Inserts all rows
    into Postgres in a single transaction.
    """
    count = batch_df.count()
    if count == 0:
        logging.info(f"[batch {batch_id}] – no rows to write.")
        return

    rows = batch_df.collect()
    conn = get_pg_conn()
    cur  = conn.cursor()

    insert_sql = f"""
      INSERT INTO {TABLE_NAME}
        (id, name, username, email, address, post_code, phone_number, company_name)
      VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
      
    """

    for r in rows:
        cur.execute(insert_sql, (
            r.id, r.name, r.username, r.email,
            r.address, r.post_code, r.phone_number, r.company_name
        ))
    conn.commit()
    cur.close()
    conn.close()

    logging.info(f"[batch {batch_id}] – wrote {count} rows to `{TABLE_NAME}`")

# ─────────────────────────────────────────────────────────────────────────────
# 3) Main: build Spark, read Kafka, parse JSON, start streaming
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_postgres_table()

    spark = (
        SparkSession.builder
        .appName("KafkaToPostgresStreaming")
        .config(
            "spark.jars.packages",
            # Kafka source and Postgres driver
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
            "org.postgresql:postgresql:42.5.1"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 3.1) define the JSON schema coming from Kafka
    schema = StructType([
        StructField("id",           IntegerType(), False),
        StructField("name",         StringType(),  False),
        StructField("username",     StringType(),  False),
        StructField("email",        StringType(),  False),
        StructField("address",      StringType(),  False),
        StructField("post_code",    StringType(),  False),
        StructField("phone_number", StringType(),  False),
        StructField("company_name", StringType(),  False),
    ])

    # 3.2) subscribe to Kafka topic
    kafka_df = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", "broker:9092")
             .option("subscribe", "users_created")
             .option("startingOffsets", "earliest")
             .option("failOnDataLoss", "false")    #
             .load()
    )

    # 3.3) parse the JSON payload
    parsed = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), schema).alias("d"))
        .select("d.*")
    )

    # 3.4) start the streaming query with our per-batch writer
    query = (
        parsed.writeStream
              .outputMode("update")
              .foreachBatch(foreach_batch)
              .option("checkpointLocation", "/tmp/checkpoints/kafka2pg")
              .start()
    )

    logging.info("🚀 Streaming started: reading from Kafka → writing to Postgres.")
    query.awaitTermination()
