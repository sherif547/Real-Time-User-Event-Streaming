# ⚡ Real-Time Streaming Pipeline with Kafka, Spark, and PostgreSQL

This project implements a real-time data pipeline using modern data engineering tools. It streams **user creation events** from an API, processes them with **Apache Spark Structured Streaming**, and stores them in a **PostgreSQL** database. Everything runs locally via **Docker Compose**.

---

## 🧰 Tech Stack

- **Apache Kafka** – Message broker for ingesting streaming data
- **Apache Spark Structured Streaming** – Real-time data processing engine
- **PostgreSQL** – Final data sink with schema enforcement
- **Docker Compose** – Local environment orchestration
- **REST API (FastAPI)** – For simulating user creation events

---

## 📈 Use Case

We simulate a system that generates user data in real-time (e.g. from a sign-up form). These events are:

1. Generated via a FastAPI-based endpoint
2. Pushed to Kafka as JSON messages
3. Read and processed by Spark (structured streaming)
4. Written to PostgreSQL with batch commits
5. Checkpoints are used for state recovery and fault tolerance

---

## 🗃️ Project Structure

```bash
.
├── api/                  # FastAPI server to simulate user creation events
├── spark/                # Spark Structured Streaming app
├── postgres/             # Database scripts and Docker config
├── docker-compose.yml    # Orchestration of all services
└── README.md



. ├── spark_stream.py # Spark Structured Streaming logic ├── streaming_random_user.py # Airflow DAG that pulls from API and sends to Kafka ├── init_db.py # Initializes the PostgreSQL table ├── docker-compose.yml # Sets up Kafka, Spark, Postgres, Airflow ├── requirements.txt # Python dependencies └── README.md # Project documentation

---

## 📸 Architecture Diagram


---

## 🧪 How to Run

1. Clone this repo:
   ```bash
   git clone https://github.com/YOUR_USERNAME/streaming-pipeline.git
   cd streaming-pipeline


📍 Features
✔️ Real-time ingestion and persistence

✔️ Resilient batch streaming with checkpointing

✔️ Fully containerized and reproducible

✔️ Minimal setup with Docker Compose

✔️ Schema enforcement and deduplication


