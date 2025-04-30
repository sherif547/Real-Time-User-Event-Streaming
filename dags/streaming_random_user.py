from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json, logging, time, requests
from kafka import KafkaProducer
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

default_args = {
    'owner': 'sherif',
    'start_date': datetime(2025, 4, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_data():
    res = requests.get("https://jsonplaceholder.typicode.com/users")
    res.raise_for_status()
    return res.json()[0]

def format_data(user):
    return {
        'id': user['id'],
        'name': user['name'],
        'username': user['username'],
        'email': user['email'],
        'address': f"{user['address']['street']} {user['address']['city']} {user['address']['zipcode']}",
        'post_code': user['address']['zipcode'],
        'phone_number': user['phone'],
        'company_name': user['company']['name'],
    }

def stream_data():
    producer = KafkaProducer(
        bootstrap_servers=['broker:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    start = time.time()
    while time.time() - start < 60:
        try:
            msg = format_data(get_data())
            producer.send('users_created', value=msg)
            producer.flush()
            logging.info(f"Sent: {msg}")
            time.sleep(1)
        except Exception as e:
            logging.error(f"Stream error: {e}")
            time.sleep(5)

with DAG(
    'streaming_random_user_1',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example'],
) as dag:
    PythonOperator(
        task_id='stream_random_user',
        python_callable=stream_data,
    )
