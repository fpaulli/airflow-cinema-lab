from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import json
import os

DATA_DIR = os.path.expanduser("~/airflow_home/data")
os.makedirs(DATA_DIR, exist_ok=True)

def generate():
    tickets = []
    for i in range(100):
        tickets.append({
            "id": i,
            "movie": ["Avatar", "Oppenheimer", "Barbie"][i % 3],
            "price": 400 + (i % 3) * 50
        })
    with open(f"{DATA_DIR}/tickets.json", "w") as f:
        json.dump(tickets, f)
    print(f"Created {len(tickets)} tickets")

def report():
    with open(f"{DATA_DIR}/tickets.json", "r") as f:
        tickets = json.load(f)
    total = sum(t["price"] for t in tickets)
    print("
" + "="*40)
    print("BOX OFFICE REPORT")
    print(f"Total revenue: {total} RUB")
    print(f"Total tickets: {len(tickets)}")
    print("="*40)

dag = DAG(
    "test_cinema",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
)

start = DummyOperator(task_id="start", dag=dag)
task1 = PythonOperator(task_id="generate_data", python_callable=generate, dag=dag)
task2 = PythonOperator(task_id="make_report", python_callable=report, dag=dag)
end = DummyOperator(task_id="end", dag=dag)

start >> task1 >> task2 >> end
