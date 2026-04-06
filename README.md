# Лабораторная работа №2: Apache Airflow

**Студент:** Полина Фризен

**Тема:** Кинотеатр (залы, фильмы, сеансы, билеты, кассовые сборы)

---

## Цель работы
Научиться создавать системы работы с потоками данных с использованием Apache Airflow.

---

## Выполненные задачи
- [x] Развернут Apache Airflow на macOS
- [x] Разработан DAG для обработки данных кинотеатра
- [x] DAG запущен и работает

---

## Скриншоты

### 1. Список DAG'ов в Airflow
<img width="1470" height="956" alt="1_dags_list" src="https://github.com/user-attachments/assets/da2a2e93-3abc-4dd5-afe8-9df189b13a99" />


### 2. Graph View (успешное выполнение задач)
<img width="1470" height="956" alt="2_graph_view" src="https://github.com/user-attachments/assets/4c254b80-8366-4b64-8798-2a800bad5ccd" />


### 3. Отчет о кассовых сборах
<img width="1470" height="956" alt="3_report" src="https://github.com/user-attachments/assets/9824fd8c-827a-4cd6-a20c-0cc02c45baf0" />


---

## Код DAG

```python
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
    print(f"Total revenue: {total} RUB")

dag = DAG('test_cinema', start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False)

start = DummyOperator(task_id='start', dag=dag)
task1 = PythonOperator(task_id='generate_data', python_callable=generate, dag=dag)
task2 = PythonOperator(task_id='make_report', python_callable=report, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> task1 >> task2 >> end
