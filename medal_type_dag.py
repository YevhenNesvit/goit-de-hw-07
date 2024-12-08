from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
import random
import time
from datetime import datetime

# Функція для випадкового вибору медалі
def choose_medal_type():
    return random.choice(['Bronze', 'Silver', 'Gold'])

# Функція для затримки
def add_delay():
    time.sleep(35)  # 35 секунд для перевірки сенсора

# Створення DAG
with DAG(
    'medal_type_dag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 12, 8, 0, 0),
    },
    description='DAG для обробки медалей',
    schedule_interval=None,
    catchup=False,
    tags=["yevhen_nesvit"]
) as dag:

    # Завдання 1: створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='neo_data',  # Заздалегідь налаштоване з’єднання
        sql="""
            CREATE TABLE IF NOT EXISTS olympic_results_nesvit (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                count INT,
                created_at DATETIME
            );
        """
    )

    # Завдання 2: вибір медалі
    choose_medal = BranchPythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal_type
    )

    # Завдання 3: завдання для кожного типу медалей
    for medal in ['Bronze', 'Silver', 'Gold']:
        count_records = MySqlOperator(
            task_id=f'count_{medal.lower()}_records',
            mysql_conn_id='neo_data',
            sql=f"""
                INSERT INTO olympic_results_nesvit (medal_type, count, created_at)
                SELECT '{medal}', COUNT(*), NOW()
                FROM olympic_dataset.athlete_event_results
                WHERE medal = '{medal}';
            """
        )
        choose_medal >> count_records

    # Завдання 5: затримка
    delay_task = PythonOperator(
        task_id='add_delay',
        python_callable=add_delay,
        trigger_rule='one_success',  # Виконується, якщо успішно завершено хоча б одне попереднє завдання
    )

    # Завдання 6: сенсор для перевірки часу запису
    check_latest_record = SqlSensor(
        task_id='check_latest_record',
        conn_id='neo_data',
        sql="""
            SELECT 1
            FROM olympic_results_nesvit
            WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
            ORDER BY created_at DESC
            LIMIT 1;
        """,
        timeout=60,
        poke_interval=5,
        mode='poke',
    )

    # Побудова DAG
    create_table >> choose_medal >> delay_task >> check_latest_record
    