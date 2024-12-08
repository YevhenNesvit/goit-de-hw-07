from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
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
    schedule=None,
    catchup=False,
    tags=["yevhen_nesvit"]
) as dag:

    # Завдання 1: створення таблиці
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='neo_data',  # Заздалегідь налаштоване з’єднання
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
    def branch_medal_choice(**kwargs):
        medal = choose_medal_type()
        if medal == 'Bronze':
            return 'calc_Bronze'
        elif medal == 'Silver':
            return 'calc_Silver'
        else:
            return 'calc_Gold'

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=choose_medal_type
    )

    choose_medal = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=branch_medal_choice,
        provide_context=True
    )

    # Завдання 3: обчислення для кожного типу медалі
    calc_Bronze = SQLExecuteQueryOperator(
        task_id='calc_Bronze',
        conn_id='neo_data',
        sql="""
            INSERT INTO olympic_results_nesvit (medal_type, count, created_at)
            SELECT 'Bronze', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """
    )

    calc_Silver = SQLExecuteQueryOperator(
        task_id='calc_Silver',
        conn_id='neo_data',
        sql="""
            INSERT INTO olympic_results_nesvit (medal_type, count, created_at)
            SELECT 'Silver', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
        """
    )

    calc_Gold = SQLExecuteQueryOperator(
        task_id='calc_Gold',
        conn_id='neo_data',
        sql="""
            INSERT INTO olympic_results_nesvit (medal_type, count, created_at)
            SELECT 'Gold', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
        """
    )

    # Завдання 4: затримка
    delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=add_delay,
        trigger_rule='one_success',  # Виконується, якщо успішно завершено хоча б одне попереднє завдання
    )

    # Завдання 5: сенсор для перевірки часу запису
    check_latest_record = SqlSensor(
        task_id='check_for_correctness',
        conn_id='neo_data',
        sql="""
            SELECT 1
            FROM olympic_results_nesvit
            WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30
            ORDER BY created_at DESC
            LIMIT 1;
        """,
        timeout=30,
        poke_interval=5,
        mode='poke',
    )

    # Побудова DAG
    create_table >> pick_medal >> choose_medal >> [calc_Bronze, calc_Silver, calc_Gold] >> delay_task >> check_latest_record
