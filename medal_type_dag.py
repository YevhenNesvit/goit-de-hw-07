from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
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
    def branch_medal_choice(**kwargs):
        medal = choose_medal_type()
        if medal == 'Bronze':
            return 'calc_Bronze'
        elif medal == 'Silver':
            return 'calc_Silver'
        else:
            return 'calc_Gold'

    choose_medal = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=branch_medal_choice,
        provide_context=True
    )

    # Завдання 3: обчислення для кожного типу медалі
    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id='neo_data',
        sql="""
            INSERT INTO olympic_results_nesvit (medal_type, count, created_at)
            SELECT 'Bronze', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id='neo_data',
        sql="""
            INSERT INTO olympic_results_nesvit (medal_type, count, created_at)
            SELECT 'Silver', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
        """
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id='neo_data',
        sql="""
            INSERT INTO olympic_results_nesvit (medal_type, count, created_at)
            SELECT 'Gold', COUNT(*), NOW()
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
        """
    )

    # Завдання 4: затримка
    delay_task = PythonOperator(
        task_id='add_delay',
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
        timeout=60,
        poke_interval=5,
        mode='poke',
    )

    # Побудова DAG
    create_table >> choose_medal >> [calc_Bronze, calc_Silver, calc_Gold] >> delay_task >> check_latest_record
