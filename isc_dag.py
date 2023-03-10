
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.vertica_operator import VerticaOperator

from scripts.collable import etl


source_con = BaseHook.get_connection('isc')
source_username = source_con.login
source_password = quote(source_con.password)
source_host = source_con.host
source_db = source_con.schema
eng_str = fr'mssql://{source_username}:{source_password}@{source_host}/{source_db}?driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=yes'
source_engine = sa.create_engine(eng_str)

dwh_con = BaseHook.get_connection('vertica')
ps = quote(dwh_con.password)
dwh_engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
)


default_args = {
    'owner': 'Швейников Андрей',
    'email': ['xxxRichiexxx@yandex.ru'],
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'isc',
        default_args=default_args,
        description='Получение данных из ИСК.',
        start_date=dt.datetime(2022, 2, 1),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs=1
) as dag:
    
    data_types = [
            'sales',
        ]

    start = DummyOperator(task_id='Начало')

    with TaskGroup(f'Загрузка_данных_из_ИСК') as data_to_stage:

        tasks = []

        for data_type in data_types:
            tasks.append(
                PythonOperator(
                    task_id=f'get_{data_type}',
                    python_callable=etl,
                    op_kwargs={
                        'data_type': data_type,
                        'source_engine': source_engine,
                        'dwh_engine': dwh_engine,
                    },
                )
            )

        tasks
