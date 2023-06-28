
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.operators.python import BranchPythonOperator

from isc_etl.scripts.collable import etl, date_check


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
        'isc_orders',
        default_args=default_args,
        description='Получение данных из ИСК. Заявки дилеров',
        start_date=dt.datetime(2018, 1, 1),
        schedule_interval='@monthly',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        months = [1, 2, 3, 4]

        tasks = []

        for month in months:
            tasks.append(
                PythonOperator(
                    task_id=f'get_orders_{month}',
                    python_callable=etl,
                    op_kwargs={
                        'data_type': 'orders',
                        'source_engine': source_engine,
                        'dwh_engine': dwh_engine,
                        'month': month
                    },
                )
            )

        tasks 

    with TaskGroup('Загрузка_данных_в_dds_слой') as data_to_dds:

        counteragent = VerticaOperator(
            task_id='dds_isc_counteragent',
            vertica_conn_id='vertica',
            sql='scripts/dds_isc_counteragent_pt_2.sql',
            params={
                'delta_1': dt.timedelta(days=1),
                'delta_2': dt.timedelta(days=4),
            }
        )

        orders = VerticaOperator(
            task_id='dds_isc_orders',
            vertica_conn_id='vertica',
            sql='scripts/dds_isc_orders.sql',
            params={
                'delta_1': dt.timedelta(days=1),
                'delta_2': dt.timedelta(days=4),
            }
        )

        counteragent >> orders

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        pass

        # dm_isc_realization_v = VerticaOperator(
        #             task_id='dm_isc_realization_v',
        #             vertica_conn_id='vertica',
        #             sql='scripts/dm_isc_realization_v.sql',
        #         )
        
    with TaskGroup('Проверки') as data_checks:

        pass

        # dm_isc_realization_v_check = VerticaOperator(
        #             task_id='dm_isc_realization_v_check',
        #             vertica_conn_id='vertica',
        #             sql='scripts/dm_isc_realization_v_check.sql',
        #             params={
        #                 'dm': 'dm_isc_realization_v',
        #             }
        #         )

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_checks >> end
