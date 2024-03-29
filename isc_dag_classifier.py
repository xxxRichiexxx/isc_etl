
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

from isc_etl.scripts.collable import etl, date_check, contracting_calculate


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
        'isc_classifier',
        default_args=default_args,
        description='Получение данных из ИСК. Классификатор.',
        start_date=dt.datetime(2023, 8, 13),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        tables = (
            'property_value_guide',
            'gaz_property_binding_guide',
            # 'СправочникПривязкаСвойствАвтобусы',
            'nomenclature_guide',
            'properties_guide',
        )

        tasks = []

        for table in tables:
            tasks.append(
                PythonOperator(
                    task_id=f'get_{table}',
                    python_callable=etl,
                    op_kwargs={
                        'data_type': table,
                        'source_engine': source_engine,
                        'dwh_engine': dwh_engine,
                    },
                )
            )

        tasks 

    with TaskGroup('Загрузка_данных_в_dds_слой') as data_to_dds:

        nomenclature_guide = VerticaOperator(
            task_id='nomenclature_guide',
            vertica_conn_id='vertica',
            sql='scripts/dds_isc_nomenclature_guide.sql',
            params={
                'delta_1': dt.timedelta(days=1),
                'delta_2': dt.timedelta(days=4),
            }
        )
        

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        pass

        
    with TaskGroup('Проверки') as data_checks:

        pass


    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_checks >> end
