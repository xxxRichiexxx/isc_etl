
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
        'isc',
        default_args=default_args,
        description='Получение данных из ИСК. Продажи дилеров.',
        start_date=dt.datetime(2020, 1, 1),
        schedule_interval='@monthly',
        catchup=True,
        max_active_runs=1
) as dag:

    data_types = [
            'sales',
        ]

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        daily_tasks = []

        for data_type in data_types:
            daily_tasks.append(
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

        date_check = BranchPythonOperator(
            task_id='date_check',
            python_callable=date_check,
            op_kwargs={
                'taskgroup': 'Загрузка_данных_в_stage_слой',
                },
        )

        do_nothing = DummyOperator(task_id='do_nothing')
        monthly_tasks = PythonOperator(
                    task_id='monthly_tasks',
                    python_callable=etl,
                    op_kwargs={
                        'data_type': 'sales',
                        'source_engine': source_engine,
                        'dwh_engine': dwh_engine,
                        'monthly_tasks': True,
                    },
                )
        collapse = DummyOperator(
            task_id='collapse',
            trigger_rule='none_failed',
        )

        daily_tasks >> date_check >> [do_nothing, monthly_tasks] >> collapse

    with TaskGroup('Загрузка_данных_в_dds_слой') as data_to_dds:

        data_types = [
            'dealer',
            'buyer',
        ]

        tasks = []

        for data_type in data_types:
            tasks.append(
                VerticaOperator(
                    task_id=f'dds_isc_{data_type}',
                    vertica_conn_id='vertica',
                    sql=f'scripts/dds_{data_type}.sql',
                    params={
                        'delta_1': dt.timedelta(days=1),
                        'delta_2': dt.timedelta(days=4),
                    }
                )
            )

        sales = VerticaOperator(
            task_id='dds_isc_sales',
            vertica_conn_id='vertica',
            sql='scripts/dds_sales.sql',
            params={
                'delta_1': dt.timedelta(days=1),
                'delta_2': dt.timedelta(days=4),
            }
        )

        tasks >> sales

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        dm_isc_sales_v = VerticaOperator(
                    task_id='dm_isc_sales_v',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_isc_sales_v.sql',
                )

        dm_isc_dealer_sales_RF = VerticaOperator(
                    task_id='dm_isc_dealer_sales_RF',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_isc_dealer_sales_RF.sql',
                    params={
                        'delta_1': dt.timedelta(days=1),
                        'delta_2': dt.timedelta(days=4),
                    }
                )

        dm_isc_sales_RF_CIS = VerticaOperator(
                    task_id='dm_isc_sales_RF_CIS',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_isc_sales_RF_CIS.sql',
                    params={
                        'delta_1': dt.timedelta(days=1),
                        'delta_2': dt.timedelta(days=4),
                    }
                )

        dm_isc_sales_v_for_model = VerticaOperator(
                    task_id='dm_isc_sales_v_for_model',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_isc_sales_v_for_model.sql',
                )

        dm_isc_sales_v_detailed = VerticaOperator(
                    task_id='dm_isc_sales_v_detailed',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_isc_sales_v_detailed.sql',
                )

        [dm_isc_sales_v, dm_isc_dealer_sales_RF,
         dm_isc_sales_v_for_model, dm_isc_sales_v_detailed]

    with TaskGroup('Проверки') as data_checks:

        dm_isc_sales_v_check = VerticaOperator(
                    task_id='dm_isc_sales_v_check',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_isc_sales_v_check.sql',
                    params={
                        'dm': 'dm_isc_sales_v',
                    }
                )

        marts = ('dm_isc_dealer_sales_RF', 'dm_isc_sales_RF_CIS')
        check_tasks = []

        for mart in marts:
            check_tasks.append(
                VerticaOperator(
                    task_id=f'{mart}_check',
                    vertica_conn_id='vertica',
                    sql='scripts/dm_isc_sales_t_check.sql',
                    params={
                        'dm': mart,
                    }
                )
            )

        [dm_isc_sales_v_check] + check_tasks

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_checks >> end
