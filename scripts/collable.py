import pandas as pd
import datetime as dt


def extract(source_engine, data_type, execution_date):
    """Извлечение данных из источника."""

    with open(fr'/home/da/airflow/dags/isc_etl/scripts/stg_{data_type}.sql', 'r') as f:
        date_from = execution_date.replace(day=1)
        date_to = (execution_date.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
        command = f.read().format(date_from, date_to)

    print(command)

    return pd.read_sql_query(
        command,
        source_engine,
    )


def transform(data, execution_date):
    """Преобразование/трансформация данных."""
    data.columns = [
        "ModelYear",
        "vin",
        "division",
        "code",
        "SalesTerritory",
        "Recipient",
        "RecipientFullName",
        "BuyersRegion",
        "FinalBuyer",
        "BuyerINN",
        "okved",
        "LineOfWork",
        "ScopeOfUse",
        "ImplementationProgram",
        "ShipmentDate",
        "DateOfSale",
        "DateOfEntryIntoDB",
        "SoldAtRetail",
        "SoldToIndividuals",
        "BalanceAtBeginningOfPeriod",
        "BalanceAtEndOfPeriod",
        "ProductIdentifier",
    ]

    data['load_date'] = execution_date
    return data


def load(dwh_engine, data, data_type, execution_date):
    """Загрузка данных в хранилище."""

    if not data.empty:

        print(data)

        command = f"""
            SELECT DROP_PARTITIONS(
                'sttgaz.stage_isc_{data_type}',
                '{execution_date.replace(day=1)}',
                '{execution_date.replace(day=1)}'
            );
        """
        print(command)

        dwh_engine.execute(command)

        data.to_sql(
            f'stage_isc_{data_type}',
            dwh_engine,
            schema='sttgaz',
            if_exists='append',
            index=False,
        )
    else:
        print('Нет новых данных для загрузки.')


def etl(source_engine, dwh_engine, data_type, **context):
    """Запускаем ETL-процесс для заданного типа данных."""
    execution_date = context['execution_date'].date()
    data = extract(source_engine, data_type, execution_date)
    data = transform(data, execution_date)
    context['ti'].xcom_push(key='SoldAtRetail', value=sum(data['SoldAtRetail']))
    context['ti'].xcom_push(key='SoldToIndividuals', value=sum(data['SoldToIndividuals']))
    context['ti'].xcom_push(key='BalanceAtBeginningOfPeriod', value=sum(data['BalanceAtBeginningOfPeriod']))
    context['ti'].xcom_push(key='BalanceAtEndOfPeriod', value=sum(data['BalanceAtEndOfPeriod']))           
    load(dwh_engine, data, data_type, execution_date)


def date_check(taskgroup, **context):
    execution_date = context['execution_date'].date()
    if execution_date.day == 1:
        return taskgroup + '.' + 'monthly_tasks'
    return taskgroup + '.' + 'do_nothing'
