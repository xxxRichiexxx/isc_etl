import pandas as pd
import datetime as dt


def extract(source_engine, data_type, execution_date):
    """Извлечение данных из источника."""

    with open(fr'/home/da/airflow/dags/isc_etl/scripts/stg_{data_type}.sql', 'r') as f:
        date_from = execution_date.replace(day=1)
        date_to = date_from.replace(month=date_from.month + 1) - dt.timedelta(days=1)
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
            DELETE FROM sttgaz.stage_isc_{data_type}
            WHERE DATE_TRUNC('MONTH', load_date) = '{execution_date.replace(day=1)}'
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
    load(dwh_engine, data, data_type, execution_date)



def date_check(taskgroup, **context):
    execution_date = context['execution_date'].date()
    if execution_date.day == 1:
        return taskgroup + '.' + 'monthly_tasks'
    return taskgroup + '.' + 'do_nothing'
