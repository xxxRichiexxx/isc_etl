import pandas as pd


def extract(source_engine, data_type, execution_date):
    """Извлечение данных из источника."""

    with open(fr'/home/da/airflow/dags/isc_etl/scripts/stg_{data_type}.sql', 'r') as f:
        command = f.read().format(execution_date)

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

        vins = tuple(data['vin'].values)

        dwh_engine.execute(
            f"""
            DELETE FROM sttgaz.stage_isc_{data_type}
            WHERE vin IN {vins} 
            """
        )

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
