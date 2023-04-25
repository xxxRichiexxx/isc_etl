import pandas as pd
import datetime as dt


def extract(source_engine, data_type, execution_date):
    """Извлечение данных из источника."""

    print('ИЗВЛЕЧЕНИЕ ДАННЫХ')
    with open(
        fr'/home/da/airflow/dags/isc_etl/scripts/stg_{data_type}.sql', 'r'
    ) as f:
        date_from = execution_date.replace(day=1)
        date_to = (execution_date.replace(day=28) + dt.timedelta(days=4)) \
            .replace(day=1) - dt.timedelta(days=1)
        command = f.read().format(date_from, date_to)

    print(command)

    return pd.read_sql_query(
        command,
        source_engine,
        dtype_backend='pyarrow',
    )


def transform(data, execution_date, data_type):
    """Преобразование/трансформация данных."""

    print('ТРАНСФОРМАЦИЯ ДАННЫХ')
    if data_type == 'sales':
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
            "BalanceAtBeginningOfPeriodOnRoad",
            "BalanceAtEndOfPeriodOnRoad",
            "ProductIdentifier",
            "DirectionOfImplementationByApplication",
            "DirectionOfImplementationWithUKP",
            "DirectionOfImplementationPlace",
            "BuildOption",
            "BuildOptionСollapsed",
            "Engine",
            "clientsHolding",
            "BalanceAtBeginningOfPeriod",
            "BalanceAtEndOfPeriod",
        ]
    elif data_type == 'realization':
        data.columns = [
            "Client",
            "Recipient",
            "Doc",
            "BuildOption",
            "BuildOptionСollapsed",
            "PaymentType",
            "PproductTypeByDivision",
            "vin",
            "AttachmentDate",
            "DischargeDate",
            "Engine",
            "Day",
            "Division",
            "Contract",
            "Month",
            "PlaneMonth",
            "DirectionOfImplementation",
            "DirectionOfImplementationWithUKP",
            "AttachmentNumber",
            "DischargeNumber",
            "ProductIdentifier",
            "RecipientFullName",
            "Company",
            "Seller",
            "Warehouse",
            "Manufacturer",
            "Product",
            "ProductCode65",
            "ProductNumber",
            "Color",
            "RequestNumber",
            "RequestDischarge",
            "RequestResource",
            "ClientHolding",
            "Availability",
            "Turnover",
            "ExpenseVAT",
            "TurnoverWithoutVAT",
            "Price",
            "RefundAmount",
            "RefundsVAT",
            "RefundWithoutVAT",
            "SumMO",
            "MOVAT",
            "SumMOTotal",
            "ProductIdentifier2",
            "DocID",
            "RecipientID",
            "ClassifierCabType",
            "ClassifierDrive",
            "ClassifierDetailedByDivision",
            "ClassifierProductType",
            "ClassifierGBO",
            "ClassifierNumberOfSeats",
            "ClassifierEcologicalClass",
        ]

    data['load_date'] = execution_date.replace(day=1)
    return data


def load(dwh_engine, data, data_type, execution_date):
    """Загрузка данных в хранилище."""

    print('ЗАГРУЗКА ДАННЫХ')
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


def etl(source_engine, dwh_engine, data_type, monthly_tasks=False, **context):
    """Запускаем ETL-процесс для заданного типа данных."""
    if monthly_tasks:
        execution_date = (context['execution_date'].date().replace(day=1) - dt.timedelta(days=1)) \
                            .replace(day=1)
    else:
        execution_date = context['execution_date'].date()
    data = extract(source_engine, data_type, execution_date)
    data = transform(data, execution_date, data_type)
    if data_type == 'sales': 
        context['ti'].xcom_push(
            key='SoldAtRetail',
            value=sum(data['SoldAtRetail'])
        )
        context['ti'].xcom_push(
            key='SoldToIndividuals',
            value=sum(data['SoldToIndividuals'])
        )
        context['ti'].xcom_push(
            key="BalanceAtBeginningOfPeriodOnRoad",
            value=sum(data["BalanceAtBeginningOfPeriodOnRoad"])
        )
        context['ti'].xcom_push(
            key="BalanceAtEndOfPeriodOnRoad",
            value=sum(data["BalanceAtEndOfPeriodOnRoad"])
        )
    elif data_type == 'realization':
        context['ti'].xcom_push(
            key='RealizationCount',
            value=sum(data['Availability'])
        )

    load(dwh_engine, data, data_type, execution_date)


def date_check(taskgroup, **context):
    execution_date = context['execution_date'].date()
    if execution_date.day in (1, 2, 3, 4):
        return taskgroup + '.' + 'monthly_tasks'
    return taskgroup + '.' + 'do_nothing'
