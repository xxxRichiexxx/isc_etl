import pandas as pd
import datetime as dt

from sqlalchemy import text


def extract(source_engine, data_type, execution_date):
    """Извлечение данных из источника."""

    print('ИЗВЛЕЧЕНИЕ ДАННЫХ')

    date_from = execution_date.replace(day=1)
    date_to = (execution_date.replace(day=28) + dt.timedelta(days=4)) \
        .replace(day=1) - dt.timedelta(days=1)

    with open(
        fr'/home/da/airflow/dags/isc_etl/scripts/stg_{data_type}.sql', 'r'
    ) as f:
        command = f.read().format(date_from, date_to, data_type)

    print(command)

    return pd.read_sql_query(
        command,
        source_engine,
        dtype_backend='pyarrow',
        na_values='',
    )


def transform(data, execution_date, data_type):
    """Преобразование/трансформация данных."""

    print('ТРАНСФОРМАЦИЯ ДАННЫХ')
    print(data)
    
    if data_type == 'sales':
        data.columns = [
            "ModelYear",
            "vin",
            "division",
            "code",
            "SalesTerritory",  ######################################  Территория площадки
            "RecipientID",
            "Recipient",
            "RecipientFullName",
            "BuyersRegion", ########################### Территория покупателя
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
            "DiscountCRMTotal",
            "DiscountCRMDealer",
            "CustomerFromDiscountSystem",
            "StoyankaID",
            "Stoyanka",
            "StoyankaCityID",
            "StoyankaCity",
            "NomenclatureID",
            "ClassifierCabType",
            "ClassifierDrive",
            "ClassifierDetailedByDivision",
            "ClassifierProductType",
            "ClassifierGBO",
            "ClassifierNumberOfSeats",
            "ClassifierEcologicalClass",
        ]
    elif data_type == 'realization':
        data.columns = [
            "Client",               #Клиент
            "DealersUnit",          #ПлощадкаДилера
            "DealersName",          #КонтрагентПлощадкиДилера
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
            "Recipient",                    #Получатель
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
            "ClientHolding",                #ХолдингКонечныйКлиент
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
            "DealersUnitID",                #ПлощадкаДилера_Ид
            "ClassifierCabType",
            "ClassifierDrive",
            "ClassifierDetailedByDivision",
            "ClassifierProductType",
            "ClassifierGBO",
            "ClassifierNumberOfSeats",
            "ClassifierEcologicalClass",
        ]
    elif data_type == 'orders':
        data.columns = [
            "ProductCode65",
            "Color",
            "BuildOption",
            "ModelYear",
            "PriznakRezervirovania",  --------------
            "IGC",
            "DirectionOfImplementation",
            "Buyer",
            "ShipmentStatus",
            "Status",
            "ContractPeriod",
            "ShipmentMonth",
            "ProductionMonth",      
            "ProductionMonthAZ",    -----------
            "City",
            "Manufacturer",
            "ProductType",
            "Contract",
            "ShippingWarehouse",
            "ShipmentDate",
            "PrognozDataVidachiOR",
            "TypePerehodaPS",
            "ResursAZDataSdachiPlan",
            "ResursAZDataSdachiFakt",
            "ResursDataSdachiPlan",
            "ResursDataSdachiFakt",           
            "quantity",            
        ]
    elif data_type == 'properties_guide':
        data.columns = [
            "id", 
            "kind",
            "name",
        ]
    elif data_type == 'property_value_guide':
        data.columns = [
            "property_id",
            "property_value_id",
            "property_value_name",
        ]

    elif data_type == 'gaz_property_binding_guide':
        data.columns = [
            "property_id",
            "property_value_id",
            "property_value_name",
            "product_id",
            "product_id_KISU",
            "product_name",
        ]
    elif data_type == 'nomenclature_guide':
        data.columns = [
            "ID",
            "Name",
            "CDNuber",
            "Code65",
            "ModelNaZavode",
            "Status",
            "Division",
            "Proizvoditel",
            "VidTovaraID",
            "VidTovara",
            "AutoKit",
            "KisuID",
            "AdabasID",
            "VidProductaID",
            "VidProducta",  
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


def etl(source_engine, dwh_engine, data_type, monthly_tasks=False, offset=None, **context):
    """Запускаем ETL-процесс для заданного типа данных."""
    if monthly_tasks:
        execution_date = (context['execution_date'].date().replace(day=1) - dt.timedelta(days=1)) \
                            .replace(day=1)
    elif offset:
        month = context['execution_date'].month - offset
        if month <= 0:
            month = 12 + month
            execution_date = context['execution_date'].date().replace(month = month, year = context['execution_date'].year - 1, day=1)
        else:
            execution_date = context['execution_date'].date().replace(month = month, day=1)
    else:
        execution_date = context['execution_date'].date().replace(day=1)

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
    elif data_type == 'orders':
        context['ti'].xcom_push(
            key=f'OrdersCount',
            value=sum(data['quantity'])
        )

    load(dwh_engine, data, data_type, execution_date)


def date_check(taskgroup, **context):
    execution_date = context['execution_date'].date()
    if execution_date.day <= 15:
        return taskgroup + '.' + 'monthly_tasks'
    return taskgroup + '.' + 'do_nothing'


def contracting_calculate(dwh_engine, data_type, monthly_tasks=False, **context):
    """Запускаем Перерасчет витрины за текущий или предыдущий месяц."""
    if monthly_tasks:
        execution_date = (context['execution_date'].date().replace(day=1) - dt.timedelta(days=1)) \
                            .replace(day=1)
        plan_date = (context['execution_date'].date().replace(day=1) - dt.timedelta(days=1)) \
                            .replace(day=20)
    else:
        execution_date = context['execution_date'].date().replace(day=1)

        if 1 <= context['execution_date'].day <= 9:
            plan_date = context['execution_date'].date().replace(day=1)
        elif 10 <= context['execution_date'].day <= 19:
            plan_date = context['execution_date'].date().replace(day=10)
        else:
            plan_date = context['execution_date'].date().replace(day=20)            

    with open(
        fr'/home/da/airflow/dags/isc_etl/scripts/dm_isc_{data_type}.sql', 'r'
    ) as f:
        command = f.read().format(
            execution_date=execution_date,
            next_month=(execution_date.replace(day=28) + dt.timedelta(days=4)).replace(day=1),
            previous_month=(execution_date - dt.timedelta(days=1)).replace(day=1),
            plan_date=plan_date,
        )

    print(command)

    for statement in command.split(';'):
        dwh_engine.execute(text(statement))
