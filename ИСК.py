import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote


username = 'PowerBI_integration'
password = quote('VoGZX8ORSE')
host = 'cl04sql.st.tech\inst04sql'
db = 'rm_work'
eng_str = fr'mssql://{username}:{password}@{host}/{db}?driver=SQL Server'

engine_1 =sa.create_engine(eng_str)

# command = """
#     DECLARE @a date;
#     set @a = DATEADD(month, DATEDIFF(month, 0, DATEADD(YEAR, -1, DATEADD(MONTH, -1, current_timestamp))), 0)
#     DECLARE @b date;
#     set @b = EOMONTH(DATEADD(YEAR, -1, DATEADD(MONTH, -1, current_timestamp)))

#     EXECUTE [dbo].[хп_ВыгрузкаДляСайта_ПродажиДилеров] 
#     @НачалоПериода = @a,
#     @ОкончаниеПериода = @b
#     """

command = """
    DECLARE @a date;
    set @a = '2022-10-01'
    DECLARE @b date;
    set @b = '2022-11-01'

    EXECUTE [dbo].[хп_ВыгрузкаДляСайта_ПродажиДилеров] 
    @НачалоПериода = @a,
    @ОкончаниеПериода = @b
    """

data = pd.read_sql_query(
    command,
    engine_1
)

# data['ПродажаДатаЗаписиВБД'] = pd.to_datetime(data['ПродажаДатаЗаписиВБД'], format='%d.%m.%Y %H:%M')

print(data)

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

data['load_date'] = '2022-10-01'

ps = quote('s@vy7hSA')
engine_2 = sa.create_engine(
    f'vertica+vertica_python://shveynikovab:{ps}@vs-da-vertica:5433/sttgaz?'
)

vins = tuple(data['vin'].values)

# engine_2.execute(
#     f"""
#     DELETE FROM sttgaz.stage_ISC_1
#     WHERE vin IN {vins} 
#     """
# )

data.to_sql(
    'stage_ISC_1',
    engine_2,
    schema = 'sttgaz',
    if_exists='append',
    index = False,
)






# domain = 'ST'
# username = 'PowerBI_integration'
# password = 'n0l2mgucgUrRRUassTjP'
# host = 'cl06sql\inst06sql'
# db = 'sur_integration'
# eng_str = fr"mssql+pymssql://{domain}\{username}:{password}@{host}/{db}?charset=cp1251"
# engine_1 = sa.create_engine(eng_str)

# data = pd.read_sql_query(
#     """
#     SELECT ServiceStation
#     FROM dbo.DocumentsWarrantyRepair
#     """,
#     engine_1
# )


# print(data)