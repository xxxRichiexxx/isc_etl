---------------STAGE--------------------------
DROP TABLE IF EXISTS sttgaz.stage_isc_sales;
CREATE TABLE sttgaz.stage_isc_sales (
    "ModelYear" INT,
    "vin" VARCHAR(500),
    "division" VARCHAR(50),
    "code" VARCHAR(500),
    "SalesTerritory" VARCHAR(2000),
    "Recipient" VARCHAR(2000),
    "RecipientFullName" VARCHAR(2000),
    "BuyersRegion" VARCHAR(2000),
    "FinalBuyer" VARCHAR(2000),
    "BuyerINN" VARCHAR(500),
    "okved"  VARCHAR(500),
    "LineOfWork" VARCHAR(500),
    "ScopeOfUse" VARCHAR(2000),
    "ImplementationProgram" VARCHAR(2000),
    "ShipmentDate" DATE,
    "DateOfSale" DATE,
    "DateOfEntryIntoDB" VARCHAR(500),
    "SoldAtRetail" INT,
    "SoldToIndividuals" INT,
    "BalanceAtBeginningOfPeriod" INT,
    "BalanceAtEndOfPeriod" INT,
    "ProductIdentifier" INT,
    load_date DATE
)
ORDER BY load_date, Recipient, division, SalesTerritory
PARTITION BY DATE_TRUNC('MONTH', load_date);


DROP TABLE IF EXISTS sttgaz.stage_isc_classifier;
CREATE TABLE sttgaz.stage_isc_classifier (
    "Stsep" VARCHAR(500),
    "BHKID" INT,
    "Vnutrenicod" VARCHAR(500),
    "Options"  VARCHAR(500),
    "Dvigatel" VARCHAR(500),
    "Division" VARCHAR(100),
    "Kuzov" VARCHAR(100),
    "Pocolenie" VARCHAR(100),
    "Semeistvo" VARCHAR(100),
    "Naznachenie" VARCHAR(100),
    "PodrMesta" VARCHAR(100),
    "Razmer" VARCHAR(100),
    "Privid" VARCHAR(100),
    "Dvigatel2" VARCHAR(100),
    "Options2" VARCHAR(100),
    "GruppaAEB" VARCHAR(100)
)
---------------DDS------------------------
DROP TABLE IF EXISTS sttgaz.dds_isc_sales;
CREATE TABLE sttgaz.dds_isc_sales (
    id AUTO_INCREMENT PRIMARY KEY,
    "Модельный год" INT,
    "ВИН" VARCHAR(500),
    "Дилер ID" INT,                         
    "Внутренний код" VARCHAR(500),
    "Покупатель ID" INT,                                                                     
    "Спец программа реализации" VARCHAR(2000),
    "Дата отгрузки" DATE,
    "Дата продажи" DATE,
    "Дата записи продажи в БД" VARCHAR(500),
    "Продано в розницу" INT,
    "Продано физ лицам" INT,
    "Остатки на НП в пути" INT,
    "Остатки на КП в пути" INT,
    "Номерной товар ИД" INT,
    "Период" DATE
)
ORDER BY "Период", "Дилер ID", "Покупатель ID"
PARTITION BY DATE_TRUNC('MONTH', "Период");


DROP TABLE IF EXISTS sttgaz.dds_isc_dealer;
CREATE TABLE sttgaz.dds_isc_dealer (
    id AUTO_INCREMENT PRIMARY KEY,                         
    "Дивизион" VARCHAR(50),
    "Территория продаж" VARCHAR(2000),
    "Название" VARCHAR(2000),
    "Полное название (организация)" VARCHAR(2000),
    ts TIMESTAMP
)
ORDER BY id;


DROP TABLE IF EXISTS sttgaz.dds_isc_buyer;
CREATE TABLE sttgaz.dds_isc_buyer (
    id AUTO_INCREMENT PRIMARY KEY,
    "Регион" VARCHAR(2000),
    "Название" VARCHAR(2000),
    "ИНН" VARCHAR(500),
    "ОКВЭД"  VARCHAR(500),
    "Род занятий(сфера деятельности)" VARCHAR(500),
    "Сфера использования" VARCHAR(2000),
    ts TIMESTAMP
)
ORDER BY id;


DROP TABLE IF EXISTS sttgaz.dds_isc_classifier;
CREATE TABLE sttgaz.dds_isc_classifier (
    id AUTO_INCREMENT PRIMARY KEY,
    "Сцеп" VARCHAR(2000),
    "ВнК_ID" INT,
    "Внутренний код" VARCHAR(500),
    "Опции"  VARCHAR(500),
    "Двигатель" VARCHAR(500),
    "Гр1 Дивизион" VARCHAR(100),
    "Гр2 Кузов" VARCHAR(100),
    "Гр3 Поколение" VARCHAR(100),
    "Гр4 Семейство" VARCHAR(100),
    "Гр5 Назначение" VARCHAR(100),
    "Гр6 Подр (места)" VARCHAR(100),
    "Гр7 Размер" VARCHAR(100),
    "Гр8 Привод" VARCHAR(100),
    "Гр9 Двигатель" VARCHAR(100),
    "Гр10 Опции" VARCHAR(100),
    "Гр11 группа АЕБ" VARCHAR(100)
)
ORDER BY id;


----------marts---------------------

DROP TABLE IF EXISTS sttgaz.dm_isc_sales_t;
CREATE TABLE sttgaz.dm_isc_sales_t (
    id AUTO_INCREMENT PRIMARY KEY,
    "Период" DATE,
    "Дивизион" VARCHAR(50),
    "Дилер" VARCHAR(2000), 
    "Территория продаж" VARCHAR(2000),
    "Продажи в розницу" INT,
    "Продажи физ лицам" INT,
    "Остатки на НП" INT,
    "Остатки на КП" INT,
    "Продажи в розницу за прошлый месяц" INT,
    "Продажи физ лицам за прошлый месяц" INT,
    "Продажи в розницу за прошлый год" INT,
    "Продажи физ лицам за прошлый год" INT
)
ORDER BY "Период", "Дивизион", "Дилер"
PARTITION BY DATE_TRUNC('MONTH', "Период");

GRANT SELECT ON TABLE sttgaz.dm_isc_sales_t TO PowerBI_Integration WITH GRANT OPTION;