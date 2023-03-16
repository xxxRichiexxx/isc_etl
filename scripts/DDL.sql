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
    "Полное название (организация)" VARCHAR(2000)
)
ORDER BY id
PARTITION BY "Территория продаж";


DROP TABLE IF EXISTS sttgaz.dds_isc_buyer;
CREATE TABLE sttgaz.dds_isc_buyer (
    id AUTO_INCREMENT PRIMARY KEY,
    "Регион" VARCHAR(2000),
    "Название" VARCHAR(2000),
    "ИНН" VARCHAR(500),
    "ОКВЭД"  VARCHAR(500),
    "Род занятий(сфера деятельности)" VARCHAR(500),
    "Сфера использования" VARCHAR(2000)
)
ORDER BY id
PARTITION BY "Регион";
