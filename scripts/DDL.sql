---------------STAGE--------------------------
DROP TABLE IF EXISTS sttgaz.stage_isc_sales;
CREATE TABLE sttgaz.stage_isc_sales (
    "ModelYear" INT,  --"Модельный год"
    "vin" VARCHAR(500), --"ВИН"
    "division" VARCHAR(50), --"Дивизион"
    "code" VARCHAR(500), --"Внутренний код"
    "SalesTerritory" VARCHAR(2000), --"Территория продаж"
    "Recipient" VARCHAR(2000), --"Название" (Дилер)
    "RecipientFullName" VARCHAR(2000), --"Полное название (организация)"
    "BuyersRegion" VARCHAR(2000), --"Регион"
    "FinalBuyer" VARCHAR(2000), --"Название" (Покупатель)
    "BuyerINN" VARCHAR(500), --"ИНН"
    "okved"  VARCHAR(500), --"ОКВЭД"
    "LineOfWork" VARCHAR(500), --"Род занятий(сфера деятельности)"
    "ScopeOfUse" VARCHAR(2000), --"Сфера использования"
    "ImplementationProgram" VARCHAR(2000), --"Спец программа реализации"
    "ShipmentDate" DATE, --"Дата отгрузки"
    "DateOfSale" DATE, --"Дата продажи"
    "DateOfEntryIntoDB" VARCHAR(500), --"Дата записи продажи в БД"
    "SoldAtRetail" INT, --"Продано в розницу"
    "SoldToIndividuals" INT, --"Продано физ лицам"
    "BalanceAtBeginningOfPeriodOnRoad" INT, --"Остатки на НП в пути"
    "BalanceAtEndOfPeriodOnRoad" INT, --"Остатки на КП в пути"
    "ProductIdentifier" INT, --"Номерной товар ИД"
    "DirectionOfImplementationByApplication" VARCHAR(500),
    "DirectionOfImplementationWithUKP" VARCHAR(500),
    "DirectionOfImplementationPlace" VARCHAR(500),
    "BuildOption" VARCHAR(500),
    "BuildOptionСollapsed" VARCHAR(500),
    "Engine" VARCHAR(200),
    "clientsHolding" VARCHAR(500),
    "BalanceAtBeginningOfPeriod" INT,
    "BalanceAtEndOfPeriod" INT,
    "load_date" DATE
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
);

DROP TABLE IF EXISTS sttgaz.stage_isc_classifier_2;
CREATE TABLE sttgaz.stage_isc_classifier_2 (
    "Item" VARCHAR(500),
    "Value" VARCHAR(500),
    "ItemType"  VARCHAR(500),
    "ProductType"  VARCHAR(500)
);


DROP TABLE IF EXISTS sttgaz.stage_isc_realization;
CREATE TABLE sttgaz.stage_isc_realization(
    "Client" VARCHAR(500),
    "DealersUnit" VARCHAR(500),
    "Doc" VARCHAR(500),
    "BuildOption" VARCHAR(100),
    "BuildOptionСollapsed" VARCHAR(100),
    "PaymentType" VARCHAR(100),
    "PproductTypeByDivision" VARCHAR(100), 
    "vin" VARCHAR(500),
    "AttachmentDate" VARCHAR(500),
    "DischargeDate" VARCHAR(500),
    "Engine" VARCHAR(500),
    "Day" DATE,
    "Division" VARCHAR(50),
    "Contract" VARCHAR(500),
    "Month" VARCHAR(500),
    "PlaneMonth" VARCHAR(500),
    "DirectionOfImplementation" VARCHAR(500),
    "DirectionOfImplementationWithUKP" VARCHAR(500),
    "AttachmentNumber" VARCHAR(100),
    "DischargeNumber" INT,
    "ProductIdentifier" INT,
    "Recipient" VARCHAR(500),
    "Company" VARCHAR(500),
    "Seller" VARCHAR(500),
    "Warehouse" VARCHAR(500),
    "Manufacturer" VARCHAR(200),
    "Product" VARCHAR(500),
    "ProductCode65" VARCHAR(500),
    "ProductNumber" VARCHAR(500),
    "Color" VARCHAR(500),
    "RequestNumber" INT,
    "RequestDischarge" VARCHAR(500),
    "RequestResource" INT,
    "ClientHolding" VARCHAR(500),
    "Availability" INT,
    "Turnover" NUMERIC(12,2),
    "ExpenseVAT" NUMERIC(12,2),
    "TurnoverWithoutVAT" NUMERIC(12,2),
    "Price" NUMERIC(12,2),
    "RefundAmount" NUMERIC(12,2),
    "RefundsVAT" NUMERIC(12,2),
    "RefundWithoutVAT" NUMERIC(12,2),
    "SumMO" NUMERIC(12,2),
    "MOVAT" NUMERIC(12,2),
    "SumMOTotal" NUMERIC(12,2),
    "ProductIdentifier2" INT,
    "DocID" INT,
    "DealersUnitID" INT,
    "ClassifierCabType" VARCHAR(500),
    "ClassifierDrive" VARCHAR(500),
    "ClassifierDetailedByDivision" VARCHAR(500),
    "ClassifierProductType" VARCHAR(500),
    "ClassifierGBO" VARCHAR(500),
    "ClassifierNumberOfSeats" VARCHAR(100),
    "ClassifierEcologicalClass" INT,
    "load_date" DATE
)
ORDER BY load_date
PARTITION BY DATE_TRUNC('month', "load_date");


---------------DDS------------------------
DROP TABLE IF EXISTS sttgaz.dds_isc_sales;
CREATE TABLE sttgaz.dds_isc_sales (
    id AUTO_INCREMENT PRIMARY KEY,
    "Модельный год" INT,  --"ModelYear"
    "ВИН" VARCHAR(500),  --"vin"
    "Дилер ID" INT,                         
    "Внутренний код" VARCHAR(500), --"code"
    "Территория продаж" VARCHAR(2000), --"SalesTerritory"
    "Покупатель ID" INT,                                                                     
    "Спец программа реализации" VARCHAR(2000), --"ImplementationProgram"
    "Дата отгрузки" DATE, --"ShipmentDate"
    "Дата продажи" DATE, --"DateOfSale"
    "Дата записи продажи в БД" VARCHAR(500), --"DateOfEntryIntoDB"
    "Продано в розницу" INT,  --"SoldAtRetail"
    "Продано физ лицам" INT,  --"SoldToIndividuals"
    "Остатки на НП в пути" INT, --"BalanceAtBeginningOfPeriodOnRoad"
    "Остатки на КП в пути" INT, --"BalanceAtEndOfPeriodOnRoad"
    "Номерной товар ИД" INT, --"ProductIdentifier"
    "Направление реализации по приложению" VARCHAR(500),
    "Направление реализации с учетом УКП" VARCHAR(500),
    "Направление реализации площадки" VARCHAR(500),
    "Вариант сборки" VARCHAR(500),
    "Вариант сборки свернутый" VARCHAR(500),
    "Двигатель" VARCHAR(200),
    "Остатки на НП" INT,
    "Остатки на КП" INT,
    "Период" DATE
)
ORDER BY "Период", "Дилер ID", "Покупатель ID"
PARTITION BY DATE_TRUNC('MONTH', "Период");


DROP TABLE IF EXISTS sttgaz.dds_isc_dealer;
CREATE TABLE sttgaz.dds_isc_dealer (
    id AUTO_INCREMENT PRIMARY KEY,                         
    "Дивизион" VARCHAR(50),  --"division"
    "Название" VARCHAR(2000), --"Recipient"
    "Полное название (организация)" VARCHAR(2000), --"RecipientFullName"
    ts TIMESTAMP
)
ORDER BY id;


DROP TABLE IF EXISTS sttgaz.dds_isc_buyer;
CREATE TABLE sttgaz.dds_isc_buyer (
    id AUTO_INCREMENT PRIMARY KEY,
    "Регион" VARCHAR(2000), --"BuyersRegion"
    "Название" VARCHAR(2000), --"FinalBuyer"
    "ИНН" VARCHAR(500), --"BuyerINN"
    "ОКВЭД"  VARCHAR(500), --"okved"
    "Род занятий(сфера деятельности)" VARCHAR(500), --"LineOfWork"
    "Сфера использования" VARCHAR(2000), --"ScopeOfUse"
    "Холдинг" VARCHAR(500),
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

DROP TABLE IF EXISTS sttgaz.dds_isc_classifier_2;
CREATE TABLE sttgaz.dds_isc_classifier_2 (
    id AUTO_INCREMENT PRIMARY KEY,
    "Внутренний код" VARCHAR(500),
    "Значение" VARCHAR(500),
    "Вид товара"  VARCHAR(500),
    "Вид продукции"  VARCHAR(500)
);

DROP TABLE IF EXISTS sttgaz.dds_isc_realization;
CREATE TABLE sttgaz.dds_isc_realization (
	"id" AUTO_INCREMENT,
	"Контрагент ID" INT,
	"Документ" VARCHAR(500),
	"Транспортное средство ID" INT,
	"Вид оплаты" VARCHAR(100), 
	"Дата приложения" VARCHAR(500),
	"Дата разнарядки" VARCHAR(500),
	"День документа" DATE,
	"Договор" VARCHAR(500),
	"Месяц документа" VARCHAR(500),
	"Месяц планирования" VARCHAR(500),
	"Направление реализации" VARCHAR(500), 
	"Направление реализации с учетом УКП ID" VARCHAR(500),
	"Номер приложения" VARCHAR(100),
	"Номер разнярядки" INT,
	"Фирма" VARCHAR(500),
	"Продавец" VARCHAR(500),
	"Склад" VARCHAR(500),
	"Заявка номер" INT,
	"Заявка разнарядка" VARCHAR(500),
	"Заявка ресурс" INT,
	"Холдинг конечного клиента" VARCHAR(500),
	"Наличие" INT,
	"Оборот" NUMERIC(12,2),
	"НДС Расхода" NUMERIC(12,2),
	"Оборот без НДС" NUMERIC(12,2),
	"Цена" NUMERIC(12,2),
	"Сумма возмещения" NUMERIC(12,2),
	"НДС возмещения" NUMERIC(12,2),
	"Сумма возмещения без НДС" NUMERIC(12,2),
	"Сумма МО" NUMERIC(12,2),
	"НДС МО" NUMERIC(12,2),
	"Сумма МО Общ" NUMERIC(12,2),
	"Документ ISC ID" INT,
	"Период" DATE
)
ORDER BY "Период", "Контрагент ID", "Транспортное средство ID"
PARTITION BY DATE_TRUNC('month', "Период");

DROP TABLE IF EXISTS sttgaz.dds_isc_DirectionOfImplementationWithUKP;
CREATE TABLE sttgaz.dds_isc_DirectionOfImplementationWithUKP (
	"id" AUTO_INCREMENT,
	"Направление реализации с учетом УКП" VARCHAR(500)
);

DROP TABLE IF EXISTS sttgaz.dds_isc_counteragent_stt;
CREATE TABLE sttgaz.dds_isc_counteragent_stt (
	"id" AUTO_INCREMENT,
	"Клиент" VARCHAR(500),
    "Получатель" VARCHAR(500),
	"Площадка дилера ISK ID" INT,
	"Площадка дилера" VARCHAR(500)
);


DROP TABLE IF EXISTS sttgaz.dds_isc_product;
CREATE TABLE sttgaz.dds_isc_product (
	"id" AUTO_INCREMENT,
	"Вариант сборки" VARCHAR(100),
	"Вариант сборки свернутый" VARCHAR(100),
	"Вид товара по дивизиону" VARCHAR(100),
	"ВИН" VARCHAR(500),
	"Двигатель по прайсу" VARCHAR(500),
	"Дивизион ID" INT,
	"ИД номерного товара" INT,
    "Производитель ID" INT,
	"Товар" VARCHAR(500),
	"ТоварКод65" VARCHAR(500),
	"Номерной товар" VARCHAR(100),
	"Цвет" VARCHAR(100),
	"Номерной товар ИД" INT,
	"Классификатор дивизион тип кабины" VARCHAR(500),
	"Классификатор привод" VARCHAR(500),
	"Классификатор подробно по дивизионам 22" VARCHAR(500),
	"Классификатор вид товара" VARCHAR(500),
	"Классификатор ГБО" VARCHAR(500),
	"Классификатор число посадочных мест" VARCHAR(500),
	"Классификатор экологический класс" INT
)
ORDER BY id;


DROP TABLE IF EXISTS sttgaz.dds_isc_manufacturer;
CREATE TABLE sttgaz.dds_isc_manufacturer(
	"id" AUTO_INCREMENT,
	"Производитель" VARCHAR(200)
)
ORDER BY id;


DROP TABLE IF EXISTS sttgaz.dds_isc_division;
CREATE TABLE sttgaz.dds_isc_division(
	"id" AUTO_INCREMENT,
	"Дивизион" VARCHAR(50)
)
ORDER BY id;


----------marts---------------------

DROP TABLE IF EXISTS sttgaz.dm_isc_dealer_sales_RF;
CREATE TABLE sttgaz.dm_isc_dealer_sales_RF (
    id AUTO_INCREMENT PRIMARY KEY,
    "Продажа Дата" DATE,
    "Площадка получателя" VARCHAR(2000), 
    "Дивизион" VARCHAR(50),
    "Территория РФ" VARCHAR(2000),
    "Напр реализ по прилож с учетом УКП" VARCHAR(500),
    "Внутренний код" VARCHAR(500),
    "ВИН" VARCHAR(500),
    "Вариант сборки" VARCHAR(500),
    "Номерной товар ИД" INT,
    "Розница ТП" INT,
    "Остаток НП+ВПути" INT,
    "Остаток КП+ВПути" INT,
    "Розница АППГ (по дате продажи)" INT,
    "Розница АППГ (по дате записи в БД)" INT,
    "Месяц" DATE
)
ORDER BY "Продажа Дата", "Дивизион", "Напр реализ по прилож с учетом УКП"
PARTITION BY "Месяц";

GRANT SELECT ON TABLE sttgaz.dm_isc_dealer_sales_RF TO PowerBI_Integration WITH GRANT OPTION;


DROP TABLE IF EXISTS sttgaz.dm_isc_sales_RF_CIS;
CREATE TABLE sttgaz.dm_isc_sales_RF_CIS (
    id AUTO_INCREMENT PRIMARY KEY,
    "Продажа Дата" DATE,
    "Дивизион" VARCHAR(50),
    "Территория РФ" VARCHAR(2000),
    "Напр реализ по прилож с учетом УКП" VARCHAR(500),
    "Внутренний код" VARCHAR(500),
    "ВИН" VARCHAR(500),
    "Вариант сборки" VARCHAR(500),
    "Номерной товар ИД" INT,
    "Розница ТП" INT,
    "Остаток НП+ВПути" INT,
    "Остаток КП+ВПути" INT,
    "Остаток НП" INT,
    "Остаток КП" INT,
    "Розница АППГ (по дате продажи)" INT,
    "Розница АППГ (по дате записи в БД)" INT,
    "Месяц" DATE
)
ORDER BY "Продажа Дата", "Дивизион", "Напр реализ по прилож с учетом УКП"
PARTITION BY "Месяц";

GRANT SELECT ON TABLE sttgaz.dm_isc_sales_RF_CIS TO PowerBI_Integration WITH GRANT OPTION;