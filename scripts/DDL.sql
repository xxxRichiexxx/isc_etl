---------------STAGE--------------------------
DROP TABLE IF EXISTS sttgaz.stage_isc_sales;
CREATE TABLE sttgaz.stage_isc_sales (
    "ModelYear" INT,  --"Модельный год"
    "vin" VARCHAR(500), --"ВИН"
    "division" VARCHAR(50), --"Дивизион"
    "code" VARCHAR(500), --"Внутренний код"
    "SalesTerritory" VARCHAR(2000), --"Территория продаж"
    "RecipientID" INT,
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
    "DiscountCRMTotal" NUMERIC(11,2),
    "DiscountCRMDealer" NUMERIC(11,2),
    "load_date" DATE
)
ORDER BY load_date, Recipient, division, SalesTerritory
PARTITION BY DATE_TRUNC('MONTH', load_date);

COMMENT ON TABLE sttgaz.stage_isc_sales IS 'Продажи ТС дилеров';


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
    "DealersName" VARCHAR(500),
    "Doc" VARCHAR(500),
    "BuildOption" VARCHAR(200),
    "BuildOptionСollapsed" VARCHAR(200),
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

COMMENT ON TABLE sttgaz.stage_isc_realization IS 'Реализация ТС';



DROP TABLE IF EXISTS sttgaz.stage_isc_orders;
CREATE TABLE sttgaz.stage_isc_orders (
    "ProductCode65" VARCHAR(500),
    "Color" VARCHAR(500),
    "BuildOption" VARCHAR(200),
    "ModelYear" INT,
    "AdditionalProps14" VARCHAR(200),
    "IGC" VARCHAR(500),
    "DirectionOfImplementation" VARCHAR(500),
    "Buyer" VARCHAR(500),
    "ShipmentStatus" VARCHAR(500),
    "Status" VARCHAR(500),
    "ContractPeriod" VARCHAR(200),
    "ShipmentMonth" VARCHAR(200),
    "ProductionMonth" VARCHAR(200),
    "City" VARCHAR(200),
    "Manufacturer" VARCHAR(200),
    "ProductType" VARCHAR(200),
    "Contract" VARCHAR(500),
    "ShippingWarehouse" VARCHAR(200),
    "quantity" INT,
    "load_date" DATE
)
ORDER BY "DirectionOfImplementation", "Buyer"
PARTITION BY DATE_TRUNC('month', "load_date");

COMMENT ON table sttgaz.stage_isc_orders IS 'Заявки дилеров(контрактация)';


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
    "Скидка CRM ИТОГО" NUMERIC(11,2),
    "Скидка CRM дилера" NUMERIC(11,2),
    "Период" DATE
)
ORDER BY "Период", "Дилер ID", "Покупатель ID"
PARTITION BY DATE_TRUNC('MONTH', "Период");

COMMENT ON TABLE sttgaz.dds_isc_sales IS 'Продажи ТС дилеров';



DROP TABLE IF EXISTS sttgaz.dds_isc_dealer;
CREATE TABLE sttgaz.dds_isc_dealer (
    id AUTO_INCREMENT PRIMARY KEY,
    "ИСК ID" INT,                         
    "Дивизион" VARCHAR(50),  --"division"
    "Название" VARCHAR(2000), --"Recipient"
    "Полное название (организация)" VARCHAR(2000), --"RecipientFullName"
    ts TIMESTAMP
)
ORDER BY id;

COMMENT ON TABLE sttgaz.dds_isc_dealer IS 'Справочник дилерских площадок. Связан с "Продажи дилнров".';


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

COMMENT ON TABLE sttgaz.dds_isc_buyer IS 'Справочник конечных покупателей. Связан с "Продажи дилнров".';


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
DROP TABLE IF EXISTS sttgaz.dds_isc_dealer_sales;
DROP TABLE IF EXISTS sttgaz.dds_isc_product;
DROP TABLE IF EXISTS sttgaz.dds_isc_manufacturer;
DROP TABLE IF EXISTS sttgaz.dds_isc_division;
DROP TABLE IF EXISTS sttgaz.dds_isc_counteragent;
DROP TABLE IF EXISTS sttgaz.dds_isc_dealer_unit;
DROP TABLE IF EXISTS sttgaz.dds_isc_DirectionOfImplementationWithUKP;
DROP TABLE IF EXISTS sttgaz.dds_isc_orders;


CREATE TABLE sttgaz.dds_isc_DirectionOfImplementationWithUKP (
	"id" AUTO_INCREMENT PRIMARY KEY,
	"Направление реализации с учетом УКП" VARCHAR(500)
);

COMMENT ON TABLE sttgaz.dds_isc_DirectionOfImplementationWithUKP IS 'Справочник направлений реализации с УКП. Связан с "реализация".';


CREATE TABLE sttgaz.dds_isc_counteragent (
	"id" AUTO_INCREMENT PRIMARY KEY,
    "Наименование" VARCHAR(500)
);

COMMENT ON TABLE sttgaz.dds_isc_counteragent IS 'Справочник контрагентов. Связан с "реализация" и "заявки дилеров".';


CREATE TABLE sttgaz.dds_isc_dealer_unit (
	"id" AUTO_INCREMENT PRIMARY KEY,
    "Наименование_дилера" VARCHAR(500),
    "Площадка_дилера_ISK_ID" INT,
    "Площадка_дилера" VARCHAR(500),
    "ts" TIMESTAMP 
);

COMMENT ON TABLE sttgaz.dds_isc_dealer_unit IS 'Справочник дилерских площадок. Связан с "реализация".';


CREATE TABLE sttgaz.dds_isc_manufacturer(
	"id" AUTO_INCREMENT PRIMARY KEY,
	"Наименование" VARCHAR(200)
)
ORDER BY id;

COMMENT ON TABLE sttgaz.dds_isc_division IS 'Справочник производителей. Связан с продуктами.';

CREATE TABLE sttgaz.dds_isc_division(
	"id" AUTO_INCREMENT PRIMARY KEY,
	"Наименование" VARCHAR(20)
)
ORDER BY id;

COMMENT ON TABLE sttgaz.dds_isc_division IS 'Справочник дивизионов. Связан с продуктами.';

CREATE TABLE sttgaz.dds_isc_product (
	"id" AUTO_INCREMENT PRIMARY KEY,
	"Вариант сборки" VARCHAR(200),
	"Вариант сборки свернутый" VARCHAR(200),
	"Вид товара по дивизиону" VARCHAR(200),
	"ВИН" VARCHAR(500),
	"Двигатель по прайсу" VARCHAR(500),
	"ИД номерного товара" INT,
    "Производитель ID" INT REFERENCES sttgaz.dds_isc_manufacturer(id),
	"Товар" VARCHAR(500),
	"ТоварКод65" VARCHAR(500),
	"Номерной товар" VARCHAR(200),
	"Цвет" VARCHAR(100),
	"Номерной товар ИД" INT,
	"Классификатор дивизион тип кабины" VARCHAR(500),
	"Классификатор привод" VARCHAR(500),
	"Классификатор подробно по дивизионам 22" VARCHAR(500),
	"Классификатор вид товара" VARCHAR(500),
	"Классификатор ГБО" VARCHAR(500),
	"Классификатор число посадочных мест" VARCHAR(500),
	"Классификатор экологический класс" INT,
    "Дивизион ID" INT REFERENCES sttgaz.dds_isc_division(id),
    ts TIMESTAMP,

    CONSTRAINT dds_isc_product_unique UNIQUE("Вариант сборки", "Вариант сборки свернутый", "ВИН", "Номерной товар ИД")
)
ORDER BY id;

COMMENT ON TABLE sttgaz.dds_isc_product IS 'Справочник продуктов (ТС). Связан с реализацией.';

CREATE TABLE sttgaz.dds_isc_realization (
	"id" AUTO_INCREMENT PRIMARY KEY,
    "Контрагент ID" INT REFERENCES sttgaz.dds_isc_counteragent(id),
    "Получатель ID" INT REFERENCES sttgaz.dds_isc_counteragent(id),
    "Площадка дилера ID" INT REFERENCES sttgaz.dds_isc_dealer_unit(id),
	"Документ" VARCHAR(500),
	"Продукт ID" INT REFERENCES sttgaz.dds_isc_product(id),
	"Вид оплаты" VARCHAR(100), 
	"Дата приложения" VARCHAR(500),
	"Дата разнарядки" VARCHAR(500),
	"День документа" DATE,
	"Договор" VARCHAR(500),
	"Месяц документа" VARCHAR(500),
	"Месяц планирования" VARCHAR(500),
	"Направление реализации" VARCHAR(500), 
	"Направление реализации с учетом УКП ID" INT REFERENCES sttgaz.dds_isc_DirectionOfImplementationWithUKP(id),
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
ORDER BY "Период", "Контрагент ID", "Продукт ID"
PARTITION BY DATE_TRUNC('month', "Период");

COMMENT ON TABLE sttgaz.dds_isc_realization IS 'Реализация ТС';



CREATE TABLE sttgaz.dds_isc_dealer_sales
(
    id  IDENTITY PRIMARY KEY,
    "Продукт ID" int REFERENCES sttgaz.dds_isc_product(id),
    "Площадка дилера ID" INT REFERENCES sttgaz.dds_isc_dealer_unit(id),
    "Территория продаж" varchar(2000),
    "Конечный клиент ID" int REFERENCES sttgaz.dds_isc_buyer(id),
    "Спец программа реализации" varchar(2000),
    "Дата отгрузки" date,
    "Дата продажи" date,
    "Дата записи продажи в БД" varchar(500),
    "Продано в розницу" int,
    "Продано физ лицам" int,
    "Остатки на НП" int,
    "Остатки на НП в пути" int,
    "Остатки на КП" int,
    "Остатки на КП в пути" int,
    "Направление реализации по приложению" varchar(500),
    "Направление реализации с учетом УКП ID" INT REFERENCES sttgaz.dds_isc_DirectionOfImplementationWithUKP(id),
    "Направление реализации площадки" varchar(500),
    Период date
)   
PARTITION BY (date_trunc('MONTH', "Период"));



DROP TABLE IF EXISTS sttgaz.dds_isc_orders;
CREATE TABLE sttgaz.dds_isc_orders (
    "Товар_Код65" VARCHAR(500),                                         ------"ProductCode65" 
    "Цвет" VARCHAR(500),                                                ------"Color"
    "ВариантСборки" VARCHAR(200),                                       ------"BuildOption"
    "Модельный год" INT,                                                ------"ModelYear"
    "Доп реквизит 14" VARCHAR(200),                                     ------"AdditionalProps14"
    "ИГК" VARCHAR(500),                                                 ------"IGC"
    "Направление реализации" VARCHAR(500),                              ------"DirectionOfImplementation"
    "Покупатель ID" INT REFERENCES sttgaz.dds_isc_counteragent(id),     ------"Buyer"
    "Статус отгрузки" VARCHAR(500),                                     ------"ShipmentStatus"
    "Статус" VARCHAR(500),                                              ------"Status"
    "Период контрактации ИСК" VARCHAR(200),                             ----"ContractPeriod"
    "Месяц отгрузки" VARCHAR(200),                                      ----"ShipmentMonth"
    "Месяц производства" VARCHAR(200),                                  ----"ProductionMonth"
    "Город" VARCHAR(200),                                               ---"City"
    "Производитель" VARCHAR(200),                                       ----"Manufacturer"
    "Вид продукции" VARCHAR(200),                                        -----"ProductType",
    "Договор" VARCHAR(500),                                             --------"Contract",
    "Склад отгрузки" VARCHAR(200),  
    "Количество" INT,                                                   ------"quantity"
    "Период контрактации VERTICA" DATE                                 ------"load_date"
)
ORDER BY "Период контрактации VERTICA", "Направление реализации", "Покупатель ID"
PARTITION BY DATE_TRUNC('month', "Период контрактации VERTICA");

COMMENT ON TABLE sttgaz.dds_isc_orders IS 'Заявки дилеров(контрактация)';


----------marts---------------------

DROP TABLE IF EXISTS sttgaz.dm_isc_dealer_sales_RF;
CREATE TABLE sttgaz.dm_isc_dealer_sales_RF (
    id AUTO_INCREMENT PRIMARY KEY,
    "Продажа Дата" DATE,
    "Площадка получателя" VARCHAR(2000), 
    "Дивизион" VARCHAR(50),
    "Территория РФ" VARCHAR(2000),
    "Cубъект РФ покупателя" VARCHAR(2000),
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
	"Скидка CRM ИТОГО" NUMERIC(11,2),
	"Скидка CRM дилера" NUMERIC(11,2),
    "скидка СТТ (CRM  по ПСП)" NUMERIC(11,2),
	"Скидка CRM ИТОГО в прошлом году(по дате продажи)" NUMERIC(11,2),
	"Скидка CRM дилера в прошлом году(по дате продажи)" NUMERIC(11,2),
    "Скидка CRM ИТОГО в прошлом году(по дате записи в БД)" NUMERIC(11,2),
    "Скидка CRM дилера в прошлом году(по дате записи в БД)" NUMERIC(11,2),    
    "Месяц" DATE
)
ORDER BY "Продажа Дата", "Дивизион", "Напр реализ по прилож с учетом УКП"
PARTITION BY "Месяц";

GRANT SELECT ON TABLE sttgaz.dm_isc_dealer_sales_RF TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_isc_dealer_sales_RF IS 'Продажи (ТС) дилеров по РФ. Витрина данных с посчитанными метриками.'


DROP TABLE IF EXISTS sttgaz.dm_isc_sales_RF_CIS;
CREATE TABLE sttgaz.dm_isc_sales_RF_CIS (
    id AUTO_INCREMENT PRIMARY KEY,
    "Продажа Дата" DATE,
    "Дивизион" VARCHAR(50),
    "Дилер" VARCHAR(2000),
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
COMMENT ON TABLE sttgaz.dm_isc_sales_RF_CIS IS 'Продажи (ТС) дилеров по РФ и СНГ. Витрина данных с посчитанными метриками.'


DROP TABLE IF EXISTS sttgaz.dm_isc_contracting;
CREATE TABLE sttgaz.dm_isc_contracting(
    "Период" DATE NOT NULL,
    "Направление реализации" VARCHAR(500) NOT NULL,
    "Дилер" VARCHAR(500),
    "Производитель" VARCHAR(200) NOT NULL,
    "Город" VARCHAR(200), 
    "Вид оплаты" VARCHAR(200), 
    "Вид продукции" VARCHAR(200) NOT NULL,
    "Догруз на начало месяца" INT,
    "План контрактации" INT,
    "План контрактации. Неделя 1" INT,
    "План контрактации. Неделя 2" INT,
    "План контрактации. Неделя 3" INT,
    "План контрактации. Неделя 4" INT,
    "Факт выдачи ОР" INT,
    "Догруз на конец месяца" INT,
    "Отгрузка в счет следующего месяца" INT,
    "Отгрузка в предыдущем месяце из плана текущего месяца" INT,
    "Фиксированный план на 1, 10, 20 число" INT,
    "Фиксированный план. Неделя 1" INT,
    "Фиксированный план. Неделя 2" INT,
    "Фиксированный план. Неделя 3" INT,
    "Фиксированный план. Неделя 4" INT,
    "Дата фиксации плана" DATE
)
ORDER BY "Период", "Направление реализации"
PARTITION BY DATE_TRUNC('month', "Период");

COMMENT ON TABLE sttgaz.dm_isc_contracting IS 'Заявки дилеров(контрактация). Витрина данных с посчитанными метриками.';

DROP TABLE IF EXISTS sttgaz.dm_isc_contracting_plan;
CREATE TABLE sttgaz.dm_isc_contracting_plan(
    "Дата" DATE NOT NULL,
    "Направление реализации" VARCHAR(500) NOT NULL,
    "Дилер" VARCHAR(500),
    "Производитель" VARCHAR(200) NOT NULL,
    "Город" VARCHAR(200), 
    "Вид оплаты" VARCHAR(200), 
    "Вид продукции" VARCHAR(200) NOT NULL,
    "План контрактации" INT,
    "План контрактации. Неделя 1" INT,
    "План контрактации. Неделя 2" INT,
    "План контрактации. Неделя 3" INT,
    "План контрактации. Неделя 4" INT,
    ts TIMESTAMP
)
ORDER BY "Дата", "Направление реализации"
PARTITION BY DATE_TRUNC('month', "Дата");

COMMENT ON TABLE sttgaz.dm_isc_contracting_plan IS 'Заявки дилеров(контрактация). План, сохраненный по дням (контрольные точки).';