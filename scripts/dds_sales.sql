SELECT DROP_PARTITIONS(
    'sttgaz.dds_isc_sales',
    '{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}',
    '{{execution_date.date().replace(day=1)}}'
);

INSERT INTO sttgaz.dds_isc_sales
("Модельный год", "ВИН", "Дилер ID", "Внутренний код", "Территория продаж", "Покупатель ID",
 "Спец программа реализации", "Дата отгрузки", "Дата продажи", "Дата записи продажи в БД",
 "Продано в розницу", "Продано физ лицам", "Остатки на НП в пути", "Остатки на КП в пути",
 "Номерной товар ИД", "Направление реализации по приложению", "Направление реализации с учетом УКП",
 "Направление реализации площадки", "Вариант сборки", "Вариант сборки свернутый", "Двигатель",
 "Остатки на НП", "Остатки на КП", "Скидка CRM ИТОГО", "Скидка CRM дилера", 
 "Номенклатура ID", "Дивизион", "Классификатор дивизион тип кабины", "Классификатор привод",
 "Классификатор подробно по дивизионам 22", "Классификатор вид товара", "Классификатор ГБО",
 "Классификатор число посадочных мест", "Классификатор экологический класс", "Период")
WITH sq AS(
    SELECT *
    FROM sttgaz.stage_isc_sales
    WHERE DATE_TRUNC('MONTH', load_date) IN(
                    '{{execution_date.date().replace(day=1)}}',
                    '{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
                )
)
SELECT
    "ModelYear"                                 AS "Модельный год",
    "vin"                                       AS "ВИН",
    d.id                                        AS "Дилер ID",
    "code"                                      AS "Внутренний код",
    "SalesTerritory"                            AS "Территория продаж",
    b.id                                        AS "Покупатель ID",
    "ImplementationProgram"                     AS "Спец программа реализации",
    "ShipmentDate"                              AS "Дата отгрузки",
    "DateOfSale"                                AS "Дата продажи",
    "DateOfEntryIntoDB"                         AS "Дата записи продажи в БД",
    "SoldAtRetail"                              AS "Продано в розницу",
    "SoldToIndividuals"                         AS "Продано физ лицам",
    "BalanceAtBeginningOfPeriodOnRoad"          AS "Остатки на НП в пути",
    "BalanceAtEndOfPeriodOnRoad"                AS "Остатки на КП в пути",
    "ProductIdentifier"                         AS "Номерной товар ИД",
    "DirectionOfImplementationByApplication"    AS "Направление реализации по приложению",
    "DirectionOfImplementationWithUKP"          AS "Направление реализации с учетом УКП",
    "DirectionOfImplementationPlace"            AS "Направление реализации площадки",
    "BuildOption"                               AS "Вариант сборки",
    "BuildOptionСollapsed"                      AS "Вариант сборки свернутый",
    "Engine"                                    AS "Двигатель",
    "BalanceAtBeginningOfPeriod"                AS "Остатки на НП",
    "BalanceAtEndOfPeriod"                      AS "Остатки на КП",
    "DiscountCRMTotal",
    "DiscountCRMDealer",
    "NomenclatureID",
    "division",
    "ClassifierCabType",
    "ClassifierDrive",
    "ClassifierDetailedByDivision",
    "ClassifierProductType",
    "ClassifierGBO",
    "ClassifierNumberOfSeats",
    "ClassifierEcologicalClass",
    DATE_TRUNC('MONTH', load_date)              AS "Период"    
FROM sq                         AS s
LEFT JOIN sttgaz.dds_isc_dealer AS d
    ON    HASH(d."ИСК ID", d."Название", d."Полное название (организация)", d."Название из системы скидок", d."Стоянка ID", d."Стоянка", d."Город стоянки ID", d."Город стоянки") =
          HASH(s."RecipientID", s."Recipient", s."RecipientFullName", s."CustomerFromDiscountSystem", s."StoyankaID", s."Stoyanka", s."StoyankaCityID", s."StoyankaCity")
LEFT JOIN sttgaz.dds_isc_buyer  AS b
    ON    HASH(b."Регион", b."Название", b."ИНН", b."ОКВЭД", b."Род занятий(сфера деятельности)", b."Сфера использования", b."Холдинг") =
          HASH(s.BuyersRegion, s.FinalBuyer, s.BuyerINN, s.okved, s.LineOfWork, s.ScopeOfUse, s.clientsHolding);
