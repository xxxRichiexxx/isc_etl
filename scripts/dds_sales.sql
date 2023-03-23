SELECT DROP_PARTITIONS(
    'sttgaz.dds_isc_sales',
    '{{execution_date.date().replace(day=1)}}',
    '{{execution_date.date().replace(day=1)}}'
);

INSERT INTO sttgaz.dds_isc_sales
("Модельный год", "ВИН", "Дилер ID", "Внутренний код", "Покупатель ID",
 "Спец программа реализации", "Дата отгрузки", "Дата продажи", "Дата записи продажи в БД",
 "Продано в розницу", "Продано физ лицам", "Остатки на НП в пути", "Остатки на КП в пути",
 "Номерной товар ИД", "Направление реализации по приложению", "Направление реализации с учетом УКП",
 "Направление реализации площадки", "Вариант сборки", "Вариант сборки свернутый", "Двигатель",
 "Остатки на НП", "Остатки на КП", "Период")
WITH sq AS(
    SELECT *
    FROM sttgaz.stage_isc_sales
    WHERE load_date = '{{execution_date.date()}}'
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
    "BuildOptionСollapsed"                      AS "Вариант сборки свернутый"
    "Engine"                                    AS "Двигатель",
    "BalanceAtBeginningOfPeriod"                AS "Остатки на НП",
    "BalanceAtEndOfPeriod"                      AS "Остатки на КП",
    "load_date"                                 AS "Период"    
FROM sq                         AS s
LEFT JOIN sttgaz.dds_isc_dealer AS d
    ON    d."Дивизион" = s.division
    AND   d."Название" = s.Recipient
    AND   d."Полное название (организация)" = s.RecipientFullName
LEFT JOIN sttgaz.dds_isc_buyer  AS b
    ON    b."Регион" = s.BuyersRegion
    AND   b."Название" = s.FinalBuyer
    AND   b."ИНН" = s.BuyerINN
    AND   b."ОКВЭД" = s.okved
    AND   b."Род занятий(сфера деятельности)" = s.LineOfWork
    AND   b."Сфера использования" = s.ScopeOfUse
    AND   b."Холдинг" = s.clientsHolding
