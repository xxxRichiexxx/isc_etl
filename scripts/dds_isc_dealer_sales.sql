SELECT DROP_PARTITIONS(
    'sttgaz.dds_isc_dealer_sales',
    '{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}',
    '{{execution_date.date().replace(day=1)}}'
);

INSERT INTO sttgaz.dds_isc_dealer_sales
("Продукт ID", "Дилер ID", "Территория продаж", "Конечный клиент ID",
 "Спец программа реализации", "Дата отгрузки", "Дата продажи", "Дата записи продажи в БД",
 "Продано в розницу", "Продано физ лицам", "Остатки на НП", "Остатки на НП в пути",
 "Остатки на КП", "Остатки на КП в пути", "Направление реализации по приложению",
 "Направление реализации с учетом УКП ID", "Направление реализации площадки", "Период")
WITH sq AS(
    SELECT *
    FROM sttgaz.stage_isc_sales
    WHERE DATE_TRUNC('MONTH', load_date) IN(
                    '{{execution_date.date().replace(day=1)}}',
                    '{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
                )
)
SELECT
    p.id                                        AS "Продукт ID",
    d.id                                        AS "Дилер ID",
    "SalesTerritory"                            AS "Территория продаж",
    b.id                                        AS "Конечный клиент ID",  ---
    "ImplementationProgram"                     AS "Спец программа реализации",
    "ShipmentDate"                              AS "Дата отгрузки",
    "DateOfSale"                                AS "Дата продажи",
    "DateOfEntryIntoDB"                         AS "Дата записи продажи в БД",
    "SoldAtRetail"                              AS "Продано в розницу",
    "SoldToIndividuals"                         AS "Продано физ лицам",
    "BalanceAtBeginningOfPeriod"                AS "Остатки на НП",
    "BalanceAtBeginningOfPeriodOnRoad"          AS "Остатки на НП в пути",
    "BalanceAtEndOfPeriod"                      AS "Остатки на КП",
    "BalanceAtEndOfPeriodOnRoad"                AS "Остатки на КП в пути",
    "DirectionOfImplementationByApplication"    AS "Направление реализации по приложению",
    d.id                                        AS "Направление реализации с учетом УКП ID",
    "DirectionOfImplementationPlace"            AS "Направление реализации площадки",
    DATE_TRUNC('MONTH', load_date)              AS "Период"    
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
LEFT JOIN sttgaz.dds_isc_product AS p
	ON HASH(s.vin, s.ProductIdentifier) = HASH(p.ВИН, p."ИД номерного товара");
LEFT JOIN sttgaz.dds_isc_DirectionOfImplementationWithUKP AS dir
    ON d.Название = s.DirectionOfImplementationWithUKP;
