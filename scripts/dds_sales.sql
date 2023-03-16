SELECT DROP_PARTITIONS(
    'sttgaz.dds_isc_sales',
    '{{execution_date.replace(day=1)}}',
    '{{execution_date.replace(day=1)}}'
);

INSERT INTO sttgaz.dds_isc_sales
("Модельный год", "ВИН", "Дилер ID", "Внутренний код", "Покупатель ID",
 "Спец программа реализации", "Дата отгрузки", "Дата продажи", "Дата записи продажи в БД",
 "Продано в розницу", "Продано физ лицам", "Остатки на НП в пути", "Остатки на КП в пути",
 "Номерной товар ИД", "Период")
WITH sq AS(
    SELECT *
    FROM sttgaz.stage_isc_sales
    WHERE load_date = {{execution_date}}
)
SELECT
    "ModelYear"                     AS "Модельный год",
    "vin"                           AS "ВИН",
    d.id                            AS "Дилер ID",
    "code"                          AS "Внутренний код",
    b.id                            AS "Покупатель ID",
    "ImplementationProgram"         AS "Спец программа реализации",
    "ShipmentDate"                  AS "Дата отгрузки",
    "DateOfSale"                    AS "Дата продажи",
    "DateOfEntryIntoDB"             AS "Дата записи продажи в БД",
    "SoldAtRetail"                  AS "Продано в розницу",
    "SoldToIndividuals"             AS "Продано физ лицам",
    "BalanceAtBeginningOfPeriod"    AS "Остатки на НП в пути",
    "BalanceAtEndOfPeriod"          AS "Остатки на КП в пути",
    "ProductIdentifier"             AS "Номерной товар ИД",
    load_date                       AS "Период"    
FROM sq                         AS s
LEFT JOIN sttgaz.dds_isc_dealer AS d
    ON d."Дивизион" = s.division,
       d."Территория продаж" = s.SalesTerritory,
       d."Название" = s.Recipient,
       d."Полное название (организация)" = s.RecipientFullName
LEFT JOIN sttgaz.dds_isc_buyer  AS b
    ON b."Регион" = s.BuyersRegion,
       b."Название" = s.FinalBuyer,
       b."ИНН" = s.BuyerINN,
       b."ОКВЭД" = s.okved,
       b."Род занятий(сфера деятельности)" = s.LineOfWork,
       b."Сфера использования" = s.ScopeOfUse
