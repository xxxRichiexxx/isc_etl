DROP VIEW IF EXISTS sttgaz.dm_isc_balance;
CREATE OR REPLACE VIEW sttgaz.dm_isc_balance AS
WITH  
sales AS ( 
	SELECT dds_isc_sales.id,
        dds_isc_sales."Модельный год",
        dds_isc_sales.ВИН,
        dds_isc_sales."Дилер ID",
        dds_isc_sales."Внутренний код",
        dds_isc_sales."Территория продаж",
        dds_isc_sales."Покупатель ID",
        dds_isc_sales."Спец программа реализации",
        dds_isc_sales."Дата отгрузки",
        dds_isc_sales."Дата продажи",
        dds_isc_sales."Дата записи продажи в БД",
        dds_isc_sales."Продано в розницу",
        dds_isc_sales."Продано физ лицам",
        dds_isc_sales."Остатки на НП в пути",
        dds_isc_sales."Остатки на КП в пути",
        dds_isc_sales."Номерной товар ИД",
        dds_isc_sales."Направление реализации по приложению",
        dds_isc_sales."Направление реализации с учетом УКП",
        dds_isc_sales."Направление реализации площадки",
        dds_isc_sales."Вариант сборки",
        dds_isc_sales."Вариант сборки свернутый",
        dds_isc_sales.Двигатель,
        dds_isc_sales."Остатки на НП",
        dds_isc_sales."Остатки на КП",
        dds_isc_sales.Период
 	FROM sttgaz.dds_isc_sales
 	WHERE (((((dds_isc_sales."Направление реализации с учетом УКП" ~~ 'РФ-%'::varchar(6))
 			OR (dds_isc_sales."Направление реализации с учетом УКП" ~~ 'СНГ-%'::varchar(8)))
 			OR (dds_isc_sales."Направление реализации с учетом УКП" = 'Товарный'::varchar(16)))
 			OR (dds_isc_sales."Направление реализации с учетом УКП" = 'УКП - Московский регион'::varchar(42)))
 		AND (date_trunc('MONTH'::varchar(5), dds_isc_sales.Период) >= '2022-01-01 00:00:00'::timestamp))
 ),
 dds_data AS (
 	SELECT d.Дивизион,
        s."Территория продаж",
        s."Внутренний код",
        s."Направление реализации с учетом УКП",
        s."Вариант сборки",
        d.Название 											AS "Площадка дилера",
        d."Название из системы скидок"							       AS "Дилер. Название из системы скидок",
        s."Остатки на КП",
        abs((s."Остатки на КП в пути" - s."Остатки на КП")) AS "Остатки в пути",
        s.Период
 	FROM sales s 
 	LEFT  JOIN sttgaz.dds_isc_dealer d ON s."Дилер ID" = d.id
 )
 SELECT
 		(date_trunc('MONTH'::varchar(5), s.Период))::date AS Период,
        s.Дивизион,
        s."Территория продаж",
        s."Внутренний код",
        s."Направление реализации с учетом УКП",
        s."Вариант сборки",
        s."Площадка дилера",
        s."Дилер. Название из системы скидок",
        sum(s."Остатки на КП") AS Остатки,
        'Остатки на складе'::varchar(32) AS Признак
 FROM  dds_data s
 GROUP BY (date_trunc('MONTH'::varchar(5), s.Период))::date,
          s.Дивизион,
          s."Территория продаж",
          s."Внутренний код",
          s."Направление реализации с учетом УКП",
          s."Вариант сборки",
          s."Площадка дилера",
          s."Дилер. Название из системы скидок"
 HAVING (sum(s."Остатки на КП") > 0) 
 UNION ALL  
 SELECT 
 		(date_trunc('MONTH'::varchar(5), s.Период))::date AS Период,
        s.Дивизион,
        s."Территория продаж",
        s."Внутренний код",
        s."Направление реализации с учетом УКП",
        s."Вариант сборки",
        s."Площадка дилера",
        s."Дилер. Название из системы скидок",
        sum(s."Остатки в пути") AS Остатки,
        'Остатки в пути'::varchar(26) AS Признак
 FROM  dds_data s
 GROUP BY (date_trunc('MONTH'::varchar(5), s.Период))::date,
          s.Дивизион,
          s."Территория продаж",
          s."Внутренний код",
          s."Направление реализации с учетом УКП",
          s."Вариант сборки", 
          s."Площадка дилера",
          s."Дилер. Название из системы скидок"
 HAVING (sum(s."Остатки в пути") > 0);

COMMENT ON VIEW sttgaz.dm_isc_balance IS 'Остатки на складах дилеров. Витрина данных.';
GRANT SELECT ON TABLE sttgaz.dm_isc_balance TO PowerBI_Integration WITH GRANT OPTION;