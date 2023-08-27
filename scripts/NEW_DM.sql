--------------------------------ERP-----------------------------------------------------
DROP TABLE IF EXISTS sttgaz.dm_TEST_erp_sales;
CREATE TABLE sttgaz.dm_TEST_erp_sales AS
WITH 
	agregate_dds_kits AS(
		SELECT
			Период 																	AS "Месяц",
			s."Контрагент ID",
			s."Страна ID",
			s."Дивизион ID",
			s."Чертежный номер комплекта",
			s."Комплектация (вариант сборки)",
			SUM(s."Отгружено за указанный период")									AS "Реализовано" 
		FROM sttgaz.dds_erp_kit_sales 												AS s
		GROUP BY
			Период,
			"Контрагент ID",
			"Страна ID",
			"Дивизион ID",
			"Чертежный номер комплекта",
			"Комплектация (вариант сборки)"
		HAVING "Реализовано" IS NOT NULL AND "Реализовано" <> 0
	),
	kits_w_atributs AS(
		SELECT
			s."Месяц",
			COALESCE(d.Наименование, n.Division) 									AS "Дивизион",
			n."Name"																AS "Внутренний код",
			s."Комплектация (вариант сборки)"										AS "Вариант сборки",
			s."Реализовано",
			CASE
				WHEN cnt."Код страны" IN ('031', '051', '112', '398', '417', '498', '643', '762', '860' ) THEN 'СНГ-' || INITCAP(cnt.Страна)
				ELSE 'БЗ-' || INITCAP(cnt.Страна)
			END 																	AS "Направление реализации с учетом УКП",
			SUM(s."Реализовано") OVER (
				PARTITION BY Дивизион, n."Name", "Комплектация (вариант сборки)", "Направление реализации с учетом УКП" 
				ORDER BY Месяц)		 												AS "Продано с накоплением"  
		FROM agregate_dds_kits														AS s
--		WHERE s."Внутренний код" IS NOT NULL
		LEFT JOIN sttgaz.dds_erp_counterparty 										AS c 
			ON s."Контрагент ID" = c.id 
		LEFT JOIN sttgaz.dds_erp_сountry 											AS cnt 
			ON s."Страна ID"  = cnt.id
		LEFT JOIN sttgaz.dds_erp_division 											AS d 
			ON s."Дивизион ID" = d.id
		LEFT JOIN sttgaz.dm_isc_nomenclature_guide									AS n
			ON REGEXP_REPLACE(s."Чертежный номер комплекта", '^А', 'A') = REGEXP_REPLACE(n.ManufactureModel, '^А', 'A')
				OR REGEXP_REPLACE(s."Чертежный номер комплекта", '^С', 'C') = REGEXP_REPLACE(n.ManufactureModel, '^С', 'C')
				OR REPLACE(REGEXP_REPLACE(s."Чертежный номер комплекта", '^А', 'A'), '-00', '-') = REGEXP_REPLACE(n.Code65 , '^А', 'A')
				OR REPLACE(REGEXP_REPLACE(s."Чертежный номер комплекта", '^С', 'C'), '-00', '-') = REGEXP_REPLACE(n.Code65 , '^С', 'C')
	)
SELECT *
FROM kits_w_atributs;

GRANT SELECT ON TABLE sttgaz.dm_TEST_erp_sales TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_erp_sales IS 'Продажи автокомплектов из ERP';


---------------------------------ISC-sales-----------------------------------------------------
DROP TABLE IF EXISTS sttgaz.dm_TEST_isc_sales;
CREATE TABLE sttgaz.dm_TEST_isc_sales AS
WITH 
	sales_agregate AS (
        SELECT
        	DATE_TRUNC('MONTH', "Период")::date AS "Месяц",
            d."Дивизион",
			s."Внутренний код",
            s."Вариант сборки",
            SUM(s."Продано в розницу") AS "Продано в розницу",      
			s."Направление реализации с учетом УКП"
        FROM sttgaz.dds_isc_sales       AS s
        LEFT JOIN sttgaz.dds_isc_dealer AS d
            ON s."Дилер ID" = d.id
--        WHERE s."Внутренний код" IS NOT NULL
        GROUP BY 
        	DATE_TRUNC('MONTH', "Период")::date,
            d."Дивизион",
            s."Внутренний код",
--          s."Код65",
            s."Вариант сборки",
			s."Направление реализации с учетом УКП"
	)
SELECT
	*,
	SUM("Продано в розницу") OVER (
		PARTITION BY Дивизион, "Внутренний код", "Вариант сборки", "Направление реализации с учетом УКП" 
		ORDER BY Месяц)	 AS "Продано с накоплением"    
FROM sales_agregate;

GRANT SELECT ON TABLE sttgaz.dm_TEST_isc_sales TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_isc_sales IS 'Продажи ТС из ИСК'; 

-------------------------ISC-Balance-----------------------

DROP TABLE IF EXISTS sttgaz.dm_TEST_isc_balance;
CREATE TABLE sttgaz.dm_TEST_isc_balance AS
WITH  
 dds_data AS (
 	SELECT 
 		s.Период,
 		d.Дивизион,
        s."Внутренний код",
        s."Вариант сборки",
        s."Направление реализации с учетом УКП",
        s."Остатки на КП",
        abs((s."Остатки на КП в пути" - s."Остатки на КП")) 			AS "Остатки в пути"
 	FROM sttgaz.dds_isc_sales s 
 	LEFT  JOIN sttgaz.dds_isc_dealer d ON s."Дилер ID" = d.id
 )
 SELECT
 		(date_trunc('MONTH'::varchar(5), s.Период))::date AS Период,
        s.Дивизион,
        s."Внутренний код",
        s."Вариант сборки",
        sum(s."Остатки на КП") AS Остатки,
        s."Направление реализации с учетом УКП"
--        'Остатки на складе'::varchar(32) AS Признак
 FROM  dds_data s
 GROUP BY (date_trunc('MONTH'::varchar(5), s.Период))::date,
          s.Дивизион,
          s."Внутренний код",
          s."Направление реализации с учетом УКП",
          s."Вариант сборки"
 UNION ALL
 SELECT
 		(date_trunc('MONTH'::varchar(5), s.Период))::date AS Период,
        s.Дивизион,
        s."Внутренний код",
        s."Вариант сборки",
        sum(s."Остатки в пути"),
        s."Направление реализации с учетом УКП"
--'Остатки в пути'::varchar(26) AS Признак
 FROM  dds_data s
 GROUP BY (date_trunc('MONTH'::varchar(5), s.Период))::date,
          s.Дивизион,
          s."Внутренний код",
          s."Направление реализации с учетом УКП",
          s."Вариант сборки";
 
GRANT SELECT ON TABLE sttgaz.dm_TEST_isc_balance TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_isc_balance IS 'Остатки готовых ТС из ИСК';         
          

DROP TABLE IF EXISTS sttgaz.dm_TEST_result;
CREATE TABLE sttgaz.dm_TEST_result AS
WITH 
	sq1 AS(
		SELECT DISTINCT
	 		Дивизион,
	        "Внутренний код",
	        "Вариант сборки",
	        "Направление реализации с учетом УКП"			
		FROM sttgaz.dm_TEST_erp_sales
		UNION
		SELECT DISTINCT
	 		Дивизион,
	        "Внутренний код",
	        "Вариант сборки",
	        "Направление реализации с учетом УКП"			
		FROM sttgaz.dm_TEST_isc_sales
		UNION
		SELECT DISTINCT
	 		Дивизион,
	        "Внутренний код",
	        "Вариант сборки",
	        "Направление реализации с учетом УКП"			
		FROM sttgaz.dm_TEST_isc_balance
	),
	sq2 AS(
		SELECT DISTINCT DATE_TRUNC('MONTH', ts)::date AS "Месяц"
		FROM (SELECT DATE_TRUNC('year', '2016-01-01'::date) as tm 
			  UNION ALL
			  SELECT NOW()::date) as t
		TIMESERIES ts as '1 DAY' OVER (ORDER BY t.tm)
	),
	sq3 AS(
		SELECT *
		FROM sq1
		CROSS JOIN sq2
	)
SELECT
	sq3.*,
	LAST_VALUE(erp_sales."Продано с накоплением" ignore nulls) OVER my_window AS "Продажи ERP",
	LAST_VALUE(isc_sales."Продано с накоплением" ignore nulls) OVER my_window AS "Продажи ИСК"
FROM sq3
LEFT JOIN (SELECT * FROM sttgaz.dm_TEST_erp_sales) AS erp_sales
	ON sq3."Месяц" = erp_sales."Месяц"
	AND HASH(
	 	sq3.Дивизион,
	    sq3."Внутренний код",
--	    sq3."Вариант сборки",
	    sq3."Направление реализации с учетом УКП"	
	) = HASH(
	 	erp_sales.Дивизион,
	    erp_sales."Внутренний код",
--	    erp_sales."Вариант сборки",
	    erp_sales."Направление реализации с учетом УКП"		
	)
LEFT JOIN (SELECT * FROM sttgaz.dm_TEST_isc_sales) AS isc_sales
	ON sq3."Месяц" = isc_sales."Месяц"
	AND HASH(
	 	sq3.Дивизион,
	    sq3."Внутренний код",
--	    sq3."Вариант сборки",
	    sq3."Направление реализации с учетом УКП"	
	) = HASH(
	 	isc_sales.Дивизион,
	    isc_sales."Внутренний код",
--	    isc_sales."Вариант сборки",
	    isc_sales."Направление реализации с учетом УКП"		
	)
WINDOW my_window AS (PARTITION BY sq3.Дивизион, sq3."Внутренний код", sq3."Вариант сборки", sq3."Направление реализации с учетом УКП" ORDER BY sq3.Месяц)
ORDER BY sq3.Дивизион, sq3."Внутренний код", sq3."Вариант сборки", sq3."Направление реализации с учетом УКП", sq3.Месяц;

GRANT SELECT ON TABLE sttgaz.dm_TEST_result TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_result IS 'MVP Остатки ТС с учетом автокомплектов';

