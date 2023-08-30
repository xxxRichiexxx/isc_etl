DROP TABLE IF EXISTS sttgaz.dm_TEST_erp_sales;
CREATE TABLE sttgaz.dm_TEST_erp_sales AS
WITH
sq1 AS(
	SELECT
		Месяц,
		COALESCE(Дивизион, n.Division) 											AS "Дивизион",
		n."Name"																AS "Внутренний код",
		n.Code65																AS "ТоварКод65", 
		"Вариант сборки",
		Реализовано,
		"Направление реализации с учетом УКП"
	FROM sttgaz.dm_erp_kit_sales_v 												AS s
	LEFT JOIN sttgaz.stage_isc_nomenclature_guide								AS n
		ON s.Контрагент = n.Proizvoditel 
			AND(
				REGEXP_REPLACE(s."Чертежный номер комплекта", '^А', 'A') = REGEXP_REPLACE(n.ModelNaZavode, '^А', 'A')
				OR REGEXP_REPLACE(s."Чертежный номер комплекта", '^С', 'C') = REGEXP_REPLACE(n.ModelNaZavode, '^С', 'C')
				OR REPLACE(REGEXP_REPLACE(s."Чертежный номер комплекта", '^А', 'A'), '-00', '-') = REGEXP_REPLACE(n.Code65 , '^А', 'A')
				OR REPLACE(REGEXP_REPLACE(s."Чертежный номер комплекта", '^С', 'C'), '-00', '-') = REGEXP_REPLACE(n.Code65 , '^С', 'C')
			)
			AND n.Division IN ('LCV', 'MCV', 'BUS')
),
sq2 AS(
	SELECT
		Месяц,
		"Дивизион",
		"Внутренний код",
		ТоварКод65,
--		"Вариант сборки",
		SUM(Реализовано) 														AS	"Реализовано",
		"Направление реализации с учетом УКП"
	FROM sq1 												
	GROUP BY
		Месяц,
		"Дивизион",
		"Внутренний код",
		ТоварКод65,
--		"Вариант сборки",
		"Направление реализации с учетом УКП"
	UNION ALL
	SELECT
			r.Период								AS "Месяц",
			div.Наименование						AS "Дивизион",
			p.Товар									AS "Внутренний код",
			p.ТоварКод65, 
--			p."Вариант сборки"						AS "Вариант сборки",
			SUM(r.Наличие)							AS "Реализовано",
			d."Направление реализации с учетом УКП" AS "Направление реализации с учетом УКП" 	
	FROM sttgaz.dds_isc_realization 			AS r 
			LEFT JOIN sttgaz.dds_isc_product 		AS p
				ON r."Продукт ID" = p.id 
			LEFT JOIN sttgaz.dds_isc_DirectionOfImplementationWithUKP AS d
				ON r."Направление реализации с учетом УКП ID" = d.id
			LEFT JOIN sttgaz.dds_isc_division  		AS div
				ON p."Дивизион ID"  = div.id
	WHERE DATE_TRUNC('month', r.Период)::date IN ('2022-07-01', '2022-10-01', '2022-12-01', '2023-02-01', '2023-03-01')
				AND div.Наименование = 'LCV'
				AND d."Направление реализации с учетом УКП" = 'СНГ-Казахстан'
	GROUP BY
				r.Период,
				div.Наименование,
				p.Товар,
				p.ТоварКод65, 
--				p."Вариант сборки",
				d."Направление реализации с учетом УКП"
)
SELECT
	*,
	SUM("Реализовано") OVER (
		PARTITION BY Дивизион, "Внутренний код", ТоварКод65, --"Вариант сборки",
		"Направление реализации с учетом УКП" 
		ORDER BY Месяц
	)		 																										AS "Продано с накоплением" 	
FROM sq2;

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
--            s."Вариант сборки",
            SUM(s."Продано в розницу") AS "Продано в розницу",      
			s."Направление реализации с учетом УКП"
        FROM sttgaz.dds_isc_sales       AS s
        LEFT JOIN sttgaz.dds_isc_dealer AS d
            ON s."Дилер ID" = d.id
        GROUP BY 
        	DATE_TRUNC('MONTH', "Период")::date,
            d."Дивизион",
            s."Внутренний код",
            --s."Вариант сборки",
			s."Направление реализации с учетом УКП"
	)
SELECT
	*,
	SUM("Продано в розницу") OVER (
		PARTITION BY Дивизион, "Внутренний код", --"Вариант сборки",
		"Направление реализации с учетом УКП" 
		ORDER BY Месяц)	 AS "Продано с накоплением"    
FROM sales_agregate;

GRANT SELECT ON TABLE sttgaz.dm_TEST_isc_sales TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_isc_sales IS 'Продажи ТС из ИСК'; 

-------------------------ISC-Balance-----------------------

DROP TABLE IF EXISTS sttgaz.dm_TEST_isc_balance;
CREATE TABLE sttgaz.dm_TEST_isc_balance AS
 	SELECT 
 		(date_trunc('MONTH'::varchar(5), s.Период))::date   AS "Месяц",
 		d.Дивизион,
        s."Внутренний код",
        s."Вариант сборки",
        s."Направление реализации с учетом УКП",
        SUM(s."Остатки на НП в пути")								AS "Остатки на НП",
        SUM(s."Остатки на КП в пути")								AS "Остатки на КП"
 	FROM sttgaz.dds_isc_sales 								AS s 
 	LEFT  JOIN sttgaz.dds_isc_dealer d ON s."Дилер ID" = d.id
  	GROUP BY 
  		(date_trunc('MONTH'::varchar(5), s.Период))::date,
        d.Дивизион,
        s."Внутренний код",
        s."Вариант сборки",
		s."Направление реализации с учетом УКП";

 
GRANT SELECT ON TABLE sttgaz.dm_TEST_isc_balance TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_isc_balance IS 'Остатки готовых ТС из ИСК';         
          
-----------------------------------------------------------------------------
DROP TABLE IF EXISTS sttgaz.dm_TEST_result;
CREATE TABLE sttgaz.dm_TEST_result AS
WITH 
	sq1 AS(
		SELECT DISTINCT
	 		Дивизион,
	        "Внутренний код",
--	        "Вариант сборки",
	        "Направление реализации с учетом УКП"			
		FROM sttgaz.dm_TEST_erp_sales
		UNION
		SELECT DISTINCT
	 		Дивизион,
	        "Внутренний код",
--	        "Вариант сборки",
	        "Направление реализации с учетом УКП"			
		FROM sttgaz.dm_TEST_isc_sales
		UNION
		SELECT DISTINCT
	 		Дивизион,
	        "Внутренний код",
--	        "Вариант сборки",
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
LEFT JOIN sttgaz.dm_TEST_erp_sales AS erp_sales
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
LEFT JOIN sttgaz.dm_TEST_isc_sales AS isc_sales
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
WINDOW my_window AS (PARTITION BY sq3.Дивизион, sq3."Внутренний код", --sq3."Вариант сборки",
		 sq3."Направление реализации с учетом УКП" ORDER BY sq3.Месяц)
ORDER BY sq3.Дивизион, sq3."Внутренний код", --sq3."Вариант сборки", 
	sq3."Направление реализации с учетом УКП", sq3.Месяц;

GRANT SELECT ON TABLE sttgaz.dm_TEST_result TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_result IS 'MVP Остатки ТС с учетом автокомплектов';


















DROP TABLE IF EXISTS sttgaz.dm_TEST_erp_sales;
CREATE TABLE sttgaz.dm_TEST_erp_sales AS
WITH
sq1 AS(
	SELECT
		Месяц,
		COALESCE(Дивизион, n.Division) 											AS "Дивизион",
		n."Name"																AS "Внутренний код",
		n.Code65																AS "ТоварКод65", 
		"Вариант сборки",
		Реализовано,
		"Направление реализации с учетом УКП"
	FROM sttgaz.dm_erp_kit_sales_v 												AS s
	LEFT JOIN sttgaz.stage_isc_nomenclature_guide								AS n
		ON s.Контрагент = n.Manufacture 
			AND(
				REGEXP_REPLACE(s."Чертежный номер комплекта", '^А', 'A') = REGEXP_REPLACE(n.ManufactureModel, '^А', 'A')
				OR REGEXP_REPLACE(s."Чертежный номер комплекта", '^С', 'C') = REGEXP_REPLACE(n.ManufactureModel, '^С', 'C')
				OR REPLACE(REGEXP_REPLACE(s."Чертежный номер комплекта", '^А', 'A'), '-00', '-') = REGEXP_REPLACE(n.Code65 , '^А', 'A')
				OR REPLACE(REGEXP_REPLACE(s."Чертежный номер комплекта", '^С', 'C'), '-00', '-') = REGEXP_REPLACE(n.Code65 , '^С', 'C')
			)
),
sq2 AS(
	SELECT
		Месяц,
		"Дивизион",
		"Внутренний код",
		ТоварКод65,
		"Вариант сборки",
		SUM(Реализовано) 														AS	"Реализовано",
		"Направление реализации с учетом УКП"
	FROM sq1 												
	GROUP BY
		Месяц,
		"Дивизион",
		"Внутренний код",
		ТоварКод65,
		"Вариант сборки",
		"Направление реализации с учетом УКП"
	UNION ALL
	SELECT
			r.Период								AS "Месяц",
			div.Наименование						AS "Дивизион",
			p.Товар									AS "Внутренний код",
			p.ТоварКод65, 
			p."Вариант сборки"						AS "Вариант сборки",
			SUM(r.Наличие)							AS "Реализовано",
			d."Направление реализации с учетом УКП" AS "Направление реализации с учетом УКП" 	
	FROM sttgaz.dds_isc_realization 			AS r 
			LEFT JOIN sttgaz.dds_isc_product 		AS p
				ON r."Продукт ID" = p.id 
			LEFT JOIN sttgaz.dds_isc_DirectionOfImplementationWithUKP AS d
				ON r."Направление реализации с учетом УКП ID" = d.id
			LEFT JOIN sttgaz.dds_isc_division  		AS div
				ON p."Дивизион ID"  = div.id
	WHERE DATE_TRUNC('month', r.Период)::date IN ('2022-07-01', '2022-10-01', '2022-12-01', '2023-02-01', '2023-03-01')
				AND div.Наименование = 'LCV'
				AND d."Направление реализации с учетом УКП" = 'СНГ-Казахстан'
	GROUP BY
				r.Период,
				div.Наименование,
				p.Товар,
				p.ТоварКод65, 
				p."Вариант сборки",
				d."Направление реализации с учетом УКП"
)
SELECT
	*,
	SUM("Реализовано") OVER (
		PARTITION BY Дивизион, "Внутренний код", ТоварКод65, "Вариант сборки",
		"Направление реализации с учетом УКП" 
		ORDER BY Месяц
	)		 																										AS "Продано с накоплением" 	
FROM sq2;

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
        GROUP BY 
        	DATE_TRUNC('MONTH', "Период")::date,
            d."Дивизион",
            s."Внутренний код",
            s."Вариант сборки",
			s."Направление реализации с учетом УКП"
	)
SELECT
	*,
	SUM("Продано в розницу") OVER (
		PARTITION BY Дивизион, "Внутренний код", "Вариант сборки",
		"Направление реализации с учетом УКП" 
		ORDER BY Месяц)	 AS "Продано с накоплением"    
FROM sales_agregate;

GRANT SELECT ON TABLE sttgaz.dm_TEST_isc_sales TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_isc_sales IS 'Продажи ТС из ИСК'; 


-------------------------ISC-Balance-----------------------

DROP TABLE IF EXISTS sttgaz.dm_TEST_isc_balance;
CREATE TABLE sttgaz.dm_TEST_isc_balance AS
 	SELECT 
 		(date_trunc('MONTH'::varchar(5), s.Период))::date   AS "Месяц",
 		d.Дивизион,
        s."Внутренний код",
        s."Вариант сборки",
        s."Направление реализации с учетом УКП",
        SUM(s."Остатки на НП")								AS "Остатки на НП",
        SUM(s."Остатки на КП")								AS "Остатки на КП"
 	FROM sttgaz.dds_isc_sales 								AS s 
 	LEFT  JOIN sttgaz.dds_isc_dealer d ON s."Дилер ID" = d.id
  	GROUP BY 
  		(date_trunc('MONTH'::varchar(5), s.Период))::date,
        d.Дивизион,
        s."Внутренний код",
        s."Вариант сборки",
		s."Направление реализации с учетом УКП";

 
GRANT SELECT ON TABLE sttgaz.dm_TEST_isc_balance TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_isc_balance IS 'Остатки готовых ТС из ИСК';         
          
-----------------------------------------------------------------------------
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
LEFT JOIN sttgaz.dm_TEST_erp_sales AS erp_sales
	ON sq3."Месяц" = erp_sales."Месяц"
	AND HASH(
	 	sq3.Дивизион,
	    sq3."Внутренний код",
	    sq3."Вариант сборки",
	    sq3."Направление реализации с учетом УКП"	
	) = HASH(
	 	erp_sales.Дивизион,
	    erp_sales."Внутренний код",
	    erp_sales."Вариант сборки",
	    erp_sales."Направление реализации с учетом УКП"		
	)
LEFT JOIN sttgaz.dm_TEST_isc_sales AS isc_sales
	ON sq3."Месяц" = isc_sales."Месяц"
	AND HASH(
	 	sq3.Дивизион,
	    sq3."Внутренний код",
	    sq3."Вариант сборки",
	    sq3."Направление реализации с учетом УКП"	
	) = HASH(
	 	isc_sales.Дивизион,
	    isc_sales."Внутренний код",
	    isc_sales."Вариант сборки",
	    isc_sales."Направление реализации с учетом УКП"		
	)
WINDOW my_window AS (PARTITION BY sq3.Дивизион, sq3."Внутренний код", sq3."Вариант сборки",
		 sq3."Направление реализации с учетом УКП" ORDER BY sq3.Месяц)
ORDER BY sq3.Дивизион, sq3."Внутренний код", sq3."Вариант сборки", 
	sq3."Направление реализации с учетом УКП", sq3.Месяц;

GRANT SELECT ON TABLE sttgaz.dm_TEST_result TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON TABLE sttgaz.dm_TEST_result IS 'MVP Остатки ТС с учетом автокомплектов';
