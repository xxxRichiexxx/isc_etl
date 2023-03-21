
CREATE OR REPLACE VIEW sttgaz.dm_isc_sales_v_detailed AS
WITH
	dds_data AS(
		SELECT
        	d."Дивизион",
            d."Название" 															AS "Дилер",
            d."Территория продаж",
            s."Дата продажи",
            TO_DATE(s."Дата записи продажи в БД", 'DD.MM.YYYY') 					AS "Дата записи продажи в БД",
            s."Внутренний код",
            s."Продано в розницу"													AS "Продано в розницу",
            s."Продано физ лицам"													AS "Продано физ лицам"		
		FROM (SELECT * FROM sttgaz.dds_isc_sales WHERE "Дата продажи" IS NOT NULL)	AS s
		LEFT JOIN sttgaz.dds_isc_dealer				 								AS d
        ON s."Дилер ID" = d.id
	),
    sales_1 AS(
        SELECT
        	"Дивизион",
            "Дилер",
            "Территория продаж",
            "Дата продажи",
            "Внутренний код",
            SUM("Продано в розницу") 												AS "Продано в розницу",
            SUM("Продано физ лицам")												AS "Продано физ лицам"
        FROM dds_data 																AS s
        GROUP BY
        	"Дивизион",
            "Дилер",
            "Территория продаж",
            "Дата продажи",
            "Внутренний код"
    ),
    sales_2 AS(
        SELECT
        	"Дивизион",
            "Дилер",
            "Территория продаж",
            "Дата продажи",
            "Внутренний код",
            SUM("Продано в розницу") 												AS "Продано в розницу",
            SUM("Продано физ лицам")												AS "Продано физ лицам"
        FROM dds_data 																AS s
        WHERE 
			DATE_TRUNC('MONTH', "Дата продажи") = DATE_TRUNC('MONTH', NOW()) - INTERVAL '1 YEAR'
        	AND "Дата записи продажи в БД"::date <= (NOW() - INTERVAL '1 YEAR 1 DAY')
        GROUP BY
        	"Дивизион",
            "Дилер",
            "Территория продаж",
            "Дата продажи",
            "Внутренний код"
    ),
	sq1 AS(
		SELECT DISTINCT
			"Дивизион",
			"Дилер",
			"Территория продаж",
			"Внутренний код"
		FROM sales_1
	),
	sq2 AS(
        SELECT DISTINCT DATE_TRUNC('DAY', ts)::date AS "Дата"
        FROM (SELECT '2020-01-01 00:00:00'::TIMESTAMP as tm UNION ALL SELECT NOW()) AS t
        TIMESERIES ts as '1 DAY' OVER (ORDER BY t.tm)
	),
	sq3 AS(
		SELECT *
		FROM sq1
		CROSS JOIN sq2
	),
	sq4 AS(
		SELECT 
			"Дата",
			sq3."Дилер",
			sq3."Территория продаж",
			sq3."Внутренний код",
			sq3."Дивизион",
			COALESCE(s1_1."Продано в розницу", 0)														AS "Продажи в розницу",
			COALESCE(s1_1."Продано физ лицам", 0)														AS "Продано физ лицам",
			COALESCE(s1_2."Продано в розницу", 0)														AS "Продажи в розницу в прошлом году(по дате продажи)",
			COALESCE(s1_2."Продано физ лицам", 0)														AS "Продано физ лицам в прошлом году(по дате продажи)",
			COALESCE(s2."Продано в розницу", 0)															AS "Продажи в розницу в прошлом году(по дате записи в БД)",
			COALESCE(s2."Продано физ лицам", 0)															AS "Продано физ лицам в прошлом году(по дате записи в БД)"	
		FROM sq3
		LEFT JOIN sales_1 																				AS s1_1
			ON sq3."Дата" = s1_1."Дата продажи" 
			AND sq3."Дивизион" = s1_1."Дивизион"
			AND sq3."Дилер" = s1_1."Дилер"
			AND sq3."Территория продаж" = s1_1."Территория продаж"
			AND sq3."Внутренний код" = s1_1."Внутренний код"
		LEFT JOIN sales_1																				AS s1_2
			ON EXTRACT(YEAR FROM sq3."Дата") = EXTRACT(YEAR FROM s1_2."Дата продажи") + 1
			AND EXTRACT(MONTH FROM sq3."Дата") = EXTRACT(MONTH FROM s1_2."Дата продажи")
			AND EXTRACT(DAY FROM sq3."Дата") = EXTRACT(DAY FROM s1_2."Дата продажи") 
			AND sq3."Дивизион" = s1_2."Дивизион"
			AND sq3."Дилер" = s1_2."Дилер"
			AND sq3."Территория продаж" = s1_2."Территория продаж"
			AND sq3."Внутренний код" = s1_2."Внутренний код"
		LEFT JOIN sales_2 																				AS s2
			ON EXTRACT(YEAR FROM sq3."Дата") = EXTRACT(YEAR FROM s2."Дата продажи") + 1
			AND EXTRACT(MONTH FROM sq3."Дата") = EXTRACT(MONTH FROM s2."Дата продажи")
			AND EXTRACT(DAY FROM sq3."Дата") = EXTRACT(DAY FROM s2."Дата продажи") 
			AND sq3."Дивизион" = s2."Дивизион"
			AND sq3."Дилер" = s2."Дилер"
			AND sq3."Территория продаж" = s2."Территория продаж"
			AND sq3."Внутренний код" = s2."Внутренний код"
	)
SELECT
	*,
	CASE
		WHEN DATE_TRUNC('MONTH', "Дата") = DATE_TRUNC('MONTH', NOW()) THEN "Продажи в розницу в прошлом году(по дате записи в БД)"
		ELSE "Продажи в розницу в прошлом году(по дате продажи)"
	END 																								AS "Продажи в розницу в прошлом году (для дашборда)",	
	DATE_TRUNC('MONTH', "Дата")																			AS "Месяц"
FROM sq4;

GRANT SELECT ON TABLE sttgaz.dm_isc_sales_v_detailed TO PowerBI_Integration WITH GRANT OPTION;