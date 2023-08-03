SELECT DROP_PARTITIONS(
    'sttgaz.dm_isc_sales_RF_CIS',
    '{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}',
    '{{execution_date.date().replace(day=1)}}'
);

INSERT INTO sttgaz.dm_isc_sales_RF_CIS
("Продажа Дата", "Дивизион", "Дилер", "Дилер. Название из системы скидок", "Территория РФ", "Напр реализ по прилож с учетом УКП", "Внутренний код",
"Вариант сборки", "Розница ТП", "Остаток НП+ВПути", "Остаток КП+ВПути", "Остаток НП", "Остаток КП",
"Розница АППГ (по дате продажи)", "Розница АППГ (по дате записи в БД)", "Месяц")
WITH
	sales AS(
		SELECT *
		FROM sttgaz.dds_isc_sales
		WHERE ("Направление реализации с учетом УКП" LIKE 'РФ-%'
			OR "Направление реализации с учетом УКП" LIKE 'СНГ-%'
			OR "Направление реализации с учетом УКП" = 'Товарный'
			OR "Направление реализации с учетом УКП" = 'УКП - Московский регион')
			AND DATE_TRUNC('MONTH', "Период") IN (
				'{{execution_date.date().replace(day=1)}}',
				'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}',
				'{{execution_date.date().replace(day=1).replace(year=(execution_date.year-1))}}',
				'{{(execution_date.date().replace(day=1).replace(year=(execution_date.year-1)) - params.delta_1).replace(day=1)}}'
			)
	),
	dds_data AS(
		SELECT
        	d."Дивизион",
			d."Название" 															AS "Дилер",
			d."Название из системы скидок"											AS 	,
            s."Территория продаж",
            s."Дата продажи",
            TO_DATE(s."Дата записи продажи в БД", 'DD.MM.YYYY') 					AS "Дата записи продажи в БД",
            s."Внутренний код",
			s."Направление реализации с учетом УКП",
			s."Вариант сборки",
            s."Продано в розницу",
            s."Продано физ лицам",
            s."Остатки на НП в пути",
            s."Остатки на КП в пути",
            s."Остатки на НП",
            s."Остатки на КП",
            s."Период",
			HASH(d."Дивизион", d."Название", d."Название из системы скидок", s."Территория продаж",
				 s."Внутренний код", s."Направление реализации с учетом УКП", s."Вариант сборки") 		AS "key"
		FROM sales																	AS s
		LEFT JOIN sttgaz.dds_isc_dealer				 								AS d
        	ON s."Дилер ID" = d.id
	),
    sales_1 AS(
        SELECT
            "Дата продажи",
			"key",
            SUM("Продано в розницу") 												AS "Продано в розницу"
        FROM dds_data 																AS s
        GROUP BY
            "Дата продажи",
			"key"
	),
    sales_2 AS(
        SELECT
            "Дата продажи",
			"key",
            SUM("Продано в розницу") 												AS "Продано в розницу"
        FROM dds_data 																AS s
        WHERE 
			DATE_TRUNC('MONTH', "Дата продажи") = DATE_TRUNC('MONTH', NOW()) - INTERVAL '1 YEAR'
        	AND "Дата записи продажи в БД"::date <= (NOW() - INTERVAL '1 YEAR 1 DAY')
        GROUP BY
            "Дата продажи",
			"key"
    ),
	balance AS(
		SELECT 
			DATE_TRUNC('MONTH', "Период")::date 									AS "Период",
			"key",
			SUM("Остатки на НП в пути") 										    AS "Остатки на НП в пути",
			SUM("Остатки на КП в пути") 											AS "Остатки на КП в пути",
			SUM("Остатки на НП") 										    		AS "Остатки на НП",
			SUM("Остатки на КП") 													AS "Остатки на КП"
		FROM dds_data																AS s
		GROUP BY
			DATE_TRUNC('MONTH', "Период")::date,
			"key"
	),
	sq1 AS(
		SELECT DISTINCT
			"Дивизион",
			"Дилер",
			"Дилер. Название из системы скидок",
			"Территория продаж",
			"Внутренний код",
			"Направление реализации с учетом УКП",
			"Вариант сборки",
			"key"
		FROM dds_data
	),
	sq2 AS(
        SELECT DISTINCT DATE_TRUNC('DAY', ts)::date AS "Дата"
        FROM (SELECT '{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'::timestamp as tm 
			  UNION ALL
			  SELECT '{{(execution_date.date().replace(day=28) + params.delta_2).replace(day=1) - params.delta_1}}'::timestamp) AS t
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
			sq3."Территория продаж",
			sq3."Внутренний код",
			sq3."Дивизион",
			sq3."Дилер",
			sq3."Дилер. Название из системы скидок",
			sq3."Направление реализации с учетом УКП",
			sq3."Вариант сборки",
			COALESCE(s1_1."Продано в розницу", 0)														AS "Продажи в розницу",
			COALESCE(b."Остатки на НП в пути", 0)														AS "Остатки на НП в пути",
			COALESCE(b."Остатки на КП в пути", 0)														AS "Остатки на КП в пути",
			COALESCE(b."Остатки на НП", 0)																AS "Остатки на НП",
			COALESCE(b."Остатки на КП", 0)																AS "Остатки на КП",
			COALESCE(s1_2."Продано в розницу", 0)														AS "Продажи в розницу в прошлом году(по дате продажи)",
			COALESCE(s2."Продано в розницу", 0)															AS "Продажи в розницу в прошлом году(по дате записи в БД)"
		FROM sq3
		LEFT JOIN sales_1 																				AS s1_1
			ON sq3."Дата" = s1_1."Дата продажи"
			AND sq3.key = s1_1.key
		LEFT JOIN sales_1																				AS s1_2
			ON EXTRACT(YEAR FROM sq3."Дата") = EXTRACT(YEAR FROM s1_2."Дата продажи") + 1
			AND EXTRACT(MONTH FROM sq3."Дата") = EXTRACT(MONTH FROM s1_2."Дата продажи")
			AND EXTRACT(DAY FROM sq3."Дата") = EXTRACT(DAY FROM s1_2."Дата продажи") 
			AND sq3.key = s1_2.key
		LEFT JOIN sales_2 																				AS s2
			ON EXTRACT(YEAR FROM sq3."Дата") = EXTRACT(YEAR FROM s2."Дата продажи") + 1
			AND EXTRACT(MONTH FROM sq3."Дата") = EXTRACT(MONTH FROM s2."Дата продажи")
			AND EXTRACT(DAY FROM sq3."Дата") = EXTRACT(DAY FROM s2."Дата продажи") 
			AND sq3.key = s2.key
		LEFT JOIN balance																				AS b
			ON DATE_TRUNC('MONTH', sq3."Дата") = b."Период"
			AND sq3.key = b.key
	),
	sq5 AS(
		SELECT
			"Дата" 													AS "Продажа Дата",
			"Дивизион",
			"Дилер",
			"Дилер. Название из системы скидок",
			"Территория продаж" 									AS "Территория РФ",
			"Направление реализации с учетом УКП" 					AS "Напр реализ по прилож с учетом УКП",
			"Внутренний код",
			"Вариант сборки",
			"Продажи в розницу"										AS "Розница ТП",
			"Остатки на НП в пути" 									AS "Остаток НП+ВПути",
			"Остатки на КП в пути"									AS "Остаток КП+ВПути",
			"Остатки на НП"											AS "Остаток НП",
			"Остатки на КП"											AS "Остаток КП",
			"Продажи в розницу в прошлом году(по дате продажи)" 	AS "Розница АППГ (по дате продажи)",
			CASE
				WHEN DATE_TRUNC('MONTH', "Дата") = DATE_TRUNC('MONTH', NOW()) THEN "Продажи в розницу в прошлом году(по дате записи в БД)"
				ELSE "Продажи в розницу в прошлом году(по дате продажи)"
			END 													AS "Розница АППГ (по дате записи в БД)",	
			DATE_TRUNC('MONTH', "Дата")								AS "Месяц"
		FROM sq4
	)
SELECT *
FROM sq5
WHERE "Розница ТП" <> 0
	OR "Остаток НП+ВПути" <> 0
	OR "Остаток КП+ВПути" <> 0
	OR "Остаток НП" <> 0
	OR "Остаток КП" <> 0
	OR "Розница АППГ (по дате продажи)" <> 0
	OR "Розница АППГ (по дате записи в БД)" <> 0;