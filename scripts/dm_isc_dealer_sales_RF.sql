SELECT DROP_PARTITIONS(
    'sttgaz.dm_isc_dealer_sales_RF',
    '{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}',
    '{{execution_date.date().replace(day=1)}}'
);

INSERT INTO sttgaz.dm_isc_dealer_sales_RF
("Продажа Дата", "Площадка получателя", "Дивизион", "Территория РФ", "Cубъект РФ покупателя", 
"Напр реализ по прилож с учетом УКП", "Спец программа реализации", "Внутренний код", "Вариант сборки", 
"Розница ТП", "Остаток НП+ВПути", "Остаток КП+ВПути", "Розница АППГ (по дате продажи)", 
"Розница АППГ (по дате записи в БД)", 
"Скидка CRM ИТОГО",
"Скидка CRM дилера",
"скидка СТТ (CRM  по ПСП)",
"Скидка CRM ИТОГО в прошлом году(по дате продажи)",
"Скидка CRM дилера в прошлом году(по дате продажи)",
"Скидка CRM ИТОГО в прошлом году(по дате записи в БД)",
"Скидка CRM дилера в прошлом году(по дате записи в БД)",
"Месяц")
WITH
	sales AS(
		SELECT *
		FROM sttgaz.dds_isc_sales 
		WHERE ("Направление реализации с учетом УКП" LIKE 'РФ-%'
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
        	s."Дивизион",
            d."Название" 															AS "Дилер",
            s."Территория продаж",
            s."Дата продажи",
            TO_DATE(s."Дата записи продажи в БД", 'DD.MM.YYYY') 					AS "Дата записи продажи в БД",
			b."Регион",
            s."Внутренний код",
			s."Направление реализации с учетом УКП",
			s."Спец программа реализации",
			s."Вариант сборки",
            s."Продано в розницу",
            s."Продано физ лицам",
            s."Остатки на НП в пути",
            s."Остатки на КП в пути",
			s."Скидка CRM ИТОГО",
			s."Скидка CRM дилера",
            s."Период",
			HASH(d."Дивизион", d."Название", s."Территория продаж", b."Регион", s."Внутренний код",
				s."Направление реализации с учетом УКП", s."Спец программа реализации",
				s."Вариант сборки")													AS "key"
		FROM sales																	AS s
		LEFT JOIN sttgaz.dds_isc_dealer				 								AS d
        	ON s."Дилер ID" = d.id	
		LEFT JOIN sttgaz.dds_isc_buyer												AS b 
			ON s."Покупатель ID" = b.id
	),
    sales_1 AS(
        SELECT
            "Дата продажи",
			"key",
            SUM("Продано в розницу") 												AS "Продано в розницу",
			SUM("Скидка CRM ИТОГО")													AS "Скидка CRM ИТОГО",
			SUM("Скидка CRM дилера")												AS "Скидка CRM дилера"
        FROM dds_data 																AS s
        GROUP BY
            "Дата продажи",
			"key"
    ),
    sales_2 AS(
        SELECT
            "Дата продажи",
			"key",
            SUM("Продано в розницу") 												AS "Продано в розницу",
			SUM("Скидка CRM ИТОГО")													AS "Скидка CRM ИТОГО",
			SUM("Скидка CRM дилера")												AS "Скидка CRM дилера"
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
			SUM("Остатки на НП в пути") 										    AS "Остатки на НП",
			SUM("Остатки на КП в пути") 											AS "Остатки на КП"
		FROM dds_data																AS s
		GROUP BY
			DATE_TRUNC('MONTH', "Период")::date,
			"key"
	),
	sq1 AS(
		SELECT DISTINCT
			"Дивизион",
			"Дилер",
			"Территория продаж",
			"Регион",
			"Внутренний код",
			"Направление реализации с учетом УКП",
			"Спец программа реализации",
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
			sq3."Дилер",
			sq3."Территория продаж",
			sq3."Регион",
			sq3."Внутренний код",
			sq3."Дивизион",
			sq3."Направление реализации с учетом УКП",
			sq3."Спец программа реализации",
			sq3."Вариант сборки",
			COALESCE(s1_1."Продано в розницу", 0)														AS "Продажи в розницу",
			COALESCE(b."Остатки на НП", 0)																AS "Остатки на на начало месяца",
			COALESCE(b."Остатки на КП", 0)																AS "Остатки на конец месяца",
			COALESCE(s1_2."Продано в розницу", 0)														AS "Продажи в розницу в прошлом году(по дате продажи)",
			COALESCE(s2."Продано в розницу", 0)															AS "Продажи в розницу в прошлом году(по дате записи в БД)",
			COALESCE(s1_1."Скидка CRM ИТОГО", 0)														AS "Скидка CRM ИТОГО",
			COALESCE(s1_1."Скидка CRM дилера", 0)														AS "Скидка CRM дилера",
			COALESCE(s1_2."Скидка CRM ИТОГО", 0)														AS "Скидка CRM ИТОГО в прошлом году(по дате продажи)",
			COALESCE(s1_2."Скидка CRM дилера", 0)														AS "Скидка CRM дилера в прошлом году(по дате продажи)",
			COALESCE(s2."Скидка CRM ИТОГО", 0)															AS "Скидка CRM ИТОГО в прошлом году(по дате записи в БД)",
			COALESCE(s2."Скидка CRM дилера", 0)															AS "Скидка CRM дилера в прошлом году(по дате записи в БД)"		
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
			"Дилер" 												AS "Площадка получателя",
			"Дивизион",
			"Территория продаж" 									AS "Территория РФ",
			"Регион"												AS "Cубъект РФ покупателя",
			"Направление реализации с учетом УКП" 					AS "Напр реализ по прилож с учетом УКП",
			"Спец программа реализации",
			"Внутренний код",
			"Вариант сборки",
			"Продажи в розницу"										AS "Розница ТП",
			"Остатки на на начало месяца" 							AS "Остаток НП+ВПути",
			"Остатки на конец месяца"								AS "Остаток КП+ВПути",
			"Продажи в розницу в прошлом году(по дате продажи)" 	AS "Розница АППГ (по дате продажи)",
			CASE
				WHEN DATE_TRUNC('MONTH', "Дата") = DATE_TRUNC('MONTH', NOW()) THEN "Продажи в розницу в прошлом году(по дате записи в БД)"
				ELSE "Продажи в розницу в прошлом году(по дате продажи)"
			END 													AS "Розница АППГ (по дате записи в БД)",
			"Скидка CRM ИТОГО",
			"Скидка CRM дилера",
			"Скидка CRM ИТОГО" - "Скидка CRM дилера" 				AS "скидка СТТ (CRM  по ПСП)",
			"Скидка CRM ИТОГО в прошлом году(по дате продажи)",
			"Скидка CRM дилера в прошлом году(по дате продажи)",
			CASE
				WHEN DATE_TRUNC('MONTH', "Дата") = DATE_TRUNC('MONTH', NOW()) THEN "Скидка CRM ИТОГО в прошлом году(по дате записи в БД)"
				ELSE "Скидка CRM ИТОГО в прошлом году(по дате продажи)"
			END 													AS "Скидка CRM ИТОГО в прошлом году(по дате записи в БД)",
			CASE
				WHEN DATE_TRUNC('MONTH', "Дата") = DATE_TRUNC('MONTH', NOW()) THEN "Скидка CRM дилера в прошлом году(по дате записи в БД)"
				ELSE "Скидка CRM дилера в прошлом году(по дате продажи)"
			END 													AS "Скидка CRM дилера в прошлом году(по дате записи в БД)",		
			DATE_TRUNC('MONTH', "Дата")								AS "Месяц"
		FROM sq4
	)
SELECT *
FROM sq5
WHERE "Розница ТП" <> 0
	OR "Остаток НП+ВПути" <> 0
	OR "Остаток КП+ВПути" <> 0
	OR "Розница АППГ (по дате продажи)" <> 0
	OR "Розница АППГ (по дате записи в БД)" <> 0;