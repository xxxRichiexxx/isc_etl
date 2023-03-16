SELECT DROP_PARTITIONS(
    'sttgaz.dm_isc_sales',
    '{{execution_date.date().replace(day=1)}}',
    '{{execution_date.date().replace(day=1)}}'
);

INSERT INTO sttgaz.dm_isc_sales
("Период", "Дивизион", "Дилер", "Территория продаж", "Продажи в розницу", "Продажи физ лицам",
"Остатки на НП", "Остатки на КП", "Продажи в розницу за прошлый месяц", "Продажи физ лицам за прошлый месяц",
"Продажи в розницу за прошлый год", "Продажи физ лицам за прошлый год")
WITH
    dds_data AS(
        SELECT
            d."Дивизион",
            d."Название" AS 'Дилер',
            d."Территория продаж",
            s."Период",
            s."Продано в розницу",
            s."Продано физ лицам",
            s."Остатки на НП в пути",
            s."Остатки на КП в пути"
        FROM (
            SELECT * FROM sttgaz.dds_isc_sales WHERE DATE_TRUNC('MONTH', "Период")
            IN ('{{params.current_month}}', '{{params.previous_month}}', '{{params.previous_year}}')
        ) AS s
        LEFT JOIN sttgaz.dds_isc_dealer AS d
            ON s."Дилер ID" = d.id
        LEFT JOIN sttgaz.dds_isc_buyer AS b 
            ON s."Покупатель ID" = b.id
    ),
	sq1 AS(
		SELECT DISTINCT
			"Дивизион",
			"Дилер",
			"Территория продаж"
		FROM dds_data
	),
	sq2 AS(
		SELECT DISTINCT "Период"
		FROM dds_data	
	),
	sq3 AS(
		SELECT *
		FROM sq1
		CROSS JOIN sq2
	),
	sq4 AS(
		SELECT 
			DATE_TRUNC('MONTH', sq3."Период")::date 								AS "Период",
			sq3."Дивизион",
			sq3."Дилер",
			sq3."Территория продаж",
			SUM("Продано в розницу") 												AS "Продажи в розницу",
			SUM("Продано физ лицам") 												AS "Продажи физ лицам",
			SUM("Остатки на НП в пути") 										    AS "Остатки на НП",
			SUM("Остатки на КП в пути") 											AS "Остатки на КП"
		FROM sq3
		LEFT JOIN dds_data AS s
			ON sq3."Период" = s."Период" 
			AND sq3."Дилер" = s."Дилер"
			AND sq3."Дивизион" = s."Дивизион" 
			AND sq3."Территория продаж" = s."Территория продаж"
		GROUP BY DATE_TRUNC('MONTH', sq3."Период")::date, sq3."Дивизион", sq3."Дилер", sq3."Территория продаж"
	)
SELECT
	*,
	LAG("Продажи в розницу", 1) OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Период") 		    AS "Продажи в розницу за прошлый месяц",
	LAG("Продажи физ лицам", 1) OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Период") 		    AS "Продажи физ лицам за прошлый месяц",
	LAG("Продажи в розницу", 2) OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Период") 		AS "Продажи в розницу за прошлый год",
	LAG("Продажи физ лицам", 2) OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Период") 		AS "Продажи физ лицам за прошлый год"
FROM sq4;