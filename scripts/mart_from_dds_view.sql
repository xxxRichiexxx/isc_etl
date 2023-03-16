CREATE OR REPLACE VIEW dm_isc_sales_v
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
        FROM sttgaz.dds_isc_sales       AS s
        LEFT JOIN sttgaz.dds_isc_dealer AS d
            ON s."Дилер ID" = d.id
        LEFT JOIN sttgaz.dds_isc_buyer  AS b 
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
	LAG("Продажи в розницу") OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Период") 		    AS "Продажи в розницу за прошлый месяц",
	LAG("Продажи физ лицам") OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Период") 		    AS "Продажи физ лицам за прошлый месяц",
	LAG("Продажи в розницу", 12) OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Период") 		AS "Продажи в розницу за прошлый год",
	LAG("Продажи физ лицам", 12) OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Период") 		AS "Продажи физ лицам за прошлый год"
FROM sq4;

GRANT SELECT ON TABLE sttgaz.dm_isc_sales_v TO PowerBI_Integration WITH GRANT OPTION;