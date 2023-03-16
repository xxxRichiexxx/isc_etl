CREATE OR REPLACE VIEW dm_isc_sales_v_test AS
WITH 
	sq1 AS(
		SELECT DISTINCT
			division 																AS "Дивизион",
			Recipient		 														AS "Дилер",
			SalesTerritory															AS "Территория продаж"
		FROM sttgaz.stage_isc_sales
	),
	sq2 AS(
		SELECT DISTINCT load_date AS "Дата"
		FROM sttgaz.stage_isc_sales		
	),
	sq3 AS(
		SELECT *
		FROM sq1
		CROSS JOIN sq2
	),
	sq4 AS(
		SELECT 
			DATE_TRUNC('MONTH', "Дата")::date 										AS "Дата",
			"Дивизион",
			"Дилер",
			"Территория продаж",
			SUM(SoldAtRetail) 														AS "Продажи в розницу",
			SUM(SoldToIndividuals) 													AS "Продажи физ лицам",
			SUM(BalanceAtBeginningOfPeriod) 										AS "Остатки на НП",
			SUM(BalanceAtEndOfPeriod) 												AS "Остатки на КП"
		FROM sq3
		LEFT JOIN sttgaz.stage_isc_sales AS s
			ON sq3."Дата" = s.load_date 
			AND sq3."Дилер" = s.Recipient
			AND sq3."Дивизион" = s.division 
			AND sq3."Территория продаж" = s.SalesTerritory 
		GROUP BY DATE_TRUNC('MONTH', "Дата")::date, "Дивизион", "Дилер", "Территория продаж"
	)
SELECT
	*,
	LAG("Продажи в розницу") OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Дата") 		AS "Продажи в розницу за прошлый месяц",
	LAG("Продажи физ лицам") OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Дата") 		AS "Продажи физ лицам за прошлый месяц",
	LAG("Продажи в розницу", 12) OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Дата") 		AS "Продажи в розницу за прошлый год",
	LAG("Продажи физ лицам", 12) OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Дата") 		AS "Продажи физ лицам за прошлый год"
FROM sq4;

GRANT SELECT ON TABLE dm_isc_sales_v_test TO PowerBI_Integration WITH GRANT OPTION;