drop view if exists sttgaz.isc_test;
create view sttgaz.isc_test AS
WITH
	sq1 AS(
		SELECT
			DATE_TRUNC('MONTH', load_date)::date 									AS "Дата",
			division 																AS "Дивизион",
			Recipient		 														AS "Дилер",
			SalesTerritory															AS "Территория продаж",
			SUM(SoldAtRetail) 														AS "Продажи в розницу",
			SUM(SoldToIndividuals) 													AS "Продажи физ лицам",
			SUM(BalanceAtBeginningOfPeriod) 										AS "Остатки на НП",
			SUM(BalanceAtEndOfPeriod) 												AS "Остатки на КП"
		FROM sttgaz.stage_isc_sales
		GROUP BY DATE_TRUNC('MONTH', load_date), division, Recipient, SalesTerritory
	)
SELECT
	*,
	LAG("Продажи в розницу") OVER (PARTITION BY "Дивизион", "Территория продаж", "Дилер" ORDER BY "Дата") 		AS "Продажи в розницу за прошлый месяц",
	LAG("Продажи физ лицам") OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Дата") 		AS "Продажи физ лицам за прошлый месяц",
	LAG("Продажи в розницу", 12) OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Дата") 		AS "Продажи в розницу за прошлый год",
	LAG("Продажи физ лицам", 12) OVER (PARTITION BY "Дивизион", "Дилер", "Территория продаж" ORDER BY "Дата") 		AS "Продажи физ лицам за прошлый год"
FROM sq1;

GRANT SELECT ON TABLE sttgaz.isc_test TO PowerBI_Integration WITH GRANT OPTION;