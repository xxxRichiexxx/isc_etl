drop view if exists sttgaz.isc_test;
create view sttgaz.isc_test AS
WITH 
	calendar AS
	(
		SELECT DISTINCT DATE_TRUNC('MONTH', ts)::date 											AS "Дата"
        FROM (SELECT '2022-01-01 00:00:00'::TIMESTAMP as tm UNION ALL SELECT NOW()) as t
        TIMESERIES ts as '1 DAY' OVER (ORDER BY t.tm)	
	),
	ShipmentDate_replace AS(
		SELECT 
			*,
			CASE
				WHEN DateOfSale IS NULL AND BalanceAtBeginningOfPeriod = 0 AND BalanceAtEndOfPeriod = 1 AND load_date > ShipmentDate THEN load_date
				ELSE ShipmentDate
			END AS ShipmentDate_replace		
		FROM sttgaz.stage_isc_sales
	),
	sq1 AS
	(
		SELECT DISTINCT
			"Дата",
			"division" 																			AS "Дивизион",
			"RecipientFullName" 																AS "Дилер",
			SUM(SoldAtRetail) 																	AS "Продажи"
		FROM calendar 
		LEFT JOIN ShipmentDate_replace AS isc
			ON calendar."Дата" = DATE_TRUNC('MONTH', isc.DateOfSale::date)::date
		GROUP BY "Дата", "division", "RecipientFullName"
	),
	sq2 AS(
		SELECT
			"Дата",
			"division" 																			AS "Дивизион",
			"RecipientFullName" 																AS "Дилер",
			COUNT(
				DISTINCT REGEXP_REPLACE(vin,'^Z{1,1}' ,'X')
				 ) AS "Остатки на КП"
		FROM calendar
		LEFT JOIN ShipmentDate_replace 
			ON  DATE_TRUNC('MONTH', ShipmentDate_replace::date)::date <= calendar."Дата"
			AND (	
					(DATE_TRUNC('MONTH', DateOfSale::date)::date > calendar."Дата")
					OR (DateOfSale IS NULL AND BalanceAtEndOfPeriod = 1)
					OR (DateOfSale IS NULL AND BalanceAtEndOfPeriod = 0 AND calendar."Дата" < DATE_TRUNC('MONTH', load_date::date)::date)
				)
		GROUP BY "Дата", "division", "RecipientFullName"
	),
	sq3 AS(
		SELECT
			"Дата",
			"division" 																			AS "Дивизион",
			"RecipientFullName" 																AS "Дилер",
			COUNT(
				DISTINCT REGEXP_REPLACE(vin,'^Z{1,1}' ,'X')
				 ) 																				AS "Остатки на НП"
		FROM calendar
		LEFT JOIN ShipmentDate_replace
			ON ShipmentDate_replace::date < calendar."Дата"
			AND (	
					(DateOfSale::date >= calendar."Дата")
					OR (DateOfSale IS NULL AND BalanceAtEndOfPeriod = 1)
					OR (DateOfSale IS NULL AND BalanceAtEndOfPeriod = 0 AND calendar."Дата" <= load_date)
				)
		GROUP BY "Дата", "division", "RecipientFullName"
	)
SELECT
	*,
	LAG("Продажи", 1) OVER (PARTITION BY sq1."Дивизион", sq1."Дилер" ORDER BY sq1."Дата") 		AS "Продажи за прошлый месяц"
FROM sq1
FULL JOIN sq2 USING("Дата", "Дивизион", "Дилер")
FULL JOIN sq3 USING("Дата", "Дивизион", "Дилер");

GRANT SELECT ON TABLE sttgaz.isc_test TO PowerBI_Integration WITH GRANT OPTION;