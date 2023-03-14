WITH 
	calendar AS
	(
		SELECT DISTINCT DATE_TRUNC('MONTH', ts)::date 											AS "Дата"
        FROM (SELECT '2022-01-01 00:00:00'::TIMESTAMP as tm UNION ALL SELECT NOW()) as t
        TIMESERIES ts as '1 DAY' OVER (ORDER BY t.tm)	
	),
	sq1 AS
	(
		SELECT DISTINCT
			"Дата",
			"division" 																			AS "Дивизион",
			"RecipientFullName" 																AS "Дилер",
			SUM(SoldAtRetail) 																	AS "Продажи"
		FROM calendar 
		LEFT JOIN sttgaz.stage_isc_sales AS isc
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
		LEFT JOIN sttgaz.stage_isc_sales 
			ON  DATE_TRUNC('MONTH', ShipmentDate::date)::date <= calendar."Дата"
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
		LEFT JOIN sttgaz.stage_isc_sales
			ON ShipmentDate::date < calendar."Дата"
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
FULL JOIN sq3 USING("Дата", "Дивизион", "Дилер")
