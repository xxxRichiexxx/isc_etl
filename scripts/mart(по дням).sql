WITH 
	calendar AS
	(
		SELECT DISTINCT DATE_TRUNC('DAY', ts)::date AS "Дата"
        FROM (SELECT '2022-01-01 00:00:00'::TIMESTAMP as tm UNION ALL SELECT NOW()) as t
        TIMESERIES ts as '1 DAY' OVER (ORDER BY t.tm)	
	),
	sq1 AS
	(
		SELECT DISTINCT
			"Дата",
			"division" AS "Дивизион",
			"RecipientFullName" AS "Дилер",
			SUM(SoldAtRetail) OVER (PARTITION BY DATE_TRUNC('DAY', "Дата"), "division", "RecipientFullName")  AS "Продажи",
			SUM(SoldAtRetail) OVER (PARTITION BY DATE_TRUNC('MONTH', "Дата"), "division", "RecipientFullName" ORDER BY "Дата")  AS "Продажи с нарастающим итогом"
		FROM calendar 
		LEFT JOIN sttgaz.stage_isc_sales AS isc
			ON calendar."Дата" = isc.DateOfSale::date
	),
	sq2 AS(
		SELECT
			"Дата",
			"division" AS "Дивизион",
			"RecipientFullName" AS "Дилер",
			COUNT(
				DISTINCT REGEXP_REPLACE(vin,'^Z{1,1}' ,'X')
				 ) AS "Остатки на КП"
		FROM calendar
		LEFT JOIN sttgaz.stage_isc_sales 
			ON  ShipmentDate::date <= calendar."Дата"
			AND (	
					(DateOfSale::date > calendar."Дата")
					OR (DateOfSale IS NULL AND BalanceAtEndOfPeriod = 1)
					OR (DateOfSale IS NULL AND BalanceAtEndOfPeriod = 0 AND calendar."Дата" < load_date)
				)
		GROUP BY "Дата", "division", "RecipientFullName"
	),
	sq3 AS(
		SELECT
			"Дата",
			"division" AS "Дивизион",
			"RecipientFullName" AS "Дилер",
			COUNT(
				DISTINCT REGEXP_REPLACE(vin,'^Z{1,1}' ,'X')
				 ) AS "Остатки на НП"
		FROM calendar
		LEFT JOIN sttgaz.stage_isc_sales
			ON  ShipmentDate::date < calendar."Дата"
			AND (	
					(DateOfSale::date >= calendar."Дата")
					OR (DateOfSale IS NULL AND BalanceAtEndOfPeriod = 1)
					OR (DateOfSale IS NULL AND BalanceAtEndOfPeriod = 0 AND calendar."Дата" <= load_date)
				)
		GROUP BY "Дата", "division", "RecipientFullName"
	)
SELECT *
FROM sq1
JOIN sq2 USING("Дата", "Дивизион", "Дилер")
JOIN sq3 USING("Дата", "Дивизион", "Дилер")
