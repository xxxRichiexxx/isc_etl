INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH
sq1 AS(
	SELECT
		SUM(SoldAtRetail) AS "Продажи",
		SUM(BalanceAtBeginningOfPeriodOnRoad) AS "Остатки"
	FROM sttgaz.stage_isc_sales sis
	WHERE (DirectionOfImplementationWithUKP LIKE 'РФ-%')
		AND DATE_TRUNC('month', load_date) = '{{execution_date.date().replace(day=1)}}'
		AND division IN ('LCV', 'MCV')
),
sq2 AS(	
	SELECT
		SUM("Розница ТП") AS "Продажи"
	FROM sttgaz.sttgaz.dm_isc_dealer_sales_RF s
	WHERE ("Напр реализ по прилож с учетом УКП" LIKE 'РФ-%'
		OR "Напр реализ по прилож с учетом УКП" = 'Товарный'
		OR "Напр реализ по прилож с учетом УКП" = 'УКП - Московский регион')
		AND DATE_TRUNC('month', "Месяц") = '{{execution_date.date().replace(day=1)}}'
		AND "Дивизион" IN ('LCV', 'MCV')
),
sq3 AS(	
	SELECT
		SUM("Остаток НП+ВПути") AS "Остатки"
	FROM sttgaz.sttgaz.dm_isc_dealer_sales_RF s
	WHERE ("Напр реализ по прилож с учетом УКП" LIKE 'РФ-%')
		AND "Продажа Дата" = '{{execution_date.date().replace(day=1)}}'
		AND "Дивизион" IN ('LCV', 'MCV')
)
SELECT 
	'{{params.dm}}',
	'checking_for_accuracy_of_execution',
	'{{execution_date.date()}}',
	(SELECT "Продажи" FROM sq1) = (SELECT "Продажи" FROM sq2) AND (SELECT "Остатки" FROM sq1) = (SELECT "Остатки" FROM sq3); 