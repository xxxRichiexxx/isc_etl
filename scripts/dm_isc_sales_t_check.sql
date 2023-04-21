INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH
sq1 AS(
	SELECT
		SUM(SoldAtRetail) AS "Продажи",
		SUM(BalanceAtEndOfPeriodOnRoad) AS "Остатки"
	FROM sttgaz.stage_isc_sales sis
	WHERE (DirectionOfImplementationWithUKP LIKE 'РФ-%')
		AND DATE_TRUNC('month', load_date) = '{{execution_date.date().replace(day=1)}}'
		AND division IN ('LCV', 'MCV')
),
sq2 AS(	
	SELECT
		SUM("Розница ТП") AS "Продажи"
	FROM sttgaz.{{params.dm}} s
	WHERE "Напр реализ по прилож с учетом УКП" LIKE 'РФ-%'
		AND DATE_TRUNC('month', "Месяц") = '{{execution_date.date().replace(day=1)}}'
		AND "Дивизион" IN ('LCV', 'MCV')
),
sq3 AS(	
	SELECT
		SUM("Остаток КП+ВПути") AS "Остатки"
	FROM sttgaz.{{params.dm}} s
	WHERE ("Напр реализ по прилож с учетом УКП" LIKE 'РФ-%')
		AND "Продажа Дата" = '{{execution_date.date().replace(day=1)}}'
		AND "Дивизион" IN ('LCV', 'MCV')
)
SELECT 
	'{{params.dm}}',
	'checking_for_accuracy_of_execution: '||(SELECT "Продажи" FROM sq1) ||'='||(SELECT "Продажи" FROM sq2) ||'&'
		|| (SELECT "Остатки" FROM sq1) ||'='|| (SELECT "Остатки" FROM sq3),
	'{{execution_date.date()}}',
	(SELECT "Продажи" FROM sq1) = (SELECT "Продажи" FROM sq2) AND (SELECT "Остатки" FROM sq1) = (SELECT "Остатки" FROM sq3); 