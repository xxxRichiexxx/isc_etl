INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
SELECT
	'{{params.dm}}',
	SUM("Догруз на начало месяца") || '; 608',
	'{{execution_date.date()}}',
    SUM("Догруз на начало месяца") = 608
FROM sttgaz.{{params.dm}}
WHERE DATE_TRUNC('MONTH', "Период") = '2023-06-01';