INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
SELECT
	'{{params.dm}}',
	SUM("Догруз на начало месяца") || '; 715',
	'{{execution_date.date()}}',
    SUM("Догруз на начало месяца") = 715
FROM sttgaz.{{params.dm}}
WHERE DATE_TRUNC('MONTH', "Период") = '2023-08-01'
	AND "Направление реализации" LIKE 'РФ%'
	AND Производитель LIKE '%ГАЗ%';