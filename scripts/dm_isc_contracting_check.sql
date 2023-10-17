INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
SELECT
	'{{params.dm}}',
	'Догруз на начало месяца: ' || SUM("Догруз на начало месяца") || ', 715; ' ||  'Догруз на конец месяца: ' || SUM("Догруз на конец месяца") || ', 1236;', 
	'{{execution_date.date()}}',
    SUM("Догруз на начало месяца") = 715 AND SUM("Догруз на конец месяца") = 1236
FROM sttgaz.{{params.dm}}
WHERE DATE_TRUNC('MONTH', "Период") = '2023-08-01'
	AND "Направление реализации" LIKE 'РФ%'
	AND Производитель LIKE '%ГАЗ%';