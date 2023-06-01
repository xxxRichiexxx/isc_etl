INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
SELECT
	'{{params.dm}}',
	SUM("Реализовано") || '; {{task_instance.xcom_pull(key='RealizationCount', task_ids='Загрузка_данных_в_stage_слой.get_realization')}}',
	'{{execution_date.date()}}',
    SUM("Реализовано") = {{task_instance.xcom_pull(key='RealizationCount', task_ids='Загрузка_данных_в_stage_слой.get_realization')}}
FROM sttgaz.{{params.dm}}
WHERE DATE_TRUNC('MONTH', "Месяц") = '{{execution_date.date().replace(day=1)}}';