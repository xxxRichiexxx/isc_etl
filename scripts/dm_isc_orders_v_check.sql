INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
SELECT
	'{{params.dm}}',
	SUM("Количество") || '; {{task_instance.xcom_pull(key='OrdersCount', task_ids='Загрузка_данных_в_stage_слой.get_orders_1')}}',
	'{{execution_date.date()}}',
    SUM("Количество") = {{task_instance.xcom_pull(key='OrdersCount', task_ids='Загрузка_данных_в_stage_слой.get_orders_1')}}
FROM sttgaz.{{params.dm}}
WHERE DATE_TRUNC('MONTH', "Период контрактации VERTICA") = '{{execution_date.date().replace(day=1)}}';