INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
SELECT
	'{{params.dm}}',
	SUM("Продажи в розницу") || '; {{task_instance.xcom_pull(key='SoldAtRetail', task_ids='Загрузка_данных_в_stage_слой.get_sales')}} ||' ||
    SUM("Остатки на НП") || '; {{task_instance.xcom_pull(key='BalanceAtBeginningOfPeriodOnRoad', task_ids='Загрузка_данных_в_stage_слой.get_sales')}} ||' ||
    SUM("Остатки на КП") || '; {{task_instance.xcom_pull(key='BalanceAtEndOfPeriodOnRoad', task_ids='Загрузка_данных_в_stage_слой.get_sales')}}',
	'{{execution_date.date()}}',
    SUM("Продажи в розницу") = {{task_instance.xcom_pull(key='SoldAtRetail', task_ids='Загрузка_данных_в_stage_слой.get_sales')}}
    AND SUM("Остатки на НП") = {{task_instance.xcom_pull(key='BalanceAtBeginningOfPeriodOnRoad', task_ids='Загрузка_данных_в_stage_слой.get_sales')}}
    AND SUM("Остатки на КП") = {{task_instance.xcom_pull(key='BalanceAtEndOfPeriodOnRoad', task_ids='Загрузка_данных_в_stage_слой.get_sales')}}
FROM sttgaz.{{params.dm}}
WHERE DATE_TRUNC('MONTH', "Месяц") = '{{execution_date.date().replace(day=1)}}';