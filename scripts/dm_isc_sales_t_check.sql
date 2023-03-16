INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
SELECT
	'dm_isc_sales_t_check',
	'checking_for_accuracy_of_execution',
	NOW(),
    SUM('Продажи в розницу') = {{task_instance.pull(key='SoldAtRetail', task_ids='get_sales')}}
    AND SUM('Продажи физ лицам') = {{task_instance.xcom_pull(key='SoldToIndividuals', task_ids='get_sales')}}
    AND SUM('Остатки на НП') = {{task_instance.xcom_pull(key='BalanceAtBeginningOfPeriod', task_ids='get_sales')}}
    AND SUM('Остатки на КП') = {{task_instance.xcom_pull(key='BalanceAtEndOfPeriod', task_ids='get_sales')}}
FROM sttgaz.dm_isc_sales_t
WHERE "Период" = '{{execution_date.date().replace(day=1)}}';