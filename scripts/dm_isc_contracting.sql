SELECT DROP_PARTITIONS(
    'sttgaz.dm_isc_contracting',
    '{execution_date}',
    '{execution_date}'
);

INSERT INTO sttgaz.dm_isc_contracting 
WITH 
	base_query AS(
		 SELECT
			*,
			CASE
				WHEN "Договор" LIKE 'ДФ%' THEN 'Отсрочка'
				WHEN "Договор" LIKE 'ДР55/4%' THEN 'Отсрочка'
				WHEN "Договор" LIKE 'ДР55%' THEN 'Предоплата'
				ELSE 'Неизвестно'			
			END 																									AS "Вид оплаты",
			HASH("Направление реализации", "Наименование", "Производитель", "Город", "Вид оплаты", "Вид продукции") AS key 		
		 FROM sttgaz.dds_isc_orders 			AS o
		 LEFT JOIN sttgaz.dds_isc_counteragent 	AS c
		 	ON o."Покупатель ID" = c.id
		 WHERE o."Период контрактации VERTICA" > DATE_TRUNC('month', '{execution_date}'::date - INTERVAL '8 month')::date
	),
	matrix AS(
		SELECT DISTINCT 
			'{execution_date}'::date								AS "Период",
			"Направление реализации",
			Наименование 											AS "Дилер",
			Производитель,
			Город,
			"Вид оплаты",
			"Вид продукции",
			key
		FROM base_query
	),
	sq1 AS(
		SELECT
			key,
			SUM(Количество) 									AS "Догруз на начало месяца"
		FROM base_query
		WHERE "Статус отгрузки"  IN ('Разнарядка', 'Отгрузка')
			AND TO_DATE("Месяц отгрузки", 'YYYY-MM') >= '{execution_date}'
			AND "Период контрактации VERTICA"  < '{execution_date}'  
		GROUP BY key		
	),
	sq2 AS(
		 SELECT
			key,
		 	SUM(Количество) 									AS "План контрактации",
		 	ROUND(SUM(Количество)*0.7, 0) 						AS "План контрактации. Неделя 1",
		 	ROUND(SUM(Количество)*0.2, 0) 						AS "План контрактации. Неделя 2",
		 	ROUND(SUM(Количество)*0.05, 0) 						AS "План контрактации. Неделя 3",
		 	SUM(Количество) - ROUND(SUM(Количество)*0.7, 0) 
		 					- ROUND(SUM(Количество)*0.05, 0)
		 					- ROUND(SUM(Количество)*0.2, 0)		AS "План контрактации. Неделя 4"
		FROM base_query
		WHERE "Период контрактации VERTICA" = '{execution_date}'
		GROUP BY key	
	),
	sq3 AS(
		 SELECT
			key,
		 	SUM(Количество) 										AS "Факт выдачи ОР"
		 FROM base_query
		 WHERE "Период контрактации VERTICA" = '{execution_date}' 
			AND "Статус отгрузки"  IN ('Разнарядка', 'Отгрузка')
		GROUP BY key
	),
	sq4 AS(
		 SELECT 
			key,
		 	SUM(Количество) 									AS "Догруз на конец месяца"
		 FROM base_query
		 WHERE "Период контрактации VERTICA" <= '{execution_date}'
		 	AND "Статус отгрузки"  IN ('Разнарядка','Отгрузка', 'Пусто', 'Приложение')
			AND TO_DATE("Месяц отгрузки", 'YYYY-MM') > '{execution_date}'
		 GROUP BY key
	),
	sq5 AS(
		 SELECT
			key,
		 	SUM(Количество) 											AS "Отгрузка в счет следующего месяца" 
		 FROM base_query
		 WHERE "Период контрактации VERTICA" = '{next_month}' 
		 	AND TO_DATE("Месяц отгрузки", 'YYYY-MM') = '{execution_date}'
			AND "Статус отгрузки"  IN ('Отгрузка')
		GROUP BY key
	),
	sq6 AS(
		 SELECT
			key,
		 	SUM(Количество) 									AS "Отгрузка в предыдущем месяце из плана текущего месяца" 
		 FROM base_query
		 WHERE "Период контрактации VERTICA" = '{execution_date}'
		 	AND TO_DATE("Месяц отгрузки", 'YYYY-MM') = '{previous_month}' 
			AND "Статус отгрузки"  IN ('Отгрузка')
		GROUP BY key
	),
	sq7 AS(
		SELECT
			*,
			'{plan_date}' AS "Дата плана",
			HASH("Направление реализации", "Наименование", "Производитель", "Город", "Вид оплаты", "Вид продукции") AS key 
		FROM sttgaz.dm_isc_contracting_plan
		WHERE DATE_TRUNC('minute', ts) = (
				SELECT DATE_TRUNC('minute', MIN(ts))
				FROM sttgaz.dm_isc_contracting_plan
				WHERE "Дата" = '{plan_date}'
			)
	)
SELECT
	m."Период",
	m."Направление реализации",
	m."Дилер",
	m."Производитель",
	m."Город",
	m."Вид оплаты",
	m."Вид продукции",
	"Догруз на начало месяца",
	"План контрактации",
	"План контрактации. Неделя 1",
	"План контрактации. Неделя 2",
	"План контрактации. Неделя 3",
	"План контрактации. Неделя 4",
	"Факт выдачи ОР",
	"Догруз на конец месяца",
	"Отгрузка в счет следующего месяца",
	"Отгрузка в предыдущем месяце из плана текущего месяца"
FROM matrix AS m
LEFT JOIN sq1
	ON m.key = sq1.key
LEFT JOIN sq2
	ON m.key = sq2.key
LEFT JOIN sq3
	ON m.key = sq3.key
LEFT JOIN sq4
	ON m.key = sq4.key
LEFT JOIN sq5
	ON m.key = sq5.key
LEFT JOIN sq6
	ON m.key = sq6.key
LEFT JOIN sq7
	ON m.key = sq6.key;