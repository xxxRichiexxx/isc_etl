SELECT DROP_PARTITIONS(
    'sttgaz.dm_isc_contracting',
    '{{execution_date.date().replace(day=1)}}',
    '{{execution_date.date().replace(day=1)}}'
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
		 WHERE o."Период контрактации VERTICA" > DATE_TRUNC('month', '{{execution_date.date().replace(day=1)}}'::date - INTERVAL '8 month')::date
	),
	matrix AS(
		SELECT DISTINCT 
			'{{execution_date.date().replace(day=1)}}'::date		AS "Период",
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
			'{{execution_date.date().replace(day=1)}}'::date 	AS "Период",
			key,
			SUM(Количество) 									AS "Догруз на начало месяца"
		FROM base_query
		WHERE "Статус отгрузки"  IN ('Разнарядка', 'Отгрузка')
			AND TO_DATE("Месяц отгрузки", 'YYYY-MM') >= '{{execution_date.date().replace(day=1)}}'
			AND "Период контрактации VERTICA"  < '{{execution_date.date().replace(day=1)}}'  
		GROUP BY key		
	),
	sq2 AS(
		 SELECT
		 	'{{execution_date.date().replace(day=1)}}'::date	AS "Период",
			key,
		 	SUM(Количество) 									AS "План контрактации",
		 	ROUND(SUM(Количество)*0.7, 0) 						AS "План контрактации. Неделя 1",
		 	ROUND(SUM(Количество)*0.2, 0) 						AS "План контрактации. Неделя 2",
		 	ROUND(SUM(Количество)*0.05, 0) 						AS "План контрактации. Неделя 3",
		 	SUM(Количество) - ROUND(SUM(Количество)*0.7, 0) 
		 					- ROUND(SUM(Количество)*0.05, 0)
		 					- ROUND(SUM(Количество)*0.2, 0)		AS "План контрактации. Неделя 4"
		FROM base_query
		WHERE "Период контрактации VERTICA" = '{{execution_date.date().replace(day=1)}}'
		GROUP BY key	
	),
	sq3 AS(
		 SELECT
		 	'{{execution_date.date().replace(day=1)}}'::date		AS "Период",
			key,
		 	SUM(Количество) 										AS "Факт выдачи ОР"
		 FROM base_query
		 WHERE "Период контрактации VERTICA" = '{{execution_date.date().replace(day=1)}}' 
			AND "Статус отгрузки"  IN ('Разнарядка', 'Отгрузка')
		GROUP BY key
	),