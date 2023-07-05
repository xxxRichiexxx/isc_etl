DELETE FROM sttgaz.dm_isc_contracting_plan 
WHERE "Дата" = '{{execution_date.date()}}';

INSERT INTO sttgaz.dm_isc_contractin_plan
WITH 
	base_query AS(
		 SELECT
			*,
			CASE
				WHEN "Договор" LIKE 'ДФ%' THEN 'Отсрочка'
				WHEN "Договор" LIKE 'ДР55/4%' THEN 'Отсрочка'
				WHEN "Договор" LIKE 'ДР55%' THEN 'Предоплата'
				ELSE 'Неизвестно'			
			END 													AS "Вид оплаты"																							AS "Вид оплаты",
		 FROM sttgaz.dds_isc_orders 								AS o
		 LEFT JOIN sttgaz.dds_isc_counteragent 						AS c
		 	ON o."Покупатель ID" = c.id
		 WHERE o."Период контрактации VERTICA" = '{{execution_date.date().replace(day=1)}}'::date
	)
SELECT
	'{{execution_date.date()}}'::date 					AS "Дата",
	"Направление реализации",
	"Дилер",
	"Производитель",
	"Город",
	"Вид оплаты",
	"Вид продукции",
	SUM(Количество) 									AS "План контрактации",
	ROUND(SUM(Количество)*0.7, 0) 						AS "План контрактации. Неделя 1",
	ROUND(SUM(Количество)*0.2, 0) 						AS "План контрактации. Неделя 2",
	ROUND(SUM(Количество)*0.05, 0) 						AS "План контрактации. Неделя 3",
	SUM(Количество) - ROUND(SUM(Количество)*0.7, 0) 
		 			- ROUND(SUM(Количество)*0.05, 0)
		 			- ROUND(SUM(Количество)*0.2, 0)		AS "План контрактации. Неделя 4"
FROM base_query
GROUP BY
	"Направление реализации",
	"Дилер",
	"Производитель",
	"Город",
	"Вид оплаты",
	"Вид продукции";