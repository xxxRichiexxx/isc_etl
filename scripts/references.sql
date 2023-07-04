INSERT INTO sttgaz.dds_isc_classifier
("Сцеп","ВнК_ID","Внутренний код","Опции","Двигатель",
"Гр1 Дивизион","Гр2 Кузов","Гр3 Поколение","Гр4 Семейство",
"Гр5 Назначение","Гр6 Подр (места)","Гр7 Размер","Гр8 Привод",
"Гр9 Двигатель","Гр10 Опции","Гр11 группа АЕБ")
SELECT DISTINCT *
FROM sttgaz.stage_isc_classifier
WHERE "Options" IS NULL

CREATE OR REPLACE VIEW sttgaz.dm_isc_classifier AS
SELECT  *
FROM sttgaz.dds_isc_classifier

GRANT SELECT ON TABLE sttgaz.dm_isc_classifier TO PowerBI_Integration WITH GRANT OPTION;




CREATE OR REPLACE VIEW sttgaz.dm_isc_dealer AS
SELECT  *
FROM sttgaz.dds_isc_dealer;

GRANT SELECT ON TABLE sttgaz.dm_isc_dealer TO PowerBI_Integration WITH GRANT OPTION;




INSERT INTO sttgaz.dds_isc_classifier_2
("Внутренний код", "Значение", "Вид товара", "Вид продукции")
SELECT DISTINCT *
FROM sttgaz.stage_isc_classifier_2;

CREATE OR REPLACE VIEW sttgaz.dm_isc_classifier_2 AS
SELECT  *
FROM sttgaz.dds_isc_classifier_2;

GRANT SELECT ON TABLE sttgaz.dm_isc_classifier_2 TO PowerBI_Integration WITH GRANT OPTION;



CREATE OR REPLACE VIEW sttgaz.dm_isc_classifier_2_for_contractors AS
SELECT
	 id,
    "Значение",
    "Вид товара",
    "Вид продукции"
FROM sttgaz.dds_isc_classifier_2;

GRANT SELECT ON TABLE sttgaz.dm_isc_classifier_2_for_contractors TO PowerBI_Integration WITH GRANT OPTION;

DROP VIEW IF EXISTS sttgaz.dm_isc_contracting_v;
CREATE OR REPLACE VIEW sttgaz.dm_isc_contracting_v AS
WITH 
	base_query AS(
		 SELECT
			*,
			CASE
				WHEN "Договор" LIKE 'ДФ%' THEN 'Отсрочка'
				WHEN "Договор" LIKE 'ДР55/4%' THEN 'Отсрочка'
				WHEN "Договор" LIKE 'ДР55%' THEN 'Предоплата'
				ELSE 'Неизвестно'			
			END 													AS "Вид оплаты",
			HASH("Направление реализации", "Наименование", "Производитель", "Город", "Вид оплаты", "Вид продукции") AS key 		
		 FROM sttgaz.dds_isc_orders 			AS o
		 LEFT JOIN sttgaz.dds_isc_counteragent 	AS c
		 	ON o."Покупатель ID" = c.id
		 WHERE o."Период контрактации VERTICA" > DATE_TRUNC('month', '2023-06-01'::date - INTERVAL '8 month')::date
			AND o.Производитель LIKE '%ГАЗ ПАО%'
			AND o."Направление реализации" LIKE 'РФ%'
	),
	matrix AS(
		SELECT DISTINCT 
			'2023-06-01' 						AS "Период",
			"Направление реализации",
			Наименование 						AS "Дилер",
			Производитель,
			Город,
			"Вид оплаты",
			"Вид продукции",
			key
		FROM base_query
	),
	sq1 AS(
		SELECT
			'2023-06-01' 						AS "Период",
			key,
			SUM(Количество) 					AS "Догруз на начало месяца"
		FROM base_query
		WHERE "Статус отгрузки"  IN ('Разнарядка', 'Отгрузка')
			AND TO_DATE("Месяц отгрузки", 'YYYY-MM') > '2023-05-01'
			AND "Период контрактации VERTICA"  <= '2023-05-01'   
		GROUP BY key		
	),
	sq2 AS(
		 SELECT
		 	'2023-06-01' 										AS "Период",
			key,
		 	SUM(Количество) 									AS "План контрактации",
		 	ROUND(SUM(Количество)*0.7, 0) 						AS "План контрактации. Неделя 1",
		 	ROUND(SUM(Количество)*0.2, 0) 						AS "План контрактации. Неделя 2",
		 	ROUND(SUM(Количество)*0.05, 0) 						AS "План контрактации. Неделя 3",
		 	SUM(Количество) - ROUND(SUM(Количество)*0.7, 0) 
		 					- ROUND(SUM(Количество)*0.05, 0)
		 					- ROUND(SUM(Количество)*0.2, 0)		AS "План контрактации. Неделя 4"
		FROM base_query
		WHERE "Период контрактации VERTICA" = '2023-06-01'
		GROUP BY key	
	),
	sq3 AS(
		 SELECT
		 	'2023-06-01' 				AS "Период",
			key,
		 	SUM(Количество) 			AS "Факт выдачи ОР"
		 FROM base_query
		 WHERE "Период контрактации VERTICA" = '2023-06-01' 
			AND "Статус отгрузки"  IN ('Разнарядка', 'Отгрузка')
		GROUP BY key
	),
	sq4 AS(
		 SELECT 
		 	'2023-06-01' 				AS "Период",
			key,
		 	SUM(Количество) 			AS "Догруз на конец месяца"
		 FROM base_query
		 WHERE "Период контрактации VERTICA" <= '2023-06-01'
		 	AND "Статус отгрузки"  IN ('Разнарядка','Отгрузка', 'Пусто', 'Приложение')
			AND TO_DATE("Месяц отгрузки", 'YYYY-MM') > '2023-06-01'
		 GROUP BY key
	),
	sq5 AS(
		 SELECT
		 	'2023-06-01' 						AS "Период",
			key,
		 	SUM(Количество) 					AS "Отгрузка в счет следующего месяца" ----117
		 FROM base_query
		 WHERE "Период контрактации VERTICA" = '2023-07-01' 
		 	AND TO_DATE("Месяц отгрузки", 'YYYY-MM') = '2023-06-01'
			AND "Статус отгрузки"  IN ('Отгрузка')
		GROUP BY key
	),
	sq6 AS(
		 SELECT
		 	'2023-06-01' 						AS "Период",
			key,
		 	SUM(Количество) 					AS "Отгрузка в предыдущем месяце из плана текущего месяца" ---138
		 FROM base_query
		 WHERE "Период контрактации VERTICA" = '2023-06-01'
		 	AND TO_DATE("Месяц отгрузки", 'YYYY-MM') = '2023-05-01' 
			AND "Статус отгрузки"  IN ('Отгрузка')
		GROUP BY key
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
	ON m."Период" = sq1."Период" AND m.key = sq1.key
LEFT JOIN sq2
	ON m."Период" = sq2."Период" AND m.key = sq2.key
LEFT JOIN sq3
	ON m."Период" = sq3."Период" AND m.key = sq3.key
LEFT JOIN sq4
	ON m."Период" = sq4."Период" AND m.key = sq4.key
LEFT JOIN sq5
	ON m."Период" = sq5."Период" AND m.key = sq5.key
LEFT JOIN sq6
	ON m."Период" = sq6."Период" AND m.key = sq6.key;  

GRANT SELECT ON TABLE sttgaz.dm_isc_contracting_v TO PowerBI_Integration WITH GRANT OPTION;