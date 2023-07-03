
 ----------------------Догруз на начало месяца-------------------------------
 SELECT
 	'2023-06-01' AS "Период",
 	"Направление реализации",
 	c.Наименование AS "Дилер",
 	o.Производитель,
 	o.Город,
 	SUM(o.Количество) AS "Догруз на начало месяца"
 FROM sttgaz.dds_isc_orders AS o
 LEFT JOIN sttgaz.dds_isc_counteragent AS c
 	ON o."Покупатель ID" = c.id
 WHERE o."Статус отгрузки"  IN ('Разнарядка', 'Отгрузка')
	AND o."Месяц отгрузки" > '2023-05-01' 
	AND o."Период контрактации VERTICA"  <= '2023-05-01'   
GROUP BY "Направление реализации", c.Наименование, o.Производитель , o.Город 

--------------------------------План контрактации----------------------
 SELECT
 	'2023-06-01' AS "Период",
 	"Направление реализации",
 	c.Наименование AS "Дилер",
 	o.Производитель,
 	o.Город,
 	SUM(o.Количество) AS "План контрактации",
 	SUM(o.Количество)*0.7 AS "План контрактации. Неделя 1",
 	SUM(o.Количество)*0.2 AS "План контрактации. Неделя 2",
 	SUM(o.Количество)*0.05 AS "План контрактации. Неделя 3",
 	SUM(o.Количество)*0.05 AS "План контрактации. Неделя 4"
 FROM sttgaz.dds_isc_orders AS o
 LEFT JOIN sttgaz.dds_isc_counteragent AS c
 	ON o."Покупатель ID" = c.id
 WHERE o."Период контрактации VERTICA" = '2023-06-01'
	AND o.Производитель LIKE '%ГАЗ ПАО%'
	AND o."Направление реализации" LIKE 'РФ%'
GROUP BY "Направление реализации", c.Наименование, o.Производитель , o.Город;

---------------------------Факт выдачи ОР---------------------------
 SELECT
 	'2023-06-01' AS "Период",
	"Направление реализации",
 	c.Наименование AS "Дилер",
 	o.Производитель,
 	o.Город,
 	SUM(o.Количество) AS "Факт выдачи ОР"
 FROM sttgaz.dds_isc_orders AS o
 LEFT JOIN sttgaz.dds_isc_counteragent AS c
 	ON o."Покупатель ID" = c.id
 WHERE o."Период контрактации VERTICA" = '2023-06-01' 
	AND o."Статус отгрузки"  IN ('Разнарядка', 'Отгрузка')
	AND o.Производитель LIKE '%ГАЗ ПАО%'
	AND o."Направление реализации" LIKE 'РФ%'
GROUP BY "Направление реализации", c.Наименование, o.Производитель , o.Город;

 ----------------------Догруз на конец месяца-------------------------------
SELECT
 	'2023-06-01' AS "Период",
 	"Направление реализации",
 	c.Наименование AS "Дилер",
 	o.Производитель,
 	o.Город,
 	SUM(o.Количество) AS "Догруз на конец месяца"
FROM sttgaz.dds_isc_orders AS o
LEFT JOIN sttgaz.dds_isc_counteragent AS c
 	ON o."Покупатель ID" = c.id
WHERE o."Статус отгрузки"  IN ('Разнарядка', 'Отгрузка')
	AND o."Месяц отгрузки" > '2023-05-01' 
GROUP BY "Направление реализации", c.Наименование, o.Производитель , o.Город;


 SELECT 
-- 	*
 	SUM(o.Количество)
 FROM sttgaz.dds_isc_orders AS o
 LEFT JOIN sttgaz.dds_isc_counteragent AS c
 	ON o."Покупатель ID" = c.id
 WHERE o."Статус отгрузки"  IN ('Разнарядка','Отгрузка', 'Пусто', 'Приложение')
	AND o."Месяц отгрузки" > '2023-06-01'
	AND o.Производитель LIKE '%ГАЗ ПАО%'
	AND o."Направление реализации" LIKE 'РФ%';
	


------------------Отгрузка в счет следующего месяца---------------------------

 SELECT
 	'2023-06-01' AS "Период",
	"Направление реализации",
 	c.Наименование AS "Дилер",
 	o.Производитель,
 	o.Город,
 	SUM(o.Количество) AS "Отгрузка в счет следующего месяца"
 FROM sttgaz.dds_isc_orders AS o
 LEFT JOIN sttgaz.dds_isc_counteragent AS c
 	ON o."Покупатель ID" = c.id
 WHERE o."Период контрактации VERTICA" = '2023-07-01'
 	AND o."Месяц отгрузки" = '2023-06'
	AND o."Статус отгрузки"  IN ('Отгрузка')
	AND o.Производитель LIKE '%ГАЗ ПАО%'
	AND o."Направление реализации" LIKE 'РФ%'
GROUP BY "Направление реализации", c.Наименование, o.Производитель , o.Город;



------------------------Отгрузка в предыдущем месяце из плана текущего месяца------------------
 SELECT
 	'2023-06-01' AS "Период",
	"Направление реализации",
 	c.Наименование AS "Дилер",
 	o.Производитель,
 	o.Город,
 	SUM(o.Количество) AS "Отгрузка в предыдущем месяце из плана текущего месяца"
 FROM sttgaz.dds_isc_orders AS o
 LEFT JOIN sttgaz.dds_isc_counteragent AS c
 	ON o."Покупатель ID" = c.id
 WHERE o."Период контрактации VERTICA" = '2023-06-01'
 	AND o."Месяц отгрузки" = '2023-05' 
	AND o."Статус отгрузки"  IN ('Отгрузка')
	AND o.Производитель LIKE '%ГАЗ ПАО%'
	AND o."Направление реализации" LIKE 'РФ%'
GROUP BY "Направление реализации",  c.Наименование, o.Производитель , o.Город;

---------------------------------------------------------------------------

DROP VIEW IF EXISTS sttgaz.dm_isc_contracting_v;
CREATE OR REPLACE VIEW sttgaz.dm_isc_contracting_v AS
WITH 
	base_query AS(
		 SELECT
			*
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
			Город
		FROM base_query
	),
	sq1 AS(
		SELECT
			'2023-06-01' 						AS "Период",
			"Направление реализации",
			Наименование 						AS "Дилер",
			Производитель,
			Город,
			SUM(Количество) 					AS "Догруз на начало месяца"
		FROM base_query
		WHERE "Статус отгрузки"  IN ('Разнарядка', 'Отгрузка')
			AND "Месяц отгрузки" > '2023-05-01' 
			AND "Период контрактации VERTICA"  <= '2023-05-01'   
		GROUP BY "Направление реализации", Наименование, Производитель , Город 		
	),
	sq2 AS(
		 SELECT
		 	'2023-06-01' 										AS "Период",
		 	"Направление реализации",
		 	Наименование 										AS "Дилер",
		 	Производитель,
		 	Город,
		 	SUM(Количество) 									AS "План контрактации",
		 	ROUND(SUM(Количество)*0.7, 0) 						AS "План контрактации. Неделя 1",
		 	ROUND(SUM(Количество)*0.2, 0) 						AS "План контрактации. Неделя 2",
		 	ROUND(SUM(Количество)*0.05, 0) 						AS "План контрактации. Неделя 3",
		 	SUM(Количество) - ROUND(SUM(Количество)*0.7, 0) 
		 					- ROUND(SUM(Количество)*0.05, 0)
		 					- ROUND(SUM(Количество)*0.2, 0)		AS "План контрактации. Неделя 4"
		FROM base_query
		WHERE "Период контрактации VERTICA" = '2023-06-01'
		GROUP BY "Направление реализации", Наименование, Производитель , Город	
	),
	sq3 AS(
		 SELECT
		 	'2023-06-01' 				AS "Период",
			"Направление реализации",
		 	Наименование 				AS "Дилер",
		 	Производитель,
		 	Город,
		 	SUM(Количество) 			AS "Факт выдачи ОР"
		 FROM base_query
		 WHERE "Период контрактации VERTICA" = '2023-06-01' 
			AND "Статус отгрузки"  IN ('Разнарядка', 'Отгрузка')
		GROUP BY "Направление реализации", Наименование, Производитель , Город
	),
	sq4 AS(
		 SELECT 
		 	'2023-06-01' 				AS "Период",
			"Направление реализации",
		 	Наименование 				AS "Дилер",
		 	Производитель,
		 	Город,
		 	SUM(Количество) 			AS "Догруз на конец месяца"
		 FROM base_query
		 WHERE "Статус отгрузки"  IN ('Разнарядка','Отгрузка', 'Пусто', 'Приложение')
			AND "Месяц отгрузки" > '2023-06-01'
		 GROUP BY "Направление реализации", Наименование, Производитель , Город
	),
	sq5 AS(
		 SELECT
		 	'2023-06-01' AS "Период",
			"Направление реализации",
		 	Наименование AS "Дилер",
		 	Производитель,
		 	Город,
		 	SUM(Количество) AS "Отгрузка в счет следующего месяца"
		 FROM base_query
		 WHERE "Период контрактации VERTICA" = '2023-07-01'
		 	AND "Месяц отгрузки" = '2023-06'
			AND "Статус отгрузки"  IN ('Отгрузка')
		GROUP BY "Направление реализации", Наименование, Производитель , Город	
	),
	sq6 AS(
		 SELECT
		 	'2023-06-01' AS "Период",
			"Направление реализации",
		 	Наименование AS "Дилер",
		 	Производитель,
		 	Город,
		 	SUM(Количество) AS "Отгрузка в предыдущем месяце из плана текущего месяца"
		 FROM base_query
		 WHERE "Период контрактации VERTICA" = '2023-06-01'
		 	AND "Месяц отгрузки" = '2023-05' 
			AND "Статус отгрузки"  IN ('Отгрузка')
			AND Производитель LIKE '%ГАЗ ПАО%'
			AND "Направление реализации" LIKE 'РФ%'
		GROUP BY "Направление реализации",  Наименование, Производитель , Город
	)
SELECT
	m."Период",
	m."Направление реализации",
	m."Дилер",
	m."Производитель",
	m."Город",
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
	ON HASH(m."Период", m."Направление реализации", m."Дилер", m."Производитель", m."Город") =
	   HASH(sq1."Период", sq1."Направление реализации", sq1."Дилер", sq1."Производитель", sq1."Город")
LEFT JOIN sq2
	ON HASH(m."Период", m."Направление реализации", m."Дилер", m."Производитель", m."Город") =
	   HASH(sq2."Период", sq2."Направление реализации", sq2."Дилер", sq2."Производитель", sq2."Город")
LEFT JOIN sq3
	ON HASH(m."Период", m."Направление реализации", m."Дилер", m."Производитель", m."Город") =
	   HASH(sq3."Период", sq3."Направление реализации", sq3."Дилер", sq3."Производитель", sq3."Город")
LEFT JOIN sq4
	ON HASH(m."Период", m."Направление реализации", m."Дилер", m."Производитель", m."Город") =
	   HASH(sq4."Период", sq4."Направление реализации", sq4."Дилер", sq4."Производитель", sq4."Город")
LEFT JOIN sq5
	ON HASH(m."Период", m."Направление реализации", m."Дилер", m."Производитель", m."Город") =
	   HASH(sq5."Период", sq5."Направление реализации", sq5."Дилер", sq5."Производитель", sq5."Город")
LEFT JOIN sq6
	ON HASH(m."Период", m."Направление реализации", m."Дилер", m."Производитель", m."Город") =
	   HASH(sq6."Период", sq6."Направление реализации", sq6."Дилер", sq6."Производитель", sq6."Город");  

GRANT SELECT ON TABLE sttgaz.dm_isc_contracting_v TO PowerBI_Integration WITH GRANT OPTION;