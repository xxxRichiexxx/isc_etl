
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