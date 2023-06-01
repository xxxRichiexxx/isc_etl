DROP VIEW IF EXISTS sttgaz.dm_isc_realization_v;

CREATE OR REPLACE VIEW sttgaz.dm_isc_realization_v AS
WITH 
sq1 AS(
	SELECT
		DATE_TRUNC('month', "День документа")::date		AS "Месяц",
		d."Направление реализации с учетом УКП",
		div.Наименование								AS "Дивизион",
		m.Наименование									AS "Производитель",
		"Классификатор подробно по дивизионам 22",
		"Товар",
		SUM("Наличие")									AS "Реализовано",
		SUM("Оборот")									AS "Оборот",
		SUM("Оборот без НДС")							AS "Оборот без НДС",
		SUM("Сумма возмещения без НДС")					AS "Сумма возмещения без НДС"
	FROM sttgaz.dds_isc_realization 		AS r 
	LEFT JOIN sttgaz.dds_isc_counteragent 	AS c
		ON r."Контрагент ID"  = c.id 
	LEFT JOIN sttgaz.dds_isc_counteragent 	AS rec
		ON r."Получатель ID"  = rec.id 
	LEFT JOIN sttgaz.dds_isc_dealer_unit 	AS du
		ON r."Площадка дилера ID" = du.id 
	LEFT JOIN sttgaz.dds_isc_product 		AS p
		ON r."Продукт ID" = p.id 
	LEFT JOIN sttgaz.dds_isc_DirectionOfImplementationWithUKP AS d
		ON r."Направление реализации с учетом УКП ID" =d.id
	LEFT JOIN sttgaz.dds_isc_manufacturer 	AS m 
		ON p."Производитель ID"  = m.id
	LEFT JOIN sttgaz.dds_isc_division  		AS div
		ON p."Дивизион ID"  = div.id
	WHERE DATE_TRUNC('month', r.Период)::date >= (DATE_TRUNC('year', '{{execution_date}}'::date)::date -  INTERVAL '2 YEAR')
	GROUP BY
		"Месяц",
		d."Направление реализации с учетом УКП",
		div.Наименование,
		m.Наименование,
		"Классификатор подробно по дивизионам 22",
		"Товар"
	ORDER BY "Месяц"
),
sq2 AS(
	SELECT DISTINCT DATE_TRUNC('MONTH', ts)::date AS "Месяц"
	FROM (SELECT (DATE_TRUNC('year', NOW()) -  INTERVAL '2 YEAR') as tm UNION ALL SELECT NOW()) as t
	TIMESERIES ts as '1 DAY' OVER (ORDER BY t.tm)
),
sq3 AS(
	SELECT DISTINCT "Направление реализации с учетом УКП", "Дивизион", "Производитель", "Классификатор подробно по дивизионам 22", "Товар" FROM sq1
),
matrix AS(
	SELECT *
	FROM sq2
	CROSS JOIN sq3
),
data AS (
	SELECT
		m."Месяц",
		m."Направление реализации с учетом УКП",
		m."Дивизион",
		m."Производитель",
		m."Классификатор подробно по дивизионам 22",
		m."Товар",
		s1."Реализовано",
		s1."Оборот",
		s1."Оборот без НДС",
		s1."Сумма возмещения без НДС",
		s2."Реализовано" AS "Реализовано АППГ",
		s2."Оборот" AS "Оборот АППГ",
		s2."Оборот без НДС" AS "Оборот без НДС АППГ",
		s2."Сумма возмещения без НДС" AS "Сумма возмещения без НДС АППГ"
	FROM matrix AS m
	LEFT JOIN sq1 AS s1
		ON HASH(m."Месяц", m."Направление реализации с учетом УКП", m."Дивизион", m."Производитель", m."Классификатор подробно по дивизионам 22", m."Товар") = 
			HASH(s1."Месяц",s1."Направление реализации с учетом УКП", s1."Дивизион", s1."Производитель", s1."Классификатор подробно по дивизионам 22", s1."Товар")
	LEFT JOIN sq1 AS s2
		ON m."Месяц" = s2."Месяц" + INTERVAL '1 YEAR'
		AND HASH(m."Направление реализации с учетом УКП", m."Дивизион", m."Производитель", m."Классификатор подробно по дивизионам 22", m."Товар") = 
			HASH(s2."Направление реализации с учетом УКП", s2."Дивизион", s2."Производитель", s2."Классификатор подробно по дивизионам 22", s2."Товар")
),
windows AS(
SELECT
	*,
	SUM("Реализовано") OVER (PARTITION BY DATE_TRUNC('YEAR', "Месяц"), "Направление реализации с учетом УКП", "Дивизион", "Производитель", "Классификатор подробно по дивизионам 22", "Товар"  ORDER BY "Месяц") AS "Реализовано с начала года",
	SUM("Реализовано АППГ") OVER (PARTITION BY DATE_TRUNC('YEAR', "Месяц"), "Направление реализации с учетом УКП", "Дивизион", "Производитель", "Классификатор подробно по дивизионам 22", "Товар"  ORDER BY "Месяц") AS "Реализовано с начала прошлого года",
	CASE
		WHEN "Направление реализации с учетом УКП" LIKE 'РФ-%'
			THEN 'РФ'
		WHEN "Направление реализации с учетом УКП" LIKE 'СНГ-%'
			THEN 'СНГ'
		WHEN "Направление реализации с учетом УКП" LIKE 'ДРКП -%'
			THEN 'ДРКП'
		WHEN "Направление реализации с учетом УКП" IS NULL
			THEN NULL
		ELSE
			'Прочее'
		END AS "Направление",
	CASE
		WHEN "Производитель" LIKE '%ГАЗ%'
			THEN 'ГАЗ'
		WHEN "Производитель" LIKE '%ПАЗ%'
			THEN 'ПАЗ'
		WHEN "Производитель" LIKE '%КАВЗ%'
			THEN 'КАВЗ'
		ELSE
			'Прочее'
		END AS "Завод"
FROM data
)
SELECT *
FROM windows
WHERE
	"Реализовано" IS NOT NULL
	OR "Оборот" IS NOT NULL
	OR "Оборот без НДС" IS NOT NULL
	OR "Сумма возмещения без НДС" IS NOT NULL
	OR "Реализовано АППГ" IS NOT NULL
	OR "Оборот АППГ" IS NOT NULL
	OR "Оборот без НДС АППГ" IS NOT NULL
	OR "Сумма возмещения без НДС" IS NOT NULL
	OR "Реализовано с начала года" IS NOT NULL
	OR "Реализовано с начала прошлого года" IS NOT NULL;

GRANT SELECT ON TABLE sttgaz.dm_isc_realization_v TO PowerBI_Integration WITH GRANT OPTION;