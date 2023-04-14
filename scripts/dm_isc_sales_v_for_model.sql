CREATE OR REPLACE VIEW sttgaz.dm_isc_sales_v_for_model AS
WITH
    dds_data AS(
        SELECT
            d.id AS "Дилер ID",
            t.id AS "Территория продаж",
            s."Дата продажи",
            c.id AS "Классификатор ID",
            s."Продано в розницу"
        FROM (SELECT * FROM sttgaz.dds_isc_sales WHERE "Дата продажи" IS NOT NULL AND "Дата продажи" < '2022-07-01') AS s
        LEFT JOIN sttgaz.dds_isc_dealer AS d
            ON s."Дилер ID" = d.id
        LEFT JOIN sttgaz.dds_isc_classifier_2 AS c
        	ON s."Внутренний код" = c."Внутренний код"
        LEFT JOIN sttgaz.dds_isc_sales_territory AS t
        	ON s."Территория продаж" = t.name
		WHERE d."Дивизион" IN ('LCV', 'MCV')
			AND s."Направление реализации по приложению" LIKE 'РФ-%'
    ),
	sq1 AS(
		SELECT DISTINCT
			"Дилер ID",
			"Территория продаж",
			"Классификатор ID"
		FROM dds_data
	),
	sq2 AS(
        SELECT DISTINCT DATE_TRUNC('DAY', ts)::date AS "Дата"
        FROM (SELECT '2020-01-01 00:00:00'::TIMESTAMP as tm UNION ALL SELECT '2022-06-30 00:00:00'::TIMESTAMP) as t
        TIMESERIES ts as '1 DAY' OVER (ORDER BY t.tm)
	),
	sq3 AS(
		SELECT *
		FROM sq1
		CROSS JOIN sq2
	),
	sq4 AS(
		SELECT 
			to_char(sq3."Дата",'DD.MM.YYYY') 										AS "Дата",
			sq3."Дилер ID" 															AS "Код_Клиента",
			sq3."Территория продаж" 												AS "Код_Локации",
			sq3."Классификатор ID" 													AS "Код_Продукта",
			COALESCE(SUM("Продано в розницу"), 0)									AS "Количество"
		FROM sq3
		LEFT JOIN dds_data AS s
			ON sq3."Дата" = s."Дата продажи" 
			AND sq3."Дилер ID" = s."Дилер ID"
			AND sq3."Территория продаж" = s."Территория продаж"
			AND sq3."Классификатор ID" = s."Классификатор ID"
		GROUP BY
			to_char(sq3."Дата",'DD.MM.YYYY'),
			sq3."Дилер ID",
			sq3."Территория продаж",
			sq3."Классификатор ID" 
	)
SELECT *																															
FROM sq4;

GRANT SELECT ON TABLE sttgaz.dm_isc_sales_v_for_model TO PowerBI_Integration WITH GRANT OPTION;