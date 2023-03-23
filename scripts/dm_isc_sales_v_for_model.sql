CREATE OR REPLACE VIEW sttgaz.dm_isc_sales_v_for_model AS
WITH
    dds_data AS(
        SELECT
            d.id AS "Дилер ID",
            s."Территория продаж",
            s."Дата продажи",
            c.id AS "Внутренний код",
            s."Продано в розницу"
        FROM (SELECT * FROM sttgaz.dds_isc_sales WHERE "Дата продажи" IS NOT NULL) AS s
        LEFT JOIN sttgaz.dds_isc_dealer AS d
            ON s."Дилер ID" = d.id
        LEFT JOIN sttgaz.dds_isc_classifier AS c
        	ON s."Внутренний код" = c."Внутренний код"
    ),
	sq1 AS(
		SELECT DISTINCT
			"Дилер ID",
			"Территория продаж",
			"Внутренний код"
		FROM dds_data
	),
	sq2 AS(
        SELECT DISTINCT DATE_TRUNC('DAY', ts)::date AS "Дата"
        FROM (SELECT '2020-01-01 00:00:00'::TIMESTAMP as tm UNION ALL SELECT NOW()) as t
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
			sq3."Дилер ID",
			sq3."Территория продаж",
			sq3."Внутренний код",
			COALESCE(SUM("Продано в розницу"), 0)									AS "Продажи в розницу"
		FROM sq3
		LEFT JOIN dds_data AS s
			ON sq3."Дата" = s."Дата продажи" 
			AND sq3."Дилер ID" = s."Дилер ID"
			AND sq3."Территория продаж" = s."Территория продаж"
			AND sq3."Внутренний код" = s."Внутренний код"
		GROUP BY
			to_char(sq3."Дата",'DD.MM.YYYY'),
			sq3."Дилер ID",
			sq3."Территория продаж",
			sq3."Внутренний код" 
	)
SELECT *																															
FROM sq4;

GRANT SELECT ON TABLE sttgaz.dm_isc_sales_v_for_model TO PowerBI_Integration WITH GRANT OPTION;
