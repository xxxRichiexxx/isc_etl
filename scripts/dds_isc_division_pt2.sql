INSERT INTO sttgaz.dds_isc_division
("Наименование")
WITH 
sq1 AS(
	SELECT DISTINCT HASH("Наименование")
	FROM sttgaz.dds_isc_division
)
SELECT DISTINCT division 
FROM sttgaz.stage_isc_sales AS s
WHERE DATE_TRUNC('MONTH', load_date) IN(
		'{{execution_date.date().replace(day=1)}}',
		'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
	)
    AND HASH(division) NOT IN (SELECT * FROM sq1);