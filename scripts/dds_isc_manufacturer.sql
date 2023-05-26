INSERT INTO sttgaz.dds_isc_manufacturer
("Наименование")
WITH 
sq1 AS(
	SELECT DISTINCT HASH("Наименование")
	FROM sttgaz.dds_isc_manufacturer
)
SELECT DISTINCT Manufacturer 
FROM sttgaz.stage_isc_realization AS r
WHERE DATE_TRUNC('MONTH', load_date) IN(
		'{{execution_date.date().replace(day=1)}}',
		'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
	)
    AND Manufacturer IS NOT NULL
    AND HASH(Manufacturer) NOT IN (SELECT * FROM sq1);