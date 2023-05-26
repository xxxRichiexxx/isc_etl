INSERT INTO sttgaz.dds_isc_DirectionOfImplementationWithUKP
("Направление реализации с учетом УКП")
WITH 
sq1 AS(
	SELECT DISTINCT HASH("Направление реализации с учетом УКП")
	FROM sttgaz.dds_isc_DirectionOfImplementationWithUKP
)
SELECT DISTINCT DirectionOfImplementationWithUKP 
FROM sttgaz.stage_isc_sales AS s
WHERE DATE_TRUNC('MONTH', load_date) IN(
		'{{execution_date.date().replace(day=1)}}',
		'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
	)
    AND HASH(DirectionOfImplementationWithUKP) NOT IN (SELECT * FROM sq1);