INSERT INTO sttgaz.dds_isc_counteragent
	("Наименование")
WITH 
sq1 AS(
	SELECT DISTINCT HASH(
		"Наименование"
	)
	FROM sttgaz.dds_isc_counteragent
),
sq2 AS(
	SELECT DISTINCT
		"Client",
		"Recipient"
	FROM sttgaz.stage_isc_realization r
	WHERE DATE_TRUNC('MONTH', load_date) IN(
			'{{execution_date.date().replace(day=1)}}',
			'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
		)	
)
SELECT DISTINCT
	"Client"
FROM sq2
WHERE HASH("Client") NOT IN (SELECT * FROM sq1)
UNION
SELECT DISTINCT
	"Recipient"
FROM sq2
WHERE HASH("Recipient") NOT IN (SELECT * FROM sq1);