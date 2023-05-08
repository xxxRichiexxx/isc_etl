INSERT INTO sttgaz.dds_isc_client_stt
	("Клиент",
	 "ts")
WITH 
sq1 AS(
	SELECT DISTINCT HASH(
		"Клиент"
	)
	FROM sttgaz.dds_isc_client_stt
)
SELECT DISTINCT
	Client,
	NOW()
FROM sttgaz.stage_isc_realization r
WHERE DATE_TRUNC('MONTH', load_date) IN(
		'{{execution_date.date().replace(day=1)}}',
		'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
	)
    AND HASH(Client) NOT IN (SELECT * FROM sq1);