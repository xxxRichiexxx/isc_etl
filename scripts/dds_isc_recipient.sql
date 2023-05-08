INSERT INTO sttgaz.dds_isc_recipient
	("Получатель",
	 "Площадка дилера ISK ID",
	 "Площадка дилера",
	 "Дивизион",
	 "ts")
WITH 
sq1 AS(
	SELECT DISTINCT HASH(
	 	"Получатель",
	 	"Площадка дилера ISK ID",
	 	"Площадка дилера",
	 	"Дивизион"
	)
	FROM sttgaz.dds_isc_recipient
)
SELECT DISTINCT
	"Recipient",
	"DealersUnitID",
	"DealersUnit",
	"Division",
	NOW()
FROM sttgaz.stage_isc_realization r
WHERE DATE_TRUNC('MONTH', load_date) IN(
		'{{execution_date.date().replace(day=1)}}',
		'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
	)
    AND HASH("Recipient", "DealersUnitID", "DealersUnit", "Division") NOT IN (SELECT * FROM sq1);