INSERT INTO sttgaz.dds_isc_counteragent_stt
	("Клиент",
	 "Получатель",
	 "Площадка дилера ISK ID",
	 "Площадка дилера")
SELECT DISTINCT
	Client,
	"Recipient",
	"DealersUnitID",
	"DealersUnit"
FROM sttgaz.stage_isc_realization r