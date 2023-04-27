INSERT INTO sttgaz.dds_isc_counteragent_stt
	("Клиент",
	 "Получатель",
	 "Площадка дилера ISK ID",
	 "Площадка дилера",
	 "Дивизион")
SELECT DISTINCT
	Client,
	"Recipient",
	"DealersUnitID",
	"DealersUnit",
	"Division"
FROM sttgaz.stage_isc_realization r