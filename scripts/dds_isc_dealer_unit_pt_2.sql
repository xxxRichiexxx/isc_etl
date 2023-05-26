INSERT INTO sttgaz.dds_isc_dealer_unit
("Наименование_дилера", "Площадка_дилера", "ts")
WITH sq1 AS(
    SELECT DISTINCT
        HASH("Наименование_дилера",
             "Площадка_дилера")
    FROM sttgaz.dds_isc_dealer_unit
)
SELECT DISTINCT
    "RecipientFullName",
	"Recipient",
	NOW()
FROM sttgaz.stage_isc_sales
WHERE DATE_TRUNC('MONTH', load_date) IN(
				'{{execution_date.date().replace(day=1)}}',
				'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
            )
    AND HASH("RecipientFullName", "Recipient")
        NOT IN (SELECT * FROM sq1); 