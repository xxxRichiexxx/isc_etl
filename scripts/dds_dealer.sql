INSERT INTO sttgaz.dds_isc_dealer
("Дивизион", "Название", "Полное название (организация)", ts)
WITH 
    sq AS(
        SELECT HASH("Дивизион", "Название", "Полное название (организация)")
        FROM sttgaz.dds_isc_dealer
    )
SELECT DISTINCT
        "division",
        "Recipient",
        "RecipientFullName",
        NOW()
FROM sttgaz.stage_isc_sales
WHERE DATE_TRUNC('MONTH', load_date) IN(
				'{{execution_date.date().replace(day=1)}}',
				'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
            )
    AND HASH("division", "Recipient", "RecipientFullName")
        NOT IN (SELECT * FROM sq); 