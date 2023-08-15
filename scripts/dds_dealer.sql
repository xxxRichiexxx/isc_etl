INSERT INTO sttgaz.dds_isc_dealer
("ИСК ID", "Дивизион", "Название", "Полное название (организация)", "Название из системы скидок", ts)
WITH 
    sq AS(
        SELECT DISTINCT HASH("ИСК ID", "Дивизион", "Название", "Полное название (организация)", "Название из системы скидок")
        FROM sttgaz.dds_isc_dealer
    )
SELECT DISTINCT
        "RecipientID",
        "division",
        "Recipient",
        "RecipientFullName",
        "CustomerFromDiscountSystem",
        NOW()
FROM sttgaz.stage_isc_sales
WHERE DATE_TRUNC('MONTH', load_date) IN(
				'{{execution_date.date().replace(day=1)}}',
				'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
            )
    AND HASH("RecipientID", "division", "Recipient", "RecipientFullName", "CustomerFromDiscountSystem")
        NOT IN (SELECT * FROM sq); 