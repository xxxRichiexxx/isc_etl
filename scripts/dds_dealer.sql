INSERT INTO sttgaz.dds_isc_dealer
("ИСК ID", "Дивизион", "Название", "Полное название (организация)", "Название из системы скидок", ts)
WITH 
    sq AS(
        SELECT DISTINCT HASH(
            "ИСК ID", 
            "Название",
            "Полное название (организация)",
            "Название из системы скидок",
            "Стоянка ID",
            "Стоянка",
            "Город стоянки ID",
            "Город стоянки"
        )
        FROM sttgaz.dds_isc_dealer
    )
SELECT DISTINCT
        "RecipientID",
        "Recipient",
        "RecipientFullName",
        "CustomerFromDiscountSystem",
        "StoyankaID",
        "Stoyanka",
        "StoyankaCityID",
        "StoyankaCity",
        NOW()
FROM sttgaz.stage_isc_sales
WHERE DATE_TRUNC('MONTH', load_date) IN(
				'{{execution_date.date().replace(day=1)}}',
				'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
            )
    AND HASH("RecipientID", "Recipient", "RecipientFullName", "CustomerFromDiscountSystem", 
             "StoyankaID", "Stoyanka", "StoyankaCityID", "StoyankaCity",)
        NOT IN (SELECT * FROM sq); 