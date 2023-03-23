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
WHERE load_date = '{{execution_date.date()}}'
    AND HASH("division", "Recipient", "RecipientFullName")
        NOT IN (SELECT * FROM sq); 