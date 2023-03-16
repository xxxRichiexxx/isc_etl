INSERT INTO sttgaz.dds_isc_buyer 
("Регион", "Название", "ИНН", "ОКВЭД", "Род занятий(сфера деятельности)", "Сфера использования", ts)
WITH 
    sq AS(
        SELECT HASH("Регион", "Название", "ИНН", "ОКВЭД", "Род занятий(сфера деятельности)", "Сфера использования")
        FROM sttgaz.dds_isc_buyer 
    )
SELECT DISTINCT
    "BuyersRegion",
    "FinalBuyer",
    "BuyerINN",
    "okved",
    "LineOfWork",
    "ScopeOfUse",
    NOW()
FROM sttgaz.stage_isc_sales
WHERE load_date = '{{execution_date.date()}}'
    AND HASH("BuyersRegion", "FinalBuyer", "BuyerINN", "okved", "LineOfWork", "ScopeOfUse") NOT IN 
        (SELECT * FROM sq); 