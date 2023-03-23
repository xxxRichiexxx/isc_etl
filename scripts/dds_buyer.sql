INSERT INTO sttgaz.dds_isc_buyer 
("Регион", "Название", "ИНН", "ОКВЭД", "Род занятий(сфера деятельности)", "Сфера использования", "Холдинг", ts)
WITH 
    sq AS(
        SELECT HASH("Регион", "Название", "ИНН", "ОКВЭД", "Род занятий(сфера деятельности)", "Сфера использования", "Холдинг")
        FROM sttgaz.dds_isc_buyer 
    )
SELECT DISTINCT
    "BuyersRegion",
    "FinalBuyer",
    "BuyerINN",
    "okved",
    "LineOfWork",
    "ScopeOfUse",
    "clientsHolding",
    NOW()
FROM sttgaz.stage_isc_sales
WHERE load_date = '{{execution_date.date()}}'
    AND HASH("BuyersRegion", "FinalBuyer", "BuyerINN", "okved", "LineOfWork", "ScopeOfUse", "clientsHolding") NOT IN 
        (SELECT * FROM sq); 