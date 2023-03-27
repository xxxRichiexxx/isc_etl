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
WHERE DATE_TRUNC('MONTH', load_date) IN(
				'{{execution_date.date().replace(day=1)}}',
				'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
            )
    AND HASH("BuyersRegion", "FinalBuyer", "BuyerINN", "okved", "LineOfWork", "ScopeOfUse", "clientsHolding") NOT IN 
        (SELECT * FROM sq); 