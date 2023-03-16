MERGE
into sttgaz.dds_isc_buyer AS tgt 
USING 
    (SELECT
        "BuyersRegion",
        "FinalBuyer",
        "BuyerINN",
        "okved",
        "LineOfWork",
        "ScopeOfUse"
    FROM sttgaz.stage_isc_sales
    WHERE load_date = '{{execution_date.date()}}') AS src
ON  
    tgt."Регион" = src.BuyersRegion
    AND tgt."Название" = src.FinalBuyer
    AND tgt."ИНН" = src.BuyerINN
    AND tgt."ОКВЭД" = src.okved
    AND tgt."Род занятий(сфера деятельности)" = src.LineOfWork
    AND tgt."Сфера использования" = src.ScopeOfUse
WHEN MATCHED
    THEN UPDATE SET
        "Регион" = src.BuyersRegion,
        "Название" = src.FinalBuyer,
        "ИНН" = src.BuyerINN,
        "ОКВЭД" = src.okved,
        "Род занятий(сфера деятельности)" = src.LineOfWork,
        "Сфера использования" = src.ScopeOfUse
WHEN NOT MATCHED
    THEN INSERT("Регион", "Название", "ИНН", "ОКВЭД", "Род занятий(сфера деятельности)", "Сфера использования", ts)
    VALUES(src.BuyersRegion, src.FinalBuyer, src.BuyerINN, src.okved, src.LineOfWork, src.ScopeOfUse, now()); 