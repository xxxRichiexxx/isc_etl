MERGE
into sttgaz.dds_isc_dealer AS tgt 
USING
    (SELECT
        "division",
        "SalesTerritory",
        "Recipient",
        "RecipientFullName"
    FROM sttgaz.stage_isc_sales
    WHERE load_date = {{execution_date.date()}}) AS src
ON  
    tgt."Дивизион" = src.division,
    tgt."Территория продаж" = src.SalesTerritory,
    tgt."Название" = src.Recipient,
    tgt."Полное название (организация)" = src.RecipientFullName
WHEN MATCHED
    THEN UPDATE SET
        "Дивизион" = src.division,
        "Территория продаж" = src.SalesTerritory,
        "Название" = src.Recipient,
        "Полное название (организация)" = src.RecipientFullName
WHEN NOT MATCHED
    THEN INSERT("Дивизион", "Территория продаж", "Название", "Полное название (организация)", ts)
    VALUES(src.division, src.SalesTerritory, src.Recipient, src.RecipientFullName, now()); 