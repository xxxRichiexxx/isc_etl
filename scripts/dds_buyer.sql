DROP TABLE IF EXISTS sttgaz.dds_isc_buyer;
CREATE TABLE sttgaz.dds_isc_buyer (
    id AUTO_INCREMENT PRIMARY KEY,
    "Регион" VARCHAR(2000),
    "Название" VARCHAR(2000),
    "ИНН" VARCHAR(500),
    "ОКВЭД"  VARCHAR(500),
    "Род занятий(сфера деятельности)" VARCHAR(500),
    "Сфера использования" VARCHAR(2000)
)
ORDER BY id
PARTITION BY "Регион";

INSERT INTO sttgaz.dds_isc_buyer 
("Регион", "Название", "ИНН", "ОКВЭД", "Род занятий(сфера деятельности)", "Сфера использования")
SELECT DISTINCT
    "BuyersRegion",
    "FinalBuyer",
    "BuyerINN",
    "okved",
    "LineOfWork",
    "ScopeOfUse"
FROM sttgaz.stage_isc_sales; 