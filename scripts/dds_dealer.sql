DROP TABLE IF EXISTS sttgaz.dds_isc_dealer;
CREATE TABLE sttgaz.dds_isc_dealer (
    id AUTO_INCREMENT PRIMARY KEY,                         
    "Дивизион" VARCHAR(50),
    "Территория продаж" VARCHAR(2000),
    "Название" VARCHAR(2000),
    "Полное название (организация)" VARCHAR(2000)
)
ORDER BY id;

INSERT INTO sttgaz.dds_isc_dealer
("Дивизион", "Территория продаж", "Название", "Полное название (организация)")
SELECT DISTINCT
        "division",
        "SalesTerritory",
        "Recipient",
        "RecipientFullName"
FROM sttgaz.stage_isc_sales; 