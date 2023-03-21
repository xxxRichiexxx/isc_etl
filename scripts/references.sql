INSERT INTO sttgaz.dds_isc_classifier
("Сцеп","ВнК_ID","Внутренний код","Опции","Двигатель",
"Гр1 Дивизион","Гр2 Кузов","Гр3 Поколение","Гр4 Семейство",
"Гр5 Назначение","Гр6 Подр (места)","Гр7 Размер","Гр8 Привод",
"Гр9 Двигатель","Гр10 Опции","Гр11 группа АЕБ")
SELECT DISTINCT *
FROM sttgaz.stage_isc_classifier
WHERE "Options" IS NULL

CREATE OR REPLACE VIEW sttgaz.dm_isc_classifier AS
SELECT  *
FROM sttgaz.dds_isc_classifier

GRANT SELECT ON TABLE sttgaz.dm_isc_classifier TO PowerBI_Integration WITH GRANT OPTION;



CREATE OR REPLACE VIEW sttgaz.dm_isc_dealer AS
SELECT  *
FROM sttgaz.dds_isc_dealer;

GRANT SELECT ON TABLE sttgaz.dm_isc_dealer TO PowerBI_Integration WITH GRANT OPTION;