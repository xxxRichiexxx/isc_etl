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




INSERT INTO sttgaz.dds_isc_classifier_2
("Внутренний код", "Значение", "Вид товара", "Вид продукции")
SELECT DISTINCT *
FROM sttgaz.stage_isc_classifier_2;

CREATE OR REPLACE VIEW sttgaz.dm_isc_classifier_2 AS
SELECT  *
FROM sttgaz.dds_isc_classifier_2;

GRANT SELECT ON TABLE sttgaz.dm_isc_classifier_2 TO PowerBI_Integration WITH GRANT OPTION;



CREATE OR REPLACE VIEW sttgaz.dm_isc_classifier_2_for_contractors AS
SELECT
	 id,
    "Значение",
    "Вид товара",
    "Вид продукции"
FROM sttgaz.dds_isc_classifier_2;

GRANT SELECT ON TABLE sttgaz.dm_isc_classifier_2_for_contractors TO PowerBI_Integration WITH GRANT OPTION;