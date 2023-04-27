INSERT INTO sttgaz.dds_isc_manufacturer
("Производитель")
SELECT DISTINCT Manufacturer 
FROM sttgaz.stage_isc_realization r
WHERE Manufacturer IS NOT NULL;