INSERT INTO sttgaz.dds_isc_division
("Дивизион")
SELECT DISTINCT Division 
FROM sttgaz.stage_isc_realization r;