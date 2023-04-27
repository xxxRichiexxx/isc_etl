INSERT INTO sttgaz.dds_isc_DirectionOfImplementationWithUKP
("Направление реализации с учетом УКП")
SELECT DISTINCT DirectionOfImplementationWithUKP 
FROM sttgaz.stage_isc_realization r;