 DROP VIEW IF EXISTS sttgaz.dm_isc_orders_v;
 
 CREATE VIEW sttgaz.dm_isc_orders_v AS
 SELECT *
 FROM sttgaz.dds_isc_orders AS o
 LEFT JOIN sttgaz.dds_isc_counteragent AS c
 	ON o."Покупатель ID" = c.id;