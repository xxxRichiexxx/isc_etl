DROP VIEW IF EXISTS sttgaz.dm_isc_classifier_v;
CREATE OR REPLACE VIEW sttgaz.dm_isc_classifier_v AS
SELECT
	p.id 											AS property_id,
	p.kind 											AS property_kind,
	p.name  										AS property_name,
	v.property_value_name							AS property_value_name_1,
	m.property_value_name							AS property_value_name_2,
	m.product_id,
	m.product_id_KISU,
	m.product_name
FROM sttgaz.stage_isc_gaz_property_binding_guide 	AS m
LEFT JOIN sttgaz.stage_isc_properties_guide 		AS p
	ON m.property_id = p.id 
LEFT JOIN sttgaz.stage_isc_property_value_guide 	AS v 
	ON m.property_value_id =v.property_value_id;