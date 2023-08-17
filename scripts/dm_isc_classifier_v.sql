DROP VIEW IF EXISTS sttgaz.dm_isc_classifier_v;
CREATE OR REPLACE VIEW sttgaz.dm_isc_classifier_v AS
 SELECT p.id AS property_id,
        p.kind AS property_kind,
        p.name AS property_name,
        v.property_value_name AS property_value_name_1,
        m.property_value_name AS property_value_name_2,
        m.product_id,
        m.product_id_KISU,
        m.product_name
 FROM sttgaz.stage_isc_gaz_property_binding_guide m 
 LEFT  JOIN sttgaz.stage_isc_properties_guide p 
	ON m.property_id = p.id AND p.load_date = DATE_TRUNC('MONTH', NOW())
 LEFT  JOIN sttgaz.stage_isc_property_value_guide v 
	ON m.property_value_id = v.property_value_id AND v.load_date = DATE_TRUNC('MONTH', NOW())
 WHERE m.load_date = DATE_TRUNC('MONTH', NOW());

GRANT SELECT ON TABLE sttgaz.dm_isc_classifier_v TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_isc_classifier_v IS 'Продажи (LCV, MCV) дилеров по РФ по дням. Витрина данных для мат модели NOVO BI.'