DROP VIEW IF EXISTS sttgaz.dm_realization_with_kit_v;
CREATE OR REPLACE VIEW sttgaz.dm_realization_with_kit_v AS
SELECT
	*,
	'Собранные ТС' AS "Тип продукции" 
FROM sttgaz.dm_isc_realization_v r
UNION
SELECT
	*,
	'Автокомплекты' AS "Тип продукции" 
FROM sttgaz.dm_erp_kit_sales_with_classifier_v k;
	
GRANT SELECT ON TABLE sttgaz.dm_erp_kit_sales_with_classifier_v TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_erp_kit_sales_with_classifier_v IS 'Реализация ТС и автокомплектов. Витрина данных с посчитанными метриками и классификатором.';	