BEGIN TRANSACTION;

DROP VIEW IF EXISTS sttgaz.dm_realization_with_kit_v;
CREATE OR REPLACE VIEW sttgaz.dm_realization_with_kit_v AS
SELECT
	*,
	CASE
		WHEN 
			r."Месяц" IN ('2022-07-01', '2022-10-01', '2022-12-01', '2023-02-01', '2023-03-01')
			AND r."Дивизион" = 'LCV'
			AND r."Направление реализации с учетом УКП" = 'СНГ-Казахстан'
		THEN 'Автокомплекты'
		ELSE 'Собранные ТС'
	END 																					AS "Тип продукции" 
FROM sttgaz.dm_isc_realization_v r
UNION ALL
SELECT
	*,
	'Автокомплекты' 																		AS "Тип продукции" 
FROM sttgaz.dm_erp_kit_sales_with_classifier_v k;
	
GRANT SELECT ON TABLE sttgaz.dm_realization_with_kit_v TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_realization_with_kit_v IS 'Реализация ТС и автокомплектов. Витрина данных с посчитанными метриками и классификатором.';

COMMIT TRANSACTION;	