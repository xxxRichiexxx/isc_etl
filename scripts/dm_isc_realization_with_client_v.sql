DROP VIEW IF EXISTS sttgaz.dm_isc_realization_with_client_v;
CREATE OR REPLACE VIEW sttgaz.dm_isc_realization_with_client_v AS
	SELECT
		"День документа"::date		                    AS "День",
        c.Наименование                                  AS "Контрагент",
        rec.Наименование                                AS "Получатель",
        du.Наименование_дилера                          AS "Дилер",
		d."Направление реализации с учетом УКП",
		div.Наименование								AS "Дивизион",
		m.Наименование									AS "Производитель",
		"Классификатор подробно по дивизионам 22",
		"Товар",
		"ТоварКод65",
		"Вариант сборки",
		SUM("Наличие")									AS "Реализовано"
	FROM sttgaz.dds_isc_realization 		AS r 
	LEFT JOIN sttgaz.dds_isc_counteragent 	AS c
		ON r."Контрагент ID"  = c.id 
	LEFT JOIN sttgaz.dds_isc_counteragent 	AS rec
		ON r."Получатель ID"  = rec.id 
	LEFT JOIN sttgaz.dds_isc_dealer_unit 	AS du
		ON r."Площадка дилера ID" = du.id 
	LEFT JOIN sttgaz.dds_isc_product 		AS p
		ON r."Продукт ID" = p.id 
	LEFT JOIN sttgaz.dds_isc_DirectionOfImplementationWithUKP AS d
		ON r."Направление реализации с учетом УКП ID" = d.id
	LEFT JOIN sttgaz.dds_isc_manufacturer 	AS m 
		ON p."Производитель ID"  = m.id
	LEFT JOIN sttgaz.dds_isc_division  		AS div
		ON p."Дивизион ID"  = div.id
	GROUP BY
		"День документа"::date,
        c.Наименование,
        rec.Наименование,
        du.Наименование_дилера,
		d."Направление реализации с учетом УКП",
		div.Наименование,
		m.Наименование,
		"Классификатор подробно по дивизионам 22",
		"Товар",
		"ТоварКод65",
		"Вариант сборки"
	ORDER BY "День";

GRANT SELECT ON TABLE sttgaz.dm_isc_realization_with_client_v TO PowerBI_Integration WITH GRANT OPTION;
COMMENT ON VIEW sttgaz.dm_isc_realization_with_client_v IS 'Реализация ТС. Витрина данных с посчитанными метриками.';