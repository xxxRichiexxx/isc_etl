CREATE OR REPLACE VIEW sttgaz.dm_isc_dealer_sales_w_stoianka_v AS
SELECT
	DATE_TRUNC('MONTH', s."Дата продажи")::date			AS "Месяц",
	d."ИСК ID",
	d.Название,
	d."Полное название (организация)",
	d."Стоянка ID",
	d.Стоянка,
	d."Город стоянки",
	SUM(s."Продано в розницу")							AS "Продано в розницу",
	SUM(s."Продано физ лицам")							AS "Продано физ лицам"
FROM sttgaz.dds_isc_sales s
LEFT JOIN sttgaz.dds_isc_dealer d 
	ON s."Дилер ID" = d.id
GROUP BY 
	DATE_TRUNC('MONTH', s."Дата продажи"),
	d."ИСК ID",
	d.Название,
	d."Полное название (организация)",
	d."Стоянка ID",
	d.Стоянка,
	d."Город стоянки";