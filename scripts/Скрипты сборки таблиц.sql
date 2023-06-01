SELECT
	SUM(r.Наличие),
	SUM(r.Оборот)
--	,*
FROM sttgaz.dds_isc_realization AS r 
LEFT JOIN sttgaz.dds_isc_counteragent AS c
	ON r."Контрагент ID"  = c.id 
LEFT JOIN sttgaz.dds_isc_counteragent AS rec
	ON r."Получатель ID"  = rec.id 
LEFT JOIN sttgaz.dds_isc_dealer_unit AS du
	ON r."Площадка дилера ID" = du.id 
LEFT JOIN sttgaz.dds_isc_product AS p
	ON r."Продукт ID" = p.id 
LEFT JOIN sttgaz.dds_isc_DirectionOfImplementationWithUKP AS d
	ON r."Направление реализации с учетом УКП ID" =d.id;


SELECT
	SUM(sir.Availability),
	SUM(sir.Turnover)
FROM sttgaz.stage_isc_realization sir 
------------------------------------------------



SELECT COUNT(*), SUM(s.SoldAtRetail), SUM(s.BalanceAtBeginningOfPeriodOnRoad)
FROM sttgaz.stage_isc_sales s;

SELECT COUNT(*), SUM(s."Продано в розницу"), SUM(s."Остатки на НП в пути")
FROM sttgaz.dds_isc_dealer_sales AS s
LEFT JOIN sttgaz.dds_isc_dealer_unit AS d
	ON s."Площадка дилера ID" = d.id 
LEFT JOIN sttgaz.dds_isc_buyer AS b
	ON s."Конечный клиент ID" = b.id 
LEFT JOIN sttgaz.dds_isc_product AS p 
	ON s."Продукт ID" = p.id 
LEFT JOIN sttgaz.dds_isc_DirectionOfImplementationWithUKP dir 
	ON s."Направление реализации с учетом УКП ID" = dir.id 





