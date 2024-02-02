DROP VIEW IF EXISTS sttgaz.dm_isc_balance_with_stoianka;
CREATE OR REPLACE VIEW sttgaz.dm_isc_balance_with_stoianka AS
SELECT 
       s.Дивизион,
       d."Стоянка ID",
       d.Стоянка,
       SUM(s."Остатки на НП в пути")				AS "Остатки",
       (date_trunc('MONTH'::varchar(5), s.Период))::date	AS "Период"
FROM sttgaz.dds_isc_sales s 
LEFT JOIN sttgaz.dds_isc_dealer d 
       ON s."Дилер ID" = d.id
GROUP BY (date_trunc('MONTH'::varchar(5), s.Период))::date,
        s.Дивизион,
        d."Стоянка ID",
        d.Стоянка;

COMMENT ON VIEW sttgaz.dm_isc_balance_with_stoianka IS 'Остатки на складах дилеров. Витрина данных.';
GRANT SELECT ON TABLE sttgaz.dm_isc_balance_with_stoianka TO PowerBI_Integration WITH GRANT OPTION;