INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH
	sq1 AS(
		SELECT 
			SUM(s1."Продано в розницу")
		FROM sttgaz.dm_isc_dealer_sales_w_stoianka_v s1
		WHERE s1.Месяц = '{{execution_date.date().replace(day=1)}}'	
	),
	sq2 AS(
		SELECT 
			SUM(s2."Розница ТП")
		FROM sttgaz.dm_isc_dealer_sales_RF s2
		WHERE s2.Месяц = '{{execution_date.date().replace(day=1)}}'
	)
SELECT
	'dm_isc_dealer_sales_w_stoianka'
	,'dm_isc_dealer_sales_w_stoianka and dm_isc_dealer_sales_RF compare: ' || (SELECT * FROM sq1) || '=' || (SELECT * FROM sq2)
    ,'{{execution_date}}'
	,(SELECT * FROM sq1) = (SELECT * FROM sq2);