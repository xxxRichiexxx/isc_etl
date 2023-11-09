INSERT INTO sttgaz.stage_checks (table_name, check_name, ts, check_result)
WITH
	sq1 AS(
		SELECT 
			SUM(s1."Продано в розницу") = AVG(s1."Общие продажи в субъекте")::int
		FROM sttgaz.dm_isc_dealer_sales_w_stoianka_v s1
		WHERE s1.Месяц = '{{execution_date.date().replace(day=1)}}'
			AND "Территория продаж" = 'Московская область'
			AND s1.Дивизион = 'LCV+MCV'
	)
SELECT
	'dm_isc_dealer_sales_w_stoianka'
	,'dm_isc_dealer_sales_w_stoianka Общие продажи в субъекте check '
    ,'{{execution_date}}'
	,(SELECT * FROM sq1);