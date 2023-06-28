INSERT INTO sttgaz.dds_isc_counteragent
	("Наименование")
WITH 
sq1 AS(
	SELECT DISTINCT HASH(
		"Наименование"
	)
	FROM sttgaz.dds_isc_counteragent
),
sq2 AS(
	SELECT DISTINCT
		"Buyer"
	FROM sttgaz.stage_isc_orders AS o
    WHERE load_date >= '{{(((execution_date.date().replace(day=1) - dt.timedelta(days=1)).replace(day=1) - dt.timedelta(days=1)).replace(day=1) - dt.timedelta(days=1)).replace(day=1)}}',
        AND load_date <= '{{execution_date.date().replace(day=1)}}';
)
SELECT DISTINCT
	"Buyer"
FROM sq2
WHERE HASH("Buyer") NOT IN (SELECT * FROM sq1);