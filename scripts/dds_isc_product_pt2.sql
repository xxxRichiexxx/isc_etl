INSERT INTO sttgaz.dds_isc_product
("Вариант сборки", "Вариант сборки свернутый", ВИН, Товар, "Номерной товар ИД", "Двигатель по прайсу")
WITH
sq1 AS(
	SELECT DISTINCT HASH(ВИН, "ИД номерного товара")
	FROM sttgaz.dds_isc_product dip 
)
SELECT DISTINCT
	BuildOption,
	BuildOptionСollapsed,
	vin,
	code,  -- "Это внутренний код", Товар
	ProductIdentifier,   ---"Номерной товар ИД"
	Engine	
FROM sttgaz.stage_isc_sales sis
WHERE DATE_TRUNC('MONTH', load_date) IN(
		'{{execution_date.date().replace(day=1)}}',
		'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
	)
	AND HASH(vin, ProductIdentifier) NOT IN (SELECT * FROM sq1);