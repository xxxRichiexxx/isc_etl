INSERT INTO sttgaz.dds_isc_product
("Вариант сборки", "Вариант сборки свернутый", "ВИН", "Товар", "Номерной товар ИД", "Двигатель по прайсу", "Дивизион ID", "ts")
WITH
sq AS(
	SELECT DISTINCT HASH("ВИН", "ИД номерного товара", "Вариант сборки", "Вариант сборки свернутый")
	FROM sttgaz.dds_isc_product
)
SELECT DISTINCT
	BuildOption,
	BuildOptionСollapsed,
	vin,
	code,  -- "Это внутренний код", Товар
	ProductIdentifier,   ---"Номерной товар ИД"
	Engine,
	d.id,
	NOW()	
FROM sttgaz.stage_isc_sales s
LEFT JOIN sttgaz.dds_isc_division 		AS d 
	ON s.division = d."Наименование"
WHERE DATE_TRUNC('MONTH', load_date) IN(
		'{{execution_date.date().replace(day=1)}}',
		'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
	)
	AND HASH(vin, ProductIdentifier, BuildOption, BuildOptionСollapsed) NOT IN (SELECT * FROM sq);