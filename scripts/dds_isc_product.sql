INSERT INTO sttgaz.dds_isc_product 
("Вариант сборки", "Вариант сборки свернутый", "Вид товара по дивизиону", "ВИН", "Двигатель по прайсу",
"ИД номерного товара", "Производитель ID", "Товар", "ТоварКод65", "Номерной товар", "Цвет", "Номерной товар ИД", "Классификатор дивизион тип кабины",
"Классификатор привод", "Классификатор подробно по дивизионам 22", "Классификатор вид товара", "Классификатор ГБО",
"Классификатор число посадочных мест", "Классификатор экологический класс", "ts")
WITH 
sq1 AS(
	SELECT *
	FROM sttgaz.stage_isc_realization AS r
	WHERE DATE_TRUNC('MONTH', load_date) IN(
		'{{execution_date.date().replace(day=1)}}',
		'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
	)
),
sq2 AS(
	SELECT DISTINCT "ВИН"
	FROM sttgaz.dds_isc_product 
)
SELECT DISTINCT
	BuildOption 						AS "Вариант сборки",
	BuildOptionСollapsed 				AS "Вариант сборки свернутый",
	PproductTypeByDivision 				AS "Вид товара по дивизиону",
	vin 								AS "ВИН",
	Engine 								AS "Двигатель по прайсу",
	ProductIdentifier 					AS "ИД номерного товара",
	m.id 								AS "Производитель ID",
	Product 							AS "Товар",
	ProductCode65 						AS "ТоварКод65",
	ProductNumber 						AS "Номерной товар",
	Color 								AS "Цвет",
	ProductIdentifier2 					AS "Номерной товар ИД",
	ClassifierCabType 					AS "Классификатор дивизион тип кабины",
	ClassifierDrive 					AS "Классификатор привод",
	ClassifierDetailedByDivision 		AS "Классификатор подробно по дивизионам 22",
	ClassifierProductType 				AS "Классификатор вид товара",
	ClassifierGBO 						AS "Классификатор ГБО",
	ClassifierNumberOfSeats 			AS "Классификатор число посадочных мест",
	ClassifierEcologicalClass 			AS "Классификатор экологический класс",
	NOW()
FROM sq1 								AS r
LEFT JOIN sttgaz.dds_isc_manufacturer 	AS m
	ON r.Manufacturer = m.Производитель
WHERE r.vin NOT IN (SELECT * FROM sq2); 
