INSERT INTO sttgaz.dds_isc_product 
("Вариант сборки", "Вариант сборки свернутый", "Вид товара по дивизиону", "ВИН", "Двигатель по прайсу", "Дивизион ID",
"ИД номерного товара", "Производитель ID", "Товар", "ТоварКод65", "Номерной товар", "Цвет", "Номерной товар ИД", "Классификатор дивизион тип кабины",
"Классификатор привод", "Классификатор подробно по дивизионам 22", "Классификатор вид товара", "Классификатор ГБО",
"Классификатор число посадочных мест", "Классификатор экологический класс")
SELECT DISTINCT
	BuildOption 					AS "Вариант сборки",
	BuildOptionСollapsed 			AS "Вариант сборки свернутый",
	PproductTypeByDivision 			AS "Вид товара по дивизиону",
	vin 							AS "ВИН",
	Engine 							AS "Двигатель по прайсу",
	d.id 							AS "Дивизион ID",
	ProductIdentifier 				AS "ИД номерного товара",
	m.id 							AS "Производитель ID",
	Product 						AS "Товар",
	ProductCode65 					AS "ТоварКод65",
	ProductNumber 					AS "Номерной товар",
	Color 							AS "Цвет",
	ProductIdentifier2 				AS "Номерной товар ИД",
	ClassifierCabType 				AS "Классификатор дивизион тип кабины",
	ClassifierDrive 				AS "Классификатор привод",
	ClassifierDetailedByDivision 	AS "Классификатор подробно по дивизионам 22",
	ClassifierProductType 			AS "Классификатор вид товара",
	ClassifierGBO 					AS "Классификатор ГБО",
	ClassifierNumberOfSeats 		AS "Классификатор число посадочных мест",
	ClassifierEcologicalClass 		AS "Классификатор экологический класс"
FROM sttgaz.stage_isc_realization 		AS r
LEFT JOIN sttgaz.dds_isc_division 		AS d 
	ON r.Division = d.Дивизион
LEFT JOIN sttgaz.dds_isc_manufacturer 	AS m
	ON r.Manufacturer = m.Производитель;
