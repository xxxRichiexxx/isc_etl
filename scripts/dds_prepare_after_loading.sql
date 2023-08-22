

	
BEGIN TRANSACTION;	

---Удалить из витрин с реализацией  в январе 2022 и январе 2023 года автомобили с одновременным выполнением следующих условий: 
---Вариант сборки (BuildOption) – содержит русскую букву «З», Направление реализации – начинается с «РФ», Клиент (Дилер) – содержит «Авторитэйл». 
---Должно удалиться 40 ам в янв.22 и 126 ам в янв.23.	

DELETE FROM sttgaz.dds_isc_realization 
WHERE 
	DATE_TRUNC('month', Период)::date IN ('2022-01-01', '2023-01-01')
	AND "Продукт ID"  IN (SELECT id 
							FROM sttgaz.dds_isc_product AS p
							WHERE p."Вариант сборки" iLIKE '%/З')
	AND "Направление реализации с учетом УКП ID"  IN (SELECT id 
													    FROM sttgaz.dds_isc_DirectionOfImplementationWithUKP AS d
													    WHERE d."Направление реализации с учетом УКП" iLIKE 'РФ-%')
	AND "Контрагент ID"  IN (SELECT id 
							   FROM sttgaz.dds_isc_counteragent  AS c
							   WHERE c.Наименование iLIKE '%Авторитэйл%');

---Удалить из витрин по реализации в январе 2022 года (перенести эти объемы в сентябрь 2021 года) автобусы ПАЗ со следующим условием:
---Клиент – МИНПРОМТОРГ. Таких будет 40 штук.

UPDATE sttgaz.dds_isc_realization 
SET Период = '2021-09-01'
WHERE 
	Период = '2022-01-01'
	AND "Контрагент ID" = (SELECT id
							 FROM sttgaz.dds_isc_counteragent AS c 
							 WHERE c.Наименование ILIKE '%МИНПРОМТОРГ%')
	AND "Продукт ID" IN (SELECT p.id 
						   FROM sttgaz.dds_isc_product AS p
						   LEFT JOIN sttgaz.dds_isc_division AS d
								ON p."Дивизион ID" = d.id
						   WHERE d.Наименование iLIKE '%BUS%');
		
COMMIT TRANSACTION;	

---Проверки----

---нужный результат NULL----
SELECT SUM(r.Наличие)
FROM sttgaz.dds_isc_realization AS r
WHERE 
	DATE_TRUNC('month', r.Период)::date IN ('2022-01-01', '2023-01-01')
	AND r."Продукт ID"  IN (SELECT id 
							FROM sttgaz.dds_isc_product AS p
							WHERE p."Вариант сборки" iLIKE '%/З')
	AND r."Направление реализации с учетом УКП ID"  IN (SELECT id 
													   FROM sttgaz.dds_isc_DirectionOfImplementationWithUKP AS d
													   WHERE d."Направление реализации с учетом УКП" iLIKE 'РФ-%')
	AND r."Контрагент ID"  IN (SELECT id 
							   FROM sttgaz.dds_isc_counteragent  AS c
							   WHERE c.Наименование iLIKE '%Авторитэйл%');


---Нужный результат 40------
SELECT SUM(dir.Наличие)
FROM sttgaz.dds_isc_realization dir 
WHERE 
	Период = '2021-09-01'
	AND "Контрагент ID" = (SELECT id
							 FROM sttgaz.dds_isc_counteragent AS c 
							 WHERE c.Наименование ILIKE '%МИНПРОМТОРГ%')
	AND "Продукт ID" IN (SELECT p.id 
						   FROM sttgaz.dds_isc_product AS p
						   LEFT JOIN sttgaz.dds_isc_division AS d
								ON p."Дивизион ID" = d.id
						   WHERE d.Наименование iLIKE '%BUS%');