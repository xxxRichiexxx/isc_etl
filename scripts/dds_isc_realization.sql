SELECT DROP_PARTITIONS(
    'sttgaz.dds_isc_realization',
    '{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}',
    '{{execution_date.date().replace(day=1)}}'
);

INSERT INTO sttgaz.dds_isc_realization
   ("Контрагент ID",  ---
	"Документ", ----
	"Продукт ID", ---
	"Вид оплаты", ---
	"Дата приложения",---
	"Дата разнарядки", ---
	"День документа", ---
	"Договор", ---
	"Месяц документа", ---
	"Месяц планирования", ---
	"Направление реализации", ---
	"Направление реализации с учетом УКП ID", ---
	"Номер приложения", ---
	"Номер разнярядки", ---
	"Фирма", ---
	"Продавец", ---
	"Склад", ---
	"Заявка номер", ---
	"Заявка разнарядка",---
	"Заявка ресурс", ---
	"Холдинг конечного клиента",---
	"Наличие", ---
	"Оборот",---
	"НДС Расхода",---
	"Оборот без НДС",--
	"Цена",---
	"Сумма возмещения",---
	"НДС возмещения",---
	"Сумма возмещения без НДС",---
	"Сумма МО",---
	"НДС МО",--
	"Сумма МО Общ",---
	"Документ ISC ID",---
	"Период")
WITH 
sq AS(
	SELECT *
	FROM sttgaz.stage_isc_realization AS r
	WHERE DATE_TRUNC('MONTH', load_date) IN(
		'{{execution_date.date().replace(day=1)}}',
		'{{(execution_date.date().replace(day=1) - params.delta_1).replace(day=1)}}'
	)
)
SELECT
	c.id,---
	Doc,---
	p.id,---
	PaymentType,---
	AttachmentDate,---
	DischargeDate,---
	"Day",---
	Contract,---
	"Month",----
	PlaneMonth,---
	DirectionOfImplementation,---
	d.id,----
	AttachmentNumber,---
	DischargeNumber,---
	Company,---
	Seller,---
	Warehouse,---
	RequestNumber,---
	RequestDischarge,---
	RequestResource,---
	ClientHolding,---
	Availability,---
	Turnover,---
	ExpenseVAT,--
	TurnoverWithoutVAT,--
	Price,---
	RefundAmount,---
	RefundsVAT,--
	RefundWithoutVAT,---
	SumMO,----
	MOVAT,---
	SumMOTotal,---
	DocID,---
	load_date	---
FROM sq 
LEFT JOIN sttgaz.dds_isc_counteragent AS c
	ON HASH(sq.Client, sq.Recipient, sq.DealersUnitID, sq.DealersUnit, sq.Division)
		= HASH(c.Клиент, c.Получатель, c."Площадка дилера ISK ID", c."Площадка дилера", c."Дивизион")
LEFT JOIN sttgaz.dds_isc_product AS p
	ON HASH(sq.vin, sq.ProductIdentifier) = HASH(p.ВИН, p."ИД номерного товара");
LEFT JOIN sttgaz.dds_isc_DirectionOfImplementationWithUKP AS d
	ON sq.DirectionOfImplementationWithUKP = d."Направление реализации с учетом УКП";
