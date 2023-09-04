DROP TABLE IF EXISTS sttgaz.dds_isc_nomenclature_guide;

CREATE TABLE sttgaz.dds_isc_nomenclature_guide (
    Ид int,
    Наименование varchar(500),
    НомерСогласноКД varchar(500),
    Код65 varchar(500),
    "Модель на заводе" varchar(500),
    Статус varchar(500),
    Дивизион varchar(500),
    Производитель varchar(500),
    ИдВидаТовара int,
    НаименованиеВидаТовара varchar(500),
    АвтосборочныйКомплект varchar(500),
    ИдКИСУ int,
    ИдАдабас int,
    ИдВидаПродукции int,
    НаименованиеВидаПродукции varchar(500),
    load_date date  
);

INSERT INTO sttgaz.dds_isc_nomenclature_guide
SELECT 
    ID,
    Name,
    CDNuber,
    Code65,
    REGEXP_REPLACE(REGEXP_REPLACE(ModelNaZavode, '^А', 'A'), '^С', 'C'),
    Status,
    Division,
    Proizvoditel,
    VidTovaraID,
    VidTovara,
    AutoKit,
    KisuID,
    AdabasID,
    VidProductaID,
    VidProducta,
    load_date
FROM sttgaz.stage_isc_nomenclature_guide n
WHERE n.load_date = DATE_TRUNC('MONTH', NOW())::date;