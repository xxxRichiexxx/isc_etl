SELECT DROP_PARTITIONS(
    'sttgaz.dds_isc_orders',
    '{{(((execution_date.date().replace(day=1) - params.delta_1).replace(day=1) - params.delta_1).replace(day=1) - params.delta_1).replace(day=1)}}',
    '{{execution_date.date().replace(day=1)}}'
);

INSERT INTO sttgaz.dds_isc_orders 
    SELECT
        "ProductCode65",
        "Color",
        "BuildOption",
        "ModelYear",
        "AdditionalProps14",
        "IGC",
        "DirectionOfImplementation",
        c.id,
        "ShipmentStatus",
        "Status",
        "ContractPeriod",
        "ShipmentMonth",
        "ProductionMonth",
        "City",
        "Manufacturer",
        "quantity",
        "load_date"
    FROM sttgaz.stage_isc_orders AS o
    LEFT JOIN sttgaz.dds_isc_counteragent AS c
        ON o.Buyer = c.Наименование
    WHERE load_date >= '{{(((execution_date.date().replace(day=1) - params.delta_1).replace(day=1) - params.delta_1).replace(day=1) - params.delta_1).replace(day=1)}}',
        AND load_date <= '{{execution_date.date().replace(day=1)}}';


