BEGIN TRANSACTION;

TRUNCATE TABLE sttgaz.dds_isc_orders;

INSERT INTO sttgaz.dds_isc_orders 
    SELECT
        "ProductCode65",
        "Color",
        "BuildOption",
        "ModelYear",
        "PriznakRezervirovania",
        "IGC",
        "DirectionOfImplementation",
        c.id,
        "ShipmentStatus",
        "Status",
        "ContractPeriod",
        "ShipmentMonth",
        "ShipmentDate",
        "ProductionMonth",
        "City",
        "Manufacturer",
        "ProductType",
        "Contract",
        "ShippingWarehouse",
        "PrognozDataVidachiOR",
        "TypePerehodaPS",       
        "quantity",
        "load_date"
    FROM sttgaz.stage_isc_orders AS o
    LEFT JOIN sttgaz.dds_isc_counteragent AS c
        ON o.Buyer = c.Наименование;

COMMIT TRANSACTION;


