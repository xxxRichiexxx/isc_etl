DROP TABLE IF EXISTS sttgaz.stage_isc_sales;

CREATE TABLE sttgaz.stage_isc_sales (
    "ModelYear" INT,
    "vin" VARCHAR(500),
    "division" VARCHAR(50),
    "code" VARCHAR(500),
    "SalesTerritory" VARCHAR(2000),
    "Recipient" VARCHAR(2000),
    "RecipientFullName" VARCHAR(2000),
    "BuyersRegion" VARCHAR(2000),
    "FinalBuyer" VARCHAR(2000),
    "BuyerINN" VARCHAR(500),
    "okved"  VARCHAR(500),
    "LineOfWork" VARCHAR(500),
    "ScopeOfUse" VARCHAR(2000),
    "ImplementationProgram" VARCHAR(2000),
    "ShipmentDate" DATE,
    "DateOfSale" DATE,
    "DateOfEntryIntoDB" VARCHAR(500),
    "SoldAtRetail" INT,
    "SoldToIndividuals" INT,
    "BalanceAtBeginningOfPeriod" INT,
    "BalanceAtEndOfPeriod" INT,
    "ProductIdentifier" INT,
    load_date DATE
);