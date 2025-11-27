-- 2- DimContract
USE ChurnDWH;

CREATE TABLE DimContract (
    ContractKey INT IDENTITY(1,1) PRIMARY KEY,
    ContractID INT,
    ContractType NVARCHAR(50),
    PaperlessBilling BIT,
    StartDate DATE NOT NULL DEFAULT GETDATE(),
    EndDate DATE NULL,
    IsCurrent BIT DEFAULT 1
);
CREATE INDEX IX_DimContract_Business_Current ON dbo.DimContract(ContractID) WHERE IsCurrent = 1;

-- Query for inserting Data in ssis
SELECT DISTINCT
    c.ContractID,
    c.ContractType,
    a.PaperlessBilling,
    GETDATE() AS StartDate,
    NULL AS EndDate,
    1 AS IsCurrent
FROM CustomerChurnPrediction.dbo.Contracts c
JOIN CustomerChurnPrediction.dbo.Accounts a
    ON c.ContractID = a.ContractID;