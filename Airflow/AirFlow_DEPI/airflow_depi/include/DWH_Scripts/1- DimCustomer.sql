CREATE DATABASE ChurnDWH;

USE ChurnDWH;

-- 1. DimCustomer
CREATE TABLE DimCustomer (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID NVARCHAR(255) NOT NULL,       -- Business Key from source
    Gender CHAR(1),
    IsSeniorCitizen BIT,
    HasPartner BIT,
    HasDependents BIT,
    StartDate DATE NOT NULL DEFAULT GETDATE(),
    EndDate DATE NULL,
    IsCurrent BIT NOT NULL DEFAULT 1
);
CREATE INDEX IX_DimCustomer_Business_Current ON dbo.DimCustomer(CustomerID) WHERE IsCurrent = 1;

-- Query for inserting Data in ssis
SELECT 
    c.CustomerID,
    c.Gender,
    c.IsSeniorCitizen,
    c.HasPartner,
    c.HasDependents,
    GETDATE() AS StartDate,
    NULL AS EndDate,
    1 AS IsCurrent
FROM CustomerChurnPrediction.dbo.Customers c;