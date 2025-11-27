-- 3- DimPaymentMethod
USE ChurnDWH;

CREATE TABLE DimPaymentMethod (
    PaymentMethodKey INT IDENTITY(1,1) PRIMARY KEY,
    PaymentMethodID INT,
    PaymentMethodName NVARCHAR(100),
    StartDate DATE NOT NULL DEFAULT GETDATE(),
    EndDate DATE NULL,
    IsCurrent BIT DEFAULT 1
);
CREATE INDEX IX_DimPayment_Business_Current ON dbo.DimPaymentMethod(PaymentMethodID) WHERE IsCurrent = 1;

-- Query for inserting Data in ssis
SELECT 
    PaymentMethodID,
    PaymentMethodName,
    GETDATE() AS StartDate,
    NULL AS EndDate,
    1 AS IsCurrent
FROM CustomerChurnPrediction.dbo.PaymentMethods;