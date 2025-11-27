-- 6- FactCustomerChurn
USE ChurnDWH;

CREATE TABLE FactCustomerChurn (
    FactID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerKey INT,
    ServiceKey INT,
    ContractKey INT,
    TimeKey INT,
    PaymentMethodKey INT,
    TenureMonths INT,
    MonthlyCharges DECIMAL(10,2),
    TotalCharges DECIMAL(10,2),
    ChurnFlag BIT,

    FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
    FOREIGN KEY (ServiceKey) REFERENCES DimServices(ServiceKey),
    FOREIGN KEY (ContractKey) REFERENCES DimContract(ContractKey),
    FOREIGN KEY (PaymentMethodKey) REFERENCES DimPaymentMethod(PaymentMethodKey),
    FOREIGN KEY (TimeKey) REFERENCES DimTime(TimeKey)
);
CREATE INDEX IX_FactCustomerChurn_CustomerKey ON dbo.FactCustomerChurn(CustomerKey);
CREATE INDEX IX_FactCustomerChurn_TimeKey ON dbo.FactCustomerChurn(TimeKey);

-- Query for inserting Data in ssis 
SELECT 
    cdim.CustomerKey,
    sdim.ServiceKey,
    ctdim.ContractKey,
    CONVERT(INT, FORMAT(GETDATE(), 'yyyyMMdd')) AS TimeKey,
    pdim.PaymentMethodKey,
    a.Tenure AS TenureMonths,
    a.MonthlyCharges,
    a.TotalCharges,
    a.Churn AS ChurnFlag
FROM CustomerChurnPrediction.dbo.Accounts a
JOIN ChurnDWH.dbo.DimCustomer cdim 
    ON a.CustomerID = cdim.CustomerID AND cdim.IsCurrent = 1
JOIN ChurnDWH.dbo.DimServices sdim 
    ON sdim.CustomerID = a.CustomerID AND sdim.IsCurrent = 1
JOIN ChurnDWH.dbo.DimContract ctdim 
    ON ctdim.ContractID = a.ContractID AND ctdim.IsCurrent = 1
JOIN ChurnDWH.dbo.DimPaymentMethod pdim 
    ON pdim.PaymentMethodID = a.PaymentMethodID AND pdim.IsCurrent = 1;