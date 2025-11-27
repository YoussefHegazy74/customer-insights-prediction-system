-- 4- DimServices
USE ChurnDWH;

CREATE TABLE DimServices (
    ServiceKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID NVARCHAR(255) NOT NULL,
    InternetService NVARCHAR(50),
    OnlineSecurity BIT,
    OnlineBackup BIT,
    DeviceProtection BIT,
    PhoneService BIT,
    MultipleLines BIT,
    TechSupport BIT,
    StreamingTV BIT,
    StreamingMovies BIT,
    StartDate DATE NOT NULL DEFAULT GETDATE(),
    EndDate DATE NULL,
    IsCurrent BIT NOT NULL DEFAULT 1
);

-- Query for inserting Data in ssis
SELECT 
    cs.CustomerID,
    cs.InternetService,
    cs.OnlineSecurity,
    cs.OnlineBackup,
    cs.DeviceProtection,
    cs.PhoneService,
    cs.MultipleLines,
    cs.TechSupport,
    es.StreamingTV,
    es.StreamingMovies,
    GETDATE() AS StartDate,
    NULL AS EndDate,
    1 AS IsCurrent
FROM CustomerChurnPrediction.dbo.CustomerServices cs
JOIN CustomerChurnPrediction.dbo.EntertainmentServices es 
    ON cs.CustomerID = es.CustomerID;