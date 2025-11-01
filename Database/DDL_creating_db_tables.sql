create database [CustomerChurnPrediction]
on(
name='CustomerChurnPredictiondbmdf',
filename='D:\depi final project\final project depi\CustomerChurnPrediction.mdf')
log on(
name='CustomerChurnPredictiondbldf',
filename='D:\depi final project\final project depi\CustomerChurnPrediction.ldf')

CREATE TABLE Customers (
                    CustomerID NVARCHAR(255) PRIMARY KEY,
                    Gender CHAR(1) NOT NULL CHECK (Gender IN ('M', 'F')),
                    IsSeniorCitizen BIT NOT NULL,
                    HasPartner BIT NOT NULL,
                    HasDependents BIT NOT NULL
                );

CREATE TABLE Contracts (
                    ContractID INT IDENTITY(1,1) PRIMARY KEY,
                    ContractType NVARCHAR(50) UNIQUE NOT NULL
                );

CREATE TABLE PaymentMethods (
                    PaymentMethodID INT IDENTITY(1,1) PRIMARY KEY,
                    PaymentMethodName NVARCHAR(100) UNIQUE NOT NULL
                );

CREATE TABLE Accounts (
                    CustomerID NVARCHAR(255) PRIMARY KEY,
                    Tenure INT NOT NULL,
                    ContractID INT,
                    PaymentMethodID INT,
                    PaperlessBilling BIT NOT NULL,
                    MonthlyCharges DECIMAL(10, 2) NOT NULL,
                    TotalCharges DECIMAL(10, 2),
                    Churn BIT NOT NULL,
                    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID) ON DELETE CASCADE,
                    FOREIGN KEY (ContractID) REFERENCES Contracts(ContractID),
                    FOREIGN KEY (PaymentMethodID) REFERENCES PaymentMethods(PaymentMethodID)
                );

CREATE TABLE CustomerServices (
                    CustomerID NVARCHAR(255) PRIMARY KEY,
                    PhoneService BIT NOT NULL,
                    MultipleLines BIT NOT NULL,
                    InternetService NVARCHAR(50) NOT NULL,
                    OnlineSecurity BIT NOT NULL,
                    OnlineBackup BIT NOT NULL,
                    DeviceProtection BIT NOT NULL,
                    TechSupport BIT NOT NULL,
                    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID) ON DELETE CASCADE
                );

CREATE TABLE EntertainmentServices (
                    CustomerID NVARCHAR(255) PRIMARY KEY,
                    StreamingTV BIT NOT NULL,
                    StreamingMovies BIT NOT NULL,
                    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID) ON DELETE CASCADE
                );