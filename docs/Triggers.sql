CREATE TABLE CustomerAudit (
    AuditID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID NVARCHAR(255) NOT NULL,
    Gender CHAR(1),
    IsSeniorCitizen BIT,
    HasPartner BIT,
    HasDependents BIT,
    Action NVARCHAR(10) NOT NULL,
    ChangedAt DATETIME NOT NULL DEFAULT GETDATE()
);
go

-- if you update a customer's record, the audit table saves what that record looked like before your update. 
-- If you delete a customer, the audit table keeps a permanent copy of that deleted record.
CREATE TRIGGER trg_CustomerAudit
ON Customers
AFTER UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO CustomerAudit (
        CustomerID,
        Gender,
        IsSeniorCitizen,
        HasPartner,
        HasDependents,
        Action
    )
    SELECT
        d.CustomerID,
        d.Gender,
        d.IsSeniorCitizen,
        d.HasPartner,
        d.HasDependents,
        CASE
            WHEN EXISTS (SELECT * FROM inserted) THEN 'UPDATE'
            ELSE 'DELETE'
        END
    FROM
        deleted d;
END;
go

CREATE TRIGGER trg_ValidateCharges
ON Accounts
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- checks that TotalCharges less than MonthlyCharges AND Tenure more than 0
    IF EXISTS (
        SELECT 1
        FROM inserted
        WHERE TotalCharges < MonthlyCharges AND Tenure > 0
    )
    BEGIN
        RAISERROR ('Validation failed: TotalCharges cannot be less than MonthlyCharges for an active account.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END

    -- checks that monthly charges must be positive
    IF EXISTS (
        SELECT 1
        FROM inserted
        WHERE MonthlyCharges < 0
    )
    BEGIN
        RAISERROR ('Validation failed: MonthlyCharges cannot be negative.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
END;
GO

PRINT 'Created new validation trigger: trg_ValidateCharges';
GO