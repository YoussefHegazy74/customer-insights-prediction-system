"""
Script to load data from CustomerChurnPrediction database to ChurnDWH Data Warehouse
"""
from sqlalchemy import create_engine, text
import urllib.parse

def load_dwh_data():
    """Load data from source database to DWH"""
    
    username = 'airflow_etl'
    password = 'AirflowETL@2024!'
    server = 'host.docker.internal'
    port = '1433'
    driver = 'ODBC Driver 17 for SQL Server'
    
    # Connection to source database
    params_source = urllib.parse.quote_plus(
        f'DRIVER={driver};'
        f'SERVER={server},{port};'
        f'DATABASE=CustomerChurnPrediction;'
        f'UID={username};PWD={password};'
        f'TrustServerCertificate=yes'
    )
    source_connection_string = f'mssql+pyodbc:///?odbc_connect={params_source}'
    source_engine = create_engine(source_connection_string)
    
    # Connection to DWH database
    params_dwh = urllib.parse.quote_plus(
        f'DRIVER={driver};'
        f'SERVER={server},{port};'
        f'DATABASE=ChurnDWH;'
        f'UID={username};PWD={password};'
        f'TrustServerCertificate=yes'
    )
    dwh_connection_string = f'mssql+pyodbc:///?odbc_connect={params_dwh}'
    dwh_engine = create_engine(dwh_connection_string)
    
    print("Loading data from CustomerChurnPrediction to ChurnDWH...")

    print("Loading FactCustomerChurn...")
    with dwh_engine.begin() as conn_dwh:
        conn_dwh.execute(text("DELETE FROM FactCustomerChurn"))
        conn_dwh.execute(text("""
            INSERT INTO ChurnDWH.dbo.FactCustomerChurn 
                (CustomerKey, ServiceKey, ContractKey, TimeKey, PaymentMethodKey, 
                 TenureMonths, MonthlyCharges, TotalCharges, ChurnFlag)
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
                ON pdim.PaymentMethodID = a.PaymentMethodID AND pdim.IsCurrent = 1
        """))
    print(f"FactCustomerChurn loaded successfully")
    
    
    # 1. Load DimCustomer
    print("Loading DimCustomer...")
    with dwh_engine.begin() as conn_dwh:
        # Clear existing data first
        conn_dwh.execute(text("DELETE FROM DimCustomer"))
        
        # Insert data
        conn_dwh.execute(text("""
            INSERT INTO ChurnDWH.dbo.DimCustomer (CustomerID, Gender, IsSeniorCitizen, HasPartner, HasDependents, StartDate, EndDate, IsCurrent)
            SELECT 
                c.CustomerID,
                c.Gender,
                c.IsSeniorCitizen,
                c.HasPartner,
                c.HasDependents,
                GETDATE() AS StartDate,
                NULL AS EndDate,
                1 AS IsCurrent
            FROM CustomerChurnPrediction.dbo.Customers c
        """))
    print(f"DimCustomer loaded successfully")
    
    # 2. Load DimContract
    print("Loading DimContract...")
    with dwh_engine.begin() as conn_dwh:
        conn_dwh.execute(text("DELETE FROM DimContract"))
        conn_dwh.execute(text("""
            INSERT INTO ChurnDWH.dbo.DimContract (ContractID, ContractType, PaperlessBilling, StartDate, EndDate, IsCurrent)
            SELECT DISTINCT
                c.ContractID,
                c.ContractType,
                a.PaperlessBilling,
                GETDATE() AS StartDate,
                NULL AS EndDate,
                1 AS IsCurrent
            FROM CustomerChurnPrediction.dbo.Contracts c
            JOIN CustomerChurnPrediction.dbo.Accounts a
                ON c.ContractID = a.ContractID
        """))
    print(f"DimContract loaded successfully")
    
    # 3. Load DimPaymentMethod
    print("Loading DimPaymentMethod...")
    with dwh_engine.begin() as conn_dwh:
        conn_dwh.execute(text("DELETE FROM DimPaymentMethod"))
        conn_dwh.execute(text("""
            INSERT INTO ChurnDWH.dbo.DimPaymentMethod (PaymentMethodID, PaymentMethodName, StartDate, EndDate, IsCurrent)
            SELECT 
                PaymentMethodID,
                PaymentMethodName,
                GETDATE() AS StartDate,
                NULL AS EndDate,
                1 AS IsCurrent
            FROM CustomerChurnPrediction.dbo.PaymentMethods
        """))
    print(f"DimPaymentMethod loaded successfully")
    
    # 4. Load DimServices
    print("Loading DimServices...")
    with dwh_engine.begin() as conn_dwh:
        conn_dwh.execute(text("DELETE FROM DimServices"))
        conn_dwh.execute(text("""
            INSERT INTO ChurnDWH.dbo.DimServices 
                (CustomerID, InternetService, OnlineSecurity, OnlineBackup, DeviceProtection, 
                 PhoneService, MultipleLines, TechSupport, StreamingTV, StreamingMovies,
                 StartDate, EndDate, IsCurrent)
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
                ON cs.CustomerID = es.CustomerID
        """))
    print(f"DimServices loaded successfully")
    
    # 5. Load DimTime (populate dates from 2020 to 2030)
    print("Loading DimTime...")
    with dwh_engine.begin() as conn_dwh:
        # Check if DimTime already has data
        result = conn_dwh.execute(text("SELECT COUNT(*) FROM DimTime")).scalar()
        if result == 0:
            # Insert dates from 2020-01-01 to 2030-12-31
            conn_dwh.execute(text("""
                DECLARE @Date DATE = '2020-01-01';
                WHILE @Date <= '2030-12-31'
                BEGIN
                    INSERT INTO DimTime (
                        TimeKey, FullDate, [Day], [Month], [Year],
                        [Quarter], [WeekOfYear], [MonthName], [DayName], IsWeekend
                    )
                    VALUES (
                        CONVERT(INT, FORMAT(@Date, 'yyyyMMdd')),
                        @Date,
                        DAY(@Date),
                        MONTH(@Date),
                        YEAR(@Date),
                        DATEPART(QUARTER, @Date),
                        DATEPART(WEEK, @Date),
                        DATENAME(MONTH, @Date),
                        DATENAME(WEEKDAY, @Date),
                        CASE WHEN DATEPART(WEEKDAY, @Date) IN (1,7) THEN 1 ELSE 0 END
                    );
                    SET @Date = DATEADD(DAY, 1, @Date);
                END;
            """))
            print(f"DimTime loaded with dates from 2020-2030")
        else:
            print(f"DimTime already has {result} records, skipping...")
    print(f"DimTime loaded successfully")
    
    # 6. Load FactCustomerChurn
   
    print("All DWH data loaded successfully!")

if __name__ == '__main__':
    load_dwh_data()

