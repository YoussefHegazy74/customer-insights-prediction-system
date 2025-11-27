"""
Script to load data from CustomerChurnPrediction database to ChurnDWH Data Warehouse
with proper SCD Type 2 implementation for dimensions.
"""
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime

def load_dwh_data():
    """Load data from source database to DWH with SCD Type 2"""
    
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
    
    today = datetime.today().strftime('%Y-%m-%d')
    print("Starting DWH load with SCD Type 2...")

    # ----------------------------
    # 1. Load DimCustomer (SCD Type 2)
    # ----------------------------
    print("Loading DimCustomer with SCD Type 2...")
    with dwh_engine.begin() as conn_dwh:
        conn_dwh.execute(text("""
            -- Update old records if there are changes
            UPDATE dim
            SET dim.EndDate = CONVERT(DATE, GETDATE()-1),
                dim.IsCurrent = 0
            FROM DimCustomer dim
            JOIN CustomerChurnPrediction.dbo.Customers src
                ON dim.CustomerID = src.CustomerID
            WHERE dim.IsCurrent = 1
              AND (
                dim.Gender <> src.Gender OR
                dim.IsSeniorCitizen <> src.IsSeniorCitizen OR
                dim.HasPartner <> src.HasPartner OR
                dim.HasDependents <> src.HasDependents
              );

            -- Insert new/current records
            INSERT INTO DimCustomer (CustomerID, Gender, IsSeniorCitizen, HasPartner, HasDependents, StartDate, EndDate, IsCurrent)
            SELECT 
                src.CustomerID,
                src.Gender,
                src.IsSeniorCitizen,
                src.HasPartner,
                src.HasDependents,
                CONVERT(DATE, GETDATE()) AS StartDate,
                NULL AS EndDate,
                1 AS IsCurrent
            FROM CustomerChurnPrediction.dbo.Customers src
            LEFT JOIN DimCustomer dim
                ON src.CustomerID = dim.CustomerID AND dim.IsCurrent = 1
            WHERE dim.CustomerID IS NULL
               OR dim.Gender <> src.Gender
               OR dim.IsSeniorCitizen <> src.IsSeniorCitizen
               OR dim.HasPartner <> src.HasPartner
               OR dim.HasDependents <> src.HasDependents;
        """))
    print("DimCustomer loaded with SCD Type 2 successfully.")

    # ----------------------------
    # 2. Load DimContract (SCD Type 2)
    # ----------------------------
    print("Loading DimContract with SCD Type 2...")
    with dwh_engine.begin() as conn_dwh:
        conn_dwh.execute(text("""
            -- Update old records if there are changes
            UPDATE dim
            SET dim.EndDate = CONVERT(DATE, GETDATE()-1),
                dim.IsCurrent = 0
            FROM DimContract dim
            JOIN CustomerChurnPrediction.dbo.Contracts src
                ON dim.ContractID = src.ContractID
            JOIN CustomerChurnPrediction.dbo.Accounts a
                ON a.ContractID = src.ContractID
            WHERE dim.IsCurrent = 1
              AND dim.PaperlessBilling <> a.PaperlessBilling;

            -- Insert new/current records
            INSERT INTO DimContract (ContractID, ContractType, PaperlessBilling, StartDate, EndDate, IsCurrent)
            SELECT DISTINCT
                src.ContractID,
                src.ContractType,
                a.PaperlessBilling,
                CONVERT(DATE, GETDATE()) AS StartDate,
                NULL AS EndDate,
                1 AS IsCurrent
            FROM CustomerChurnPrediction.dbo.Contracts src
            JOIN CustomerChurnPrediction.dbo.Accounts a
                ON a.ContractID = src.ContractID
            LEFT JOIN DimContract dim
                ON src.ContractID = dim.ContractID AND dim.IsCurrent = 1
            WHERE dim.ContractID IS NULL
               OR dim.PaperlessBilling <> a.PaperlessBilling;
        """))
    print("DimContract loaded with SCD Type 2 successfully.")

    # ----------------------------
    # 3. Load DimPaymentMethod (SCD Type 2)
    # ----------------------------
    print("Loading DimPaymentMethod with SCD Type 2...")
    with dwh_engine.begin() as conn_dwh:
        conn_dwh.execute(text("""
            -- Update old records if there are changes
            UPDATE dim
            SET dim.EndDate = CONVERT(DATE, GETDATE()-1),
                dim.IsCurrent = 0
            FROM DimPaymentMethod dim
            JOIN CustomerChurnPrediction.dbo.PaymentMethods src
                ON dim.PaymentMethodID = src.PaymentMethodID
            WHERE dim.IsCurrent = 1
              AND dim.PaymentMethodName <> src.PaymentMethodName;

            -- Insert new/current records
            INSERT INTO DimPaymentMethod (PaymentMethodID, PaymentMethodName, StartDate, EndDate, IsCurrent)
            SELECT 
                src.PaymentMethodID,
                src.PaymentMethodName,
                CONVERT(DATE, GETDATE()) AS StartDate,
                NULL AS EndDate,
                1 AS IsCurrent
            FROM CustomerChurnPrediction.dbo.PaymentMethods src
            LEFT JOIN DimPaymentMethod dim
                ON src.PaymentMethodID = dim.PaymentMethodID AND dim.IsCurrent = 1
            WHERE dim.PaymentMethodID IS NULL
               OR dim.PaymentMethodName <> src.PaymentMethodName;
        """))
    print("DimPaymentMethod loaded with SCD Type 2 successfully.")

    # ----------------------------
    # 4. Load DimServices (SCD Type 2)
    # ----------------------------
    print("Loading DimServices with SCD Type 2...")
    with dwh_engine.begin() as conn_dwh:
        conn_dwh.execute(text("""
            -- Update old records if there are changes
            UPDATE dim
            SET dim.EndDate = CONVERT(DATE, GETDATE()-1),
                dim.IsCurrent = 0
            FROM DimServices dim
            JOIN CustomerChurnPrediction.dbo.CustomerServices cs
                ON dim.CustomerID = cs.CustomerID
            JOIN CustomerChurnPrediction.dbo.EntertainmentServices es
                ON cs.CustomerID = es.CustomerID
            WHERE dim.IsCurrent = 1
              AND (
                dim.InternetService <> cs.InternetService OR
                dim.OnlineSecurity <> cs.OnlineSecurity OR
                dim.OnlineBackup <> cs.OnlineBackup OR
                dim.DeviceProtection <> cs.DeviceProtection OR
                dim.PhoneService <> cs.PhoneService OR
                dim.MultipleLines <> cs.MultipleLines OR
                dim.TechSupport <> cs.TechSupport OR
                dim.StreamingTV <> es.StreamingTV OR
                dim.StreamingMovies <> es.StreamingMovies
              );

            -- Insert new/current records
            INSERT INTO DimServices (
                CustomerID, InternetService, OnlineSecurity, OnlineBackup, DeviceProtection,
                PhoneService, MultipleLines, TechSupport, StreamingTV, StreamingMovies,
                StartDate, EndDate, IsCurrent
            )
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
                CONVERT(DATE, GETDATE()) AS StartDate,
                NULL AS EndDate,
                1 AS IsCurrent
            FROM CustomerChurnPrediction.dbo.CustomerServices cs
            JOIN CustomerChurnPrediction.dbo.EntertainmentServices es
                ON cs.CustomerID = es.CustomerID
            LEFT JOIN DimServices dim
                ON cs.CustomerID = dim.CustomerID AND dim.IsCurrent = 1
            WHERE dim.CustomerID IS NULL
               OR dim.InternetService <> cs.InternetService
               OR dim.OnlineSecurity <> cs.OnlineSecurity
               OR dim.OnlineBackup <> cs.OnlineBackup
               OR dim.DeviceProtection <> cs.DeviceProtection
               OR dim.PhoneService <> cs.PhoneService
               OR dim.MultipleLines <> cs.MultipleLines
               OR dim.TechSupport <> cs.TechSupport
               OR dim.StreamingTV <> es.StreamingTV
               OR dim.StreamingMovies <> es.StreamingMovies;
        """))
    print("DimServices loaded with SCD Type 2 successfully.")

    # ----------------------------
    # 5. Load DimTime (if empty)
    # ----------------------------
    print("Loading DimTime...")
    with dwh_engine.begin() as conn_dwh:
        result = conn_dwh.execute(text("SELECT COUNT(*) FROM DimTime")).scalar()
        if result == 0:
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
            print("DimTime loaded with dates 2020-2030.")
        else:
            print(f"DimTime already has {result} records, skipping...")

    # ----------------------------
    # 6. Load FactCustomerChurn
    # ----------------------------
    print("Loading FactCustomerChurn...")
    with dwh_engine.begin() as conn_dwh:
        conn_dwh.execute(text("DELETE FROM FactCustomerChurn"))
        conn_dwh.execute(text("""
            INSERT INTO FactCustomerChurn 
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
    print("FactCustomerChurn loaded successfully.")

    print("All DWH data loaded successfully with SCD Type 2!")

if __name__ == '__main__':
    load_dwh_data()
