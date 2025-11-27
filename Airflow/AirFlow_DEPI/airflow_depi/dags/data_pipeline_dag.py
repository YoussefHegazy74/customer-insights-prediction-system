from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from pendulum import datetime
import sys, os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../include'))
import database_clean_load as dcl
from sqlalchemy import create_engine, text
import urllib.parse

# Import DWH data loading script
import load_dwh_data

DWH_SCRIPTS = [
    '/usr/local/airflow/include/DWH_Scripts/6- FactCustomerChurn.sql',
    '/usr/local/airflow/include/DWH_Scripts/1- DimCustomer.sql',
    '/usr/local/airflow/include/DWH_Scripts/2- DimContract.sql',
    '/usr/local/airflow/include/DWH_Scripts/3- DimPaymentMethod.sql',
    '/usr/local/airflow/include/DWH_Scripts/4- DimServices.sql',
    '/usr/local/airflow/include/DWH_Scripts/5- DimTime.sql' 
]

@dag(
    start_date=datetime(2025, 11, 1),
    schedule='@daily',
    catchup=False,
    default_args={"owner": "customer-insights", "retries": 1},
    tags=["etl", "churn", "dwh"]
)
def customer_data_pipeline():
    """End-to-end ETL: extract, clean, load to DB, then populate DWH."""

    @task()
    def extract_data():
        return dcl.extract_data().to_json()

    @task()
    def clean_data(df_json):
        import pandas as pd
        df = pd.read_json(df_json)
        return dcl.clean_data(df).to_json()

    @task()
    def load_data(df_json):
        import pandas as pd
        df = pd.read_json(df_json)
        dcl.load_data(df)

    @task()
    def run_dwh_scripts():
        """Execute DWH SQL scripts using SQLAlchemy"""
        # Use same connection settings as database_clean_load
        username = 'airflow_etl'
        password = 'AirflowETL@2024!'
        server = 'host.docker.internal'
        port = '1433'
        database = 'CustomerChurnPrediction'  # Default database for connection
        driver = 'ODBC Driver 17 for SQL Server'
        
        params = urllib.parse.quote_plus(
            f'DRIVER={driver};'
            f'SERVER={server},{port};'
            f'DATABASE={database};'
            f'UID={username};PWD={password};'
            f'TrustServerCertificate=yes'
        )
        connection_string = f'mssql+pyodbc:///?odbc_connect={params}'
        engine = create_engine(connection_string)
        
        # Ensure ChurnDWH database exists
        # Connect to master database to create ChurnDWH if needed
        params_master = urllib.parse.quote_plus(
            f'DRIVER={driver};'
            f'SERVER={server},{port};'
            f'DATABASE=master;'
            f'UID={username};PWD={password};'
            f'TrustServerCertificate=yes'
        )
        connection_string_master = f'mssql+pyodbc:///?odbc_connect={params_master}'
        engine_master = create_engine(connection_string_master)
        
        with engine_master.begin() as conn_master:
            # Check if ChurnDWH exists, create if not
            result = conn_master.execute(text("""
                IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'ChurnDWH')
                BEGIN
                    CREATE DATABASE ChurnDWH;
                    PRINT 'Database ChurnDWH created successfully';
                END
                ELSE
                BEGIN
                    PRINT 'Database ChurnDWH already exists';
                END
            """))
        
        # Now connect directly to ChurnDWH to grant permissions
        params_churndwh = urllib.parse.quote_plus(
            f'DRIVER={driver};'
            f'SERVER={server},{port};'
            f'DATABASE=ChurnDWH;'
            f'UID={username};PWD={password};'
            f'TrustServerCertificate=yes'
        )
        connection_string_churndwh = f'mssql+pyodbc:///?odbc_connect={params_churndwh}'
        
        # Try to grant permissions - if it fails, continue anyway (might be set up manually)
        try:
            engine_churndwh = create_engine(connection_string_churndwh)
            with engine_churndwh.begin() as conn_churndwh:
                # Grant permissions to airflow_etl user on ChurnDWH
                conn_churndwh.execute(text("""
                    IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'airflow_etl')
                    BEGIN
                        CREATE USER airflow_etl FOR LOGIN airflow_etl;
                    END
                    
                    -- Add to db_owner role if not already a member
                    IF NOT EXISTS (
                        SELECT 1 
                        FROM sys.database_role_members rm
                        INNER JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
                        INNER JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
                        WHERE r.name = 'db_owner' AND m.name = 'airflow_etl'
                    )
                    BEGIN
                        ALTER ROLE db_owner ADD MEMBER airflow_etl;
                    END
                """))
            print("Database ChurnDWH ready with proper permissions")
        except Exception as e:
            # If we can't grant permissions, warn but continue
            # User may have already set up permissions manually
            print(f"Warning: Could not grant permissions automatically. Error: {e}")
            print("If ChurnDWH was created manually, you need to run:")
            print("USE ChurnDWH; CREATE USER airflow_etl FOR LOGIN airflow_etl; ALTER ROLE db_owner ADD MEMBER airflow_etl;")
            print("Continuing anyway - permissions may already be set up...")
        
        for script_path in DWH_SCRIPTS:
            print(f"Running DWH script: {script_path}")
            try:
                # Read SQL file
                if not os.path.exists(script_path):
                    raise FileNotFoundError(f"Script file not found: {script_path}")
                
                with open(script_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                # Split SQL content by GO statements and USE statements
                # Execute each statement separately
                statements = []
                current_statement = []
                
                for line in sql_content.split('\n'):
                    line = line.strip()
                    # Skip empty lines and comments
                    if not line or line.startswith('--'):
                        continue
                    # Skip CREATE DATABASE statements (handled separately)
                    if line.upper().startswith('CREATE DATABASE'):
                        print(f"Skipping CREATE DATABASE statement (database already ensured)")
                        continue
                    # Handle USE statements separately
                    if line.upper().startswith('USE '):
                        statements.append(('USE', line))
                    # Handle GO as statement separator
                    elif line.upper() == 'GO':
                        if current_statement:
                            statements.append(('SQL', '\n'.join(current_statement)))
                            current_statement = []
                    else:
                        current_statement.append(line)
                
                # Add remaining statement
                if current_statement:
                    statements.append(('SQL', '\n'.join(current_statement)))
                
                # Execute statements - handle USE by creating separate connection
                current_db = database  # Start with default database
                
                for stmt_type, stmt in statements:
                    if stmt_type == 'USE':
                        # USE statements - create new connection to target database
                        db_name = stmt.split()[1].rstrip(';').strip('[]')
                        print(f"Switching to database: {db_name}")
                        current_db = db_name
                        # Create new engine for the target database
                        params_db = urllib.parse.quote_plus(
                            f'DRIVER={driver};'
                            f'SERVER={server},{port};'
                            f'DATABASE={db_name};'
                            f'UID={username};PWD={password};'
                            f'TrustServerCertificate=yes'
                        )
                        connection_string_db = f'mssql+pyodbc:///?odbc_connect={params_db}'
                        engine_db = create_engine(connection_string_db)
                    elif stmt_type == 'SQL' and stmt.strip():
                        # Skip SELECT statements (they're queries, not DDL/DML)
                        if stmt.strip().upper().startswith('SELECT'):
                            print(f"Skipping SELECT query (not executable): {stmt[:80]}...")
                            continue
                        
                        # Execute SQL statement in current database context
                        try:
                            if current_db != database:
                                # Use engine for target database
                                with engine_db.begin() as conn_db:
                                    conn_db.execute(text(stmt))
                            else:
                                # Use original engine
                                with engine.begin() as conn:
                                    conn.execute(text(stmt))
                        except Exception as sql_err:
                            # Handle "object already exists" errors gracefully
                            error_str = str(sql_err).lower()
                            if 'already exists' in error_str or '2714' in error_str or '42s01' in error_str:
                                print(f"Warning: Object already exists, skipping: {stmt[:100]}...")
                                continue
                            # For other errors, also handle index creation if table exists
                            elif 'index' in error_str and ('already exists' in error_str or '2501' in error_str):
                                print(f"Warning: Index already exists, skipping: {stmt[:100]}...")
                                continue
                            else:
                                # Re-raise other errors
                                raise
                
                print(f"Successfully executed: {script_path}")
                
            except Exception as e:
                print(f"Error executing {script_path}: {str(e)}")
                raise Exception(f"Failed to execute {script_path}: {str(e)}")
        
        print("All DWH scripts executed successfully!")
    
    @task()
    def load_dwh_data_task():
        """Load data from source database to DWH tables"""
        load_dwh_data.load_dwh_data()

    # Define task dependencies - execute in order
    # TaskFlow API automatically creates dependencies when passing results
    df_raw = extract_data()
    df_clean = clean_data(df_raw)
    load = load_data(df_clean)
    
    # DWH scripts run after data is loaded, then load data into DWH
    dwh = run_dwh_scripts()
    dwh_data = load_dwh_data_task()
    
    load >> dwh >> dwh_data  # Ensure DWH runs after load_data, then load DWH data

customer_data_pipeline()
