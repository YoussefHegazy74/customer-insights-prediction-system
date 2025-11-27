import pandas as pd
from sqlalchemy import create_engine, text
import urllib

CSV_FILE_PATH = '/usr/local/airflow/include/WA_Fn-UseC_-Telco-Customer-Churn.csv'

params = urllib.parse.quote_plus(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=host.docker.internal,1433;'
    'DATABASE=CustomerChurnPrediction;'
    'UID=airflow_etl;'
    'PWD=AirflowETL@2024!;'
    'TrustServerCertificate=yes;'
)
DB_CONNECTION_STRING = f'mssql+pyodbc:///?odbc_connect={params}'

def extract_data():
    print(f"Reading data from {CSV_FILE_PATH}...")
    df = pd.read_csv(CSV_FILE_PATH)
    df.columns = [col.strip() for col in df.columns]
    return df

def clean_data(df):
    print("Cleaning data...")
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    df['TotalCharges'] = df['TotalCharges'].fillna(0)
    for col in ['Partner', 'Dependents', 'PhoneService', 'PaperlessBilling', 'Churn',
                'OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 'TechSupport',
                'StreamingTV', 'StreamingMovies', 'MultipleLines']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if x == 'Yes' else False)
    df['IsSeniorCitizen'] = df['SeniorCitizen'].apply(lambda x: True if x == 1 else False)
    df['gender'] = df['gender'].str.strip().str[0]
    print("Data cleaning complete.")
    return df

def load_data(df):
    print("Starting data transformation and loading...")
    engine = create_engine(DB_CONNECTION_STRING)
    
    # Delete existing data from all tables (to avoid duplicates)
    # IMPORTANT: Delete in correct order due to foreign key constraints
    # Delete child tables first, then parent tables, then lookup tables
    # Use engine.begin() for automatic transaction management
    with engine.begin() as conn:
        # Delete child tables first
        conn.execute(text("DELETE FROM EntertainmentServices"))
        conn.execute(text("DELETE FROM CustomerServices"))
        conn.execute(text("DELETE FROM Accounts"))
        conn.execute(text("DELETE FROM Customers"))
        # Then delete lookup tables
        conn.execute(text("DELETE FROM PaymentMethods"))
        conn.execute(text("DELETE FROM Contracts"))
    print("Cleared all existing data from tables.")

    # Load lookup tables first (needed for foreign keys)
    contract_types = df[['Contract']].drop_duplicates().reset_index(drop=True)
    contract_types.rename(columns={'Contract': 'ContractType'}, inplace=True)
    contract_types.to_sql('Contracts', engine, if_exists='append', index=False)
    print("Loaded Contracts lookup table.")

    payment_methods = df[['PaymentMethod']].drop_duplicates().reset_index(drop=True)
    payment_methods.rename(columns={'PaymentMethod': 'PaymentMethodName'}, inplace=True)
    payment_methods.to_sql('PaymentMethods', engine, if_exists='append', index=False)
    print("Loaded PaymentMethods lookup table.")

    # Map lookup IDs to main dataframe
    contracts_map = pd.read_sql('SELECT * FROM Contracts', engine).set_index('ContractType')
    payment_methods_map = pd.read_sql('SELECT * FROM PaymentMethods', engine).set_index('PaymentMethodName')
    df['ContractID'] = df['Contract'].map(contracts_map['ContractID'])
    df['PaymentMethodID'] = df['PaymentMethod'].map(payment_methods_map['PaymentMethodID'])

    customers_df = df[['customerID', 'gender', 'IsSeniorCitizen', 'Partner', 'Dependents']]
    customers_df = customers_df.rename(columns={
        'customerID': 'CustomerID', 'gender': 'Gender', 'Partner': 'HasPartner', 'Dependents': 'HasDependents'
    })
    customers_df.to_sql('Customers', engine, if_exists='append', index=False)
    print("Loaded Customers table.")

    accounts_df = df[[
        'customerID', 'tenure', 'ContractID', 'PaymentMethodID', 'PaperlessBilling',
        'MonthlyCharges', 'TotalCharges', 'Churn'
    ]]
    accounts_df = accounts_df.rename(columns={'customerID': 'CustomerID', 'tenure': 'Tenure'})
    accounts_df.to_sql('Accounts', engine, if_exists='append', index=False)
    print("Loaded Accounts table.")

    services_df = df[[
        'customerID', 'PhoneService', 'MultipleLines', 'InternetService',
        'OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 'TechSupport'
    ]]
    services_df = services_df.rename(columns={'customerID': 'CustomerID'})
    services_df.to_sql('CustomerServices', engine, if_exists='append', index=False)
    print("Loaded CustomerServices table.")

    entertainment_df = df[['customerID', 'StreamingTV', 'StreamingMovies']]
    entertainment_df = entertainment_df.rename(columns={'customerID': 'CustomerID'})
    entertainment_df.to_sql('EntertainmentServices', engine, if_exists='append', index=False)
    print("Loaded EntertainmentServices table.")

    print("All data loaded successfully!")
