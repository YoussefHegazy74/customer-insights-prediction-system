import pandas as pd
from sqlalchemy import create_engine
import urllib

CSV_FILE_PATH = '/usr/local/airflow/include/WA_Fn-UseC_-Telco-Customer-Churn.csv'

username = 'airflow_etl'
password = 'AirflowETL@2024!'
server = 'host.docker.internal'
port = '1433'
database = 'CustomerChurnPrediction'
driver = 'ODBC Driver 17 for SQL Server'

params = urllib.parse.quote_plus(
    f'DRIVER={driver};'
    f'SERVER={server},{port};'
    f'DATABASE={database};'
    f'UID={username};PWD={password};'
    f'TrustServerCertificate=yes'
)
DB_CONNECTION_STRING = f'mssql+pyodbc:///?odbc_connect={params}'

def main():
    df = pd.read_csv(CSV_FILE_PATH)
    print(f'عدد الصفوف المستخرجة: {len(df)}')
    engine = create_engine(DB_CONNECTION_STRING)
    df.to_sql('CustomerChurnTable', engine, if_exists='replace', index=False)
    print('تم التحميل بنجاح!')

if __name__ == '__main__':
    main()
