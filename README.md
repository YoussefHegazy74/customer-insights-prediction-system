# Customer Insights & Prediction System

An end-to-end data engineering and machine learning project for telecom customer churn. The repository covers relational data modeling, a star-schema data warehouse, Airflow-orchestrated ETL, churn prediction, and dashboarding.

Built as the final project for the **Digital Egypt Pioneers Initiative (DEPI)**.

## Architecture

| Layer | Location | What it contains |
| --- | --- | --- |
| OLTP database | `Database/` | Normalized SQL Server schema, DDL, audit and validation triggers, and a cleaning/loading notebook |
| Data warehouse | `DWH/` | Star schema with `FactCustomerChurn` and five dimensions, SSIS ETL package, and SCD Type 2 handling |
| Orchestration | `Airflow/` | Astronomer/Airflow project with DAGs and Python loaders |
| Machine learning | `machine_learning/` | XGBoost churn classifier with SMOTE, threshold tuning, and a Streamlit app |
| Dashboard | `DashBoard.pbix` | Power BI report |

The warehouse follows a star schema:

- `FactCustomerChurn`
- `DimCustomer`
- `DimContract`
- `DimPaymentMethod`
- `DimServices`
- `DimTime`

## Repository layout

```text
.
├── Database/                  # OLTP schema, DDL, triggers, .bak backup
├── DWH/                       # Warehouse scripts, SSIS package, .bak backup
├── Airflow/                   # Airflow/Astronomer project and DAGs
├── machine_learning/          # Notebook, model artifacts, Streamlit app
├── Data/                      # Telco customer churn dataset
├── docs/                      # Planning, stakeholder, and database design docs
├── DashBoard.pbix             # Power BI dashboard
└── README.md
```

## Getting started

### Prerequisites

- SQL Server and SQL Server Management Studio
- Visual Studio with SQL Server Data Tools (SSDT) for the SSIS package
- Python 3.10+ for the ML module
- Docker and the Astronomer CLI for the Airflow project
- Power BI Desktop for the dashboard

### 1. Database

1. Open `Database/DDL_creating_db_tables.sql` in SQL Server Management Studio.
2. Update the MDF/LDF file paths at the top of the script for your SQL Server instance.
3. Run the script to create the database and tables.
4. Optionally restore `Database/CustomerChurnPrediction.bak` for a pre-loaded database.

The database includes triggers for customer auditing and charge validation. See `Database/README.md` for the schema and trigger details.

### 2. Data warehouse

1. Restore `DWH/ChurnDWH.bak`, or run the scripts in `DWH/DWH_Scripts/` in numeric order.
2. Open `DWH/ETL/Package.dtsx` in Visual Studio/SSDT.
3. Update the source and destination connection managers for your environment.
4. Execute the package to load the dimensions and the fact table.

See `DWH/README.md` for the star schema and ETL flow.

### 3. Machine learning app

```bash
cd machine_learning
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

The app loads `churn_model.pkl` and `feature_names.pkl` from the same directory. The model is tuned for recall on the churn class and uses a decision threshold of `0.25`.

Training code and experiments are in `machine_learning/DEPI_Graduation_Egypt.ipynb`. The notebook uses a Colab path; update it to `../Data/WA_Fn-UseC_-Telco-Customer-Churn.csv` when running locally.

### 4. Airflow pipeline

```bash
cd Airflow/AirFlow_DEPI/airflow_depi
astro dev start
```

The DAGs are in `dags/`, and the loader modules are in `include/`. Configure the SQL Server connections in Airflow before running the pipeline. Use Airflow connections or environment variables for credentials; do not commit secrets.

### 5. Dashboard

Open `DashBoard.pbix` in Power BI Desktop and refresh the data source if needed.

## Documentation

- [Database design](Database/README.md)
- [Data warehouse and SSIS](DWH/README.md)
- [Machine learning module](machine_learning/README.md)
- [Project planning](docs/Project_Planning.md)
- [Stakeholder analysis](docs/Stakeholder_Analysis.md)
- [Database design notes](docs/Database_Design.md)

## Team

| Name | Role | LinkedIn |
| --- | --- | --- |
| **Youssef Ali Hegazy** | Team Leader, Machine Learning | https://www.linkedin.com/in/youssef-a-hegazy |
| Osama Essam Azab | Data Collection and Preprocessing | https://www.linkedin.com/in/osama-essam-azab |
| Ahmed Tarek Elmenoufy | Database | https://www.linkedin.com/in/ahmed-tarek06 |
| Mostafa Sobhy Mahmoud | Data Warehouse and ETL (SSIS) | https://www.linkedin.com/in/shehify |
| Tomas Amir Gerges | Airflow | - |
| Mohammed Hani Ebraheem | Power BI Dashboard | - |

## Notes

- The repository contains database backups and a trained model artifact for demonstration.
- Credentials and connection strings should be supplied through environment variables or Airflow connections, not committed to the repository.
- The Power BI report and SQL Server backups require their respective desktop tools to open.
