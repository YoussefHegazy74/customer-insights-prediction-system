## 📌 Folder Structure
"""
DWH/
│
├── DWH_Scripts/
│   ├── 1- DimCustomer.sql
│   ├── 2- DimContract.sql
│   ├── 3- DimPaymentMethod.sql
│   ├── 4- DimServices.sql
│   ├── 5- DimTime.sql
│   └── 6- FactCustomerChurn.sql
│
├── ETL/
│   ├── ETL_Control_Flow.jpg
│   ├── ETL_Data_Flow.jpg
│   ├── ETL_Data_Flow_Payment_DIm.jpg
│   ├── Package.dtsx
│   └── ChurnDWH.bak
│
└── DataWarehouse_Schema.jpeg
"""

⭐ 1. Overview

This module represents the Data Warehouse layer of the Customer Insights system.
It includes:

📊 Star Schema for Customer Churn Analysis

⚙️ ELT Pipelines built using SSIS

🗄️ DWH SQL Scripts (Dimensions + Fact)

🔁 SCD (Slowly Changing Dimension) logic

🔌 Lookup transformations for foreign keys

This layer provides clean, structured, and analytics-ready data to be used by the ML and Dashboard teams.

⭐ 2. Data Warehouse Schema (Star Model)

The DWH follows a Star Schema centered around FactCustomerChurn with five dimensions:

DimCustomer

DimServices

DimContract

DimPaymentMethod

DimTime

📎 Schema Diagram

Use this path if the image is inside the DWH folder:

![Schema](DataWarehouse_Schema.jpeg)

⭐ 3. ELT Pipelines (SSIS)

All pipelines follow ELT logic:

Load raw data from source

Apply transformations (lookups, derived columns, SCD)

Load dimensions → then fact table

▶️ 3.1 Control Flow
![Control Flow](ETL/ETL_Control_Flow.jpg)


This control flow loads:

DimCustomer

DimServices

DimContract

DimPaymentMethod

Then loads FactCustomerChurn.

▶️ 3.2 Data Flow – FactCustomerChurn
![Fact Data Flow](ETL/ETL_Data_Flow.jpg)


Contains:

OLE DB Source

Lookup transformations for all FK keys

Derived columns

OLE DB Destination

▶️ 3.3 Data Flow – Payment Method SCD
![Payment Method SCD](ETL/ETL_Data_Flow_Payment_DIm.jpg)


Contains:

Slowly Changing Dimension (SCD Type 2)

Derived Columns

OLE DB Command

Union All

Insert Destination

⭐ 4. SQL Scripts

Located under /DWH_Scripts

DimCustomer.sql

DimServices.sql

DimContract.sql

DimPaymentMethod.sql

DimTime.sql

FactCustomerChurn.sql

Scripts include:

Table creation

Primary keys

Identity columns

Foreign keys

SCD logic (where applicable)

⭐ 5. How to Run the DWH
✔️ Requirements

SQL Server

SSIS (SQL Server Data Tools)

Customer dataset loaded into staging

Optional: Restore ChurnDWH.bak

▶️ Steps

Restore the .bak file (optional)

Run all .sql scripts to create the schema

Open Package.dtsx in SSIS

Update connection managers:

Src_Churn

Des_ChurnDWH

Execute Sequence Container

Verify dimensions + fact load correctly

⭐ 6. Tools & Technologies

SQL Server

SSIS

Star Schema

SCD Type 2

Lookup Transformations

Visual Studio (SSDT)

⭐ 7. Responsibilities (Your Work)

This DWH & ETL module was implemented by:

Mostafa Sobhy Mahmoud

Role: Data Warehouse & ETL Engineer

You implemented:

DWH Schema design

ELT pipelines

SCD logic

SQL scripts

Fact/dimension loading

ETL orchestration via SSIS

⭐ 8. Notes

All ETL diagrams must exist inside /ETL folder

Update image paths if filenames change

This folder is self-contained and reusable

🎉 End of README
