# **Telco Customer Churn Database Architecture**

## **System Overview**

This project focuses on the architectural design of a high-integrity relational database for telecommunications data. Utilizing **Microsoft SQL Server**, the system implements a strictly normalized **Third Normal Form (3NF)** schema to ensure data consistency, reduced redundancy, and optimized storage.

Beyond static storage, the database features **active logic layer** consisting of triggers and constraints that enforce complex business rules, revenue protection, and auditability directly at the database level.

## **🏗️ Relational Schema Design (OLTP)**

The database transforms a flat data structure into a modular, relational ecosystem consisting of five core entities and reference tables.

### **1\. Entity Breakdown**

* **Customers (Core Entity):**  
  * Stores immutable or slowly changing demographic data (Gender, Senior Status, Partner/Dependents).  
  * *Design Choice:* Serves as the root parent table for all other relationships.  
* **Accounts (Transactional Entity):**  
  * Manages dynamic billing details, tenure, contract states, and churn indicators.  
  * *Design Choice:* Separated from demographics to isolate financial data from personal data.  
* **CustomerServices & EntertainmentServices (Vertical Partitioning):**  
  * **CustomerServices**: Tracks essential utilities (Phone, Internet, Tech Support).  
  * **EntertainmentServices**: Isolates add-on products (Streaming TV, Movies).  
  * *Design Choice:* These tables are split to logically group related services and allow for modular expansion without altering the core customer table.  
* **Contracts & PaymentMethods (Lookup/Reference):**  
  * Standardizes categorical values (e.g., "Month-to-month", "Electronic check") into integer-based Foreign Keys.  
  * *Design Choice:* Reduces storage footprint (integer vs. string) and prevents data anomalies (typos in contract names).

### **2\. Relationship Strategy & Cardinality**

* **One-to-One (1:1) Relationships:**  
  * The service tables (CustomerServices, EntertainmentServices) share the same Primary Key (CustomerID) as the parent Customers table.  
  * *Benefit:* This strictly enforces that a single customer cannot have duplicate service records, maintaining logical data integrity.  
* **One-to-Many (1:M) Relationships:**  
  * Reference tables (Contracts) link to the Accounts table, allowing multiple customers to share the same contract type while maintaining a single source of truth for the contract definition.

### **3\. Referential Integrity**

* **Cascading Deletes:** Foreign Keys are configured with ON DELETE CASCADE.  
  * *Behavior:* Deleting a record from the Customers table automatically cleans up all related records in Accounts and Services tables, preventing orphaned data.

## **⚡ Active Database Features**

The database is designed to be "intelligent," rejecting invalid data and automating maintenance tasks through SQL Triggers.

### **1\. Security & Compliance: Automated Audit Trail**

* **Trigger:** trg\_CustomerAudit  
* **Mechanism:** An AFTER UPDATE, DELETE trigger.  
* **Function:** Automatically captures the state of a record *before* a change occurs. It logs the CustomerID, the specific data points, the action type (UPDATE/DELETE), and a timestamp into a dedicated immutable CustomerAudit table.  
* **Business Value:** Provides a complete historical lineage for compliance and accidental data recovery.

### **2\. Data Quality: Financial Logic Validation**

* **Trigger:** trg\_ValidateCharges  
* **Mechanism:** An AFTER INSERT, UPDATE trigger.  
* **Function:** Validates the mathematical logic of financial entries.  
  * Rule A: TotalCharges cannot be negative.  
  * Rule B: TotalCharges cannot be less than MonthlyCharges for active users.  
* **Business Value:** Ensures the database never accepts illogical financial data, preserving trust in downstream reporting.

## **🛡️ Structural Data Integrity**

* **Boolean Optimization:** All binary states (Yes/No) are stored as BIT types (0/1) for storage efficiency and faster indexing.  
* **Domain Constraints:** CHECK constraints are applied to categorical columns (e.g., Gender IN ('M', 'F')) to reject non-conforming data at the entry point.