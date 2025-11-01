# 🗃️ Database Design

## Entity Relationship Diagram (ERD)
The database schema below defines the structure for managing customer data, service subscriptions, contracts, and payment details.  
It enables tracking of customer behavior, billing, and churn analysis for predictive modeling.

📸 **ER Diagram:**  
![Database Schema](../4b6717b9-9b27-4887-9212-3430f5aca198.jpg)

---

## 🧩 Tables Overview

### 🧱 Customers
| Column | Type | Description |
|---------|------|-------------|
| CustomerID | INT (PK) | Unique identifier for each customer |
| Gender | VARCHAR | Male/Female |
| IsSeniorCitizen | BIT | Indicates senior citizen status |
| HasPartner | BIT | Whether the customer has a partner |
| HasDependents | BIT | Whether the customer has dependents |

---

### 🧱 Accounts
| Column | Type | Description |
|---------|------|-------------|
| CustomerID | INT (FK) | References Customers |
| Tenure | INT | Months active |
| ContractID | INT (FK) | References Contracts |
| PaymentMethodID | INT (FK) | References PaymentMethods |
| PaperlessBilling | BIT | Indicates if billing is paperless |
| MonthlyCharges | DECIMAL | Monthly fee |
| TotalCharges | DECIMAL | Total accumulated fee |
| Churn | BIT | Whether the customer left or not |

---

### 🧱 Contracts
| Column | Type | Description |
|---------|------|-------------|
| ContractID | INT (PK) | Unique contract type ID |
| ContractType | VARCHAR | Contract type (Monthly/Yearly/etc.) |

---

### 🧱 PaymentMethods
| Column | Type | Description |
|---------|------|-------------|
| PaymentMethodID | INT (PK) | Unique ID for payment method |
| PaymentMethodName | VARCHAR | e.g., Credit Card, Bank Transfer |

---

### 🧱 CustomerServices
| Column | Type | Description |
|---------|------|-------------|
| CustomerID | INT (FK) | References Customers |
| PhoneService | BIT | Has phone service |
| MultipleLines | BIT | Has multiple lines |
| InternetService | VARCHAR | Type of internet service |
| OnlineSecurity | BIT | Subscribed to online security |
| OnlineBackup | BIT | Subscribed to online backup |
| DeviceProtection | BIT | Has device protection plan |
| TechSupport | BIT | Subscribed to technical support |

---

### 🧱 EntertainmentServices
| Column | Type | Description |
|---------|------|-------------|
| CustomerID | INT (FK) | References Customers |
| StreamingTV | BIT | Subscribed to TV streaming |
| StreamingMovies | BIT | Subscribed to movie streaming |

---

## 🔗 Relationships Summary
- `Customers (1)` → `Accounts (1)` → `Contracts (1)` → `PaymentMethods (1)`
- `Customers (1)` ↔ `CustomerServices (1)`
- `Customers (1)` ↔ `EntertainmentServices (1)`

---

## 🏗️ Design Notes
- Designed for efficient analytical queries (supports churn prediction & service usage analysis).
- Follows **3rd Normal Form (3NF)** to reduce redundancy.
- Can be extended with a **Fact Table** in a Data Warehouse for BI dashboards.
