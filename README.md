# Enterprise AML Compliance & Fraud Detection Framework
**Unified Data Engineering on Microsoft Fabric**


## Project Strategy
In the banking sector, data is a liability if not governed correctly. This project demonstrates a production-grade **Anti-Money Laundering (AML)** pipeline built on the **Microsoft Fabric SaaS platform**. 
---

## 🏗️ Architecture: The Medallion + Quarantine Pattern
I have implemented a highly resilient **Medallion Architecture** that prioritizes data integrity through a custom **Dead Letter Queue (Quarantine)** pattern.

```mermaid
graph LR
    A[Source: UPI Data] --> B[(Bronze: Raw)]
    B --> C{Data Quality Gate}
    C -->|Valid| D[(Silver: Enriched)]
    C -->|Invalid| E[(Quarantine: DLQ)]
    D --> F[(Gold: Fraud Alerts)]
    F --> G[Power BI / SQL]
    
    style B fill:#f9f
    style D fill:#bbf
    style E fill:#ff9
    style F fill:#bfb

```

## Project Proof & Results

### 1. Data Ingestion & Quality Control
I purposefully "poisoned" the Bronze layer with invalid records (negative amounts, fake UPI IDs) to test the robustness of the **Quarantine Pattern**.

| Bronze: Raw & Poisoned | Silver: Quarantine (DLQ) |
| :--- | :--- |
| ![Bronze Data](./docs/screenshots/02_bronze_poisoned.png) | ![Quarantine Layer](./docs/screenshots/03_quarantine_layer.png) |
| *Raw data with intentional anomalies.* | *Records isolated with 'error_reason' metadata.* |

### 2. Data Engineering Excellence (SCD Type 2)
The Silver layer tracks every historical change. Notice the `Is_active` flag and `Effective_End` dates, which provide a 100% accurate audit trail for compliance.

<img src="./docs/screenshots/04_silver_scd2_history.png" width="900" alt="SCD2 History">

### 3. Fraud Detection (Gold) & Governance (RLS)
The final Gold table identifies "Smurfing" patterns. Using **Row-Level Security**, we ensure an auditor only sees data for their assigned district.

| Gold: PMLA Fraud Alerts | SQL: RLS Filtered Result |
| :--- | :--- |
| ![Gold Alerts](./docs/screenshots/05_gold_fraud_alerts.png) | ![RLS Result](./docs/screenshots/07_rls_final_result.png) |
| *Users flagged for high-velocity transactions.* | *Filtered view: Auditor sees ONLY Chennai data.* |

---

## 📁 Repository Structure
```text
├── notebooks/          # PySpark logic: Ingestion, SCD2, Fraud Analytics
├── sql/                # RLS Functions and Security Policies (T-SQL)
├── docs/screenshots/   # Proof of Work (Success logs & tables)
└── README.md           # Documentation
