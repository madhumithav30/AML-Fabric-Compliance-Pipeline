# Enterprise AML Compliance & Fraud Detection Framework
**Unified Data Engineering on Microsoft Fabric**

# Project Stratergy
In the banking sector, data is a liability if not governed correctly. This project demonstrates a production-grade **Anti-Money Laundering (AML)** pipeline built on the **Microsoft Fabric SaaS platform**. 

## Architecture: The Medallion + Quarantine Pattern
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

## Technical Proof of Work (Verified in Microsoft Fabric)

Below are the verified results of each pipeline stage, demonstrating the successful handling of data ingestion, quality enforcement, and jurisdictional security.

### 1. Raw Ingestion & Data Poisoning (Bronze)
To validate the resilience of the Data Quality Gate, I purposefully "poisoned" the Bronze layer with invalid records (negative amounts and malformed UPI handles).

| Bronze: Raw Base Table | Bronze: Poisoned Records (Anomalies) |
| :--- | :--- |
| ![Bronze Raw](https://github.com/madhumithav30/AML-Fabric-Compliance-Pipeline/blob/4b4d246830788c04278608d96e4639d4fa3212ea/AML-Fabric-Compliance-Pipeline/design%20docs/screenshots/01_bronze_layer.png) | ![Poisoned](https://github.com/madhumithav30/AML-Fabric-Compliance-Pipeline/blob/4b4d246830788c04278608d96e4639d4fa3212ea/AML-Fabric-Compliance-Pipeline/design%20docs/screenshots/02_bronze_poisoned.png) |

---

### 2. The Quarantine & Silver Layer Sync
I implemented a **Dead Letter Queue (DLQ)** pattern. Notice how the Silver layer maintains the historical SCD Type 2 records while the malformed data is isolated for audit.

| Silver: SCD2 History & Quarantine Log | Silver: Production-Ready (Clean) Data |
| :--- | :--- |
| ![SCD2 Quarantine](https://github.com/madhumithav30/AML-Fabric-Compliance-Pipeline/blob/4b4d246830788c04278608d96e4639d4fa3212ea/AML-Fabric-Compliance-Pipeline/design%20docs/screenshots/03_silver_scd2_history_quarantined.png) | ![Clean Silver](https://github.com/madhumithav30/AML-Fabric-Compliance-Pipeline/blob/4b4d246830788c04278608d96e4639d4fa3212ea/AML-Fabric-Compliance-Pipeline/design%20docs/screenshots/04_silver_without_badrec.png) |
| *Tracking changes while isolating bad records.* | *High-quality data ready for downstream analytics.* |

---

### 3. Gold Analytics & Regional Governance (RLS)
The final stage identifies high-risk "Smurfing" patterns. Row-Level Security ensures that the final result is dynamically filtered based on the auditor's assigned jurisdiction.

**Detected PMLA Fraud Alerts (Gold):**
![Gold Alerts](https://github.com/madhumithav30/AML-Fabric-Compliance-Pipeline/blob/4b4d246830788c04278608d96e4639d4fa3212ea/AML-Fabric-Compliance-Pipeline/design%20docs/screenshots/05_gold_fraud_alerts.png)

**Security Enforcement (Auditor Mapping & Result):**
| Auditor-District Mapping | Filtered Result (The "Proof") |
| :--- | :--- |
| ![Security Mapping](https://github.com/madhumithav30/AML-Fabric-Compliance-Pipeline/blob/4b4d246830788c04278608d96e4639d4fa3212ea/AML-Fabric-Compliance-Pipeline/design%20docs/screenshots/06_security_mapping.png) | ![Resultant Data](https://github.com/madhumithav30/AML-Fabric-Compliance-Pipeline/blob/4b4d246830788c04278608d96e4639d4fa3212ea/AML-Fabric-Compliance-Pipeline/design%20docs/screenshots/07_resultant_data.png) |
| *Entra ID-based access control mapping.* | *The 'Chennai Auditor' view: Filtered Result.* |

---

## Repository Structure
```text
├── notebooks/          # PySpark logic: Ingestion, SCD2, Fraud Analytics
├── sql/                # RLS Functions and Security Policies (T-SQL)
├── design docs/        # Architecture diagrams and Screenshots
└── README.md           # Documentation
```

## Lets Connect !

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](www.linkedin.com/in/madhumithaviswanathan)

**Role:**  Data Engineer | Microsoft Fabric Certified  
**Focus:** Enterprise Data Architecture & Financial Compliance
