# ChurnBase

📌 Overview
This repository contains my solution to Data Scientist Take-Home Exercise 1, simulating a Staff-level Data Scientist role at Acme Streaming.
The task focuses on transforming raw behavioral logs into:

  - Trusted analytical insights
  - A predictive churn model
  - An executive-level data story through an interactive dashboard

The goal is to address early subscriber churn (first 90 days) by connecting the dots:
ingest → explore → model → explain → recommend

🎯 Business Problem & Goals
Recent analysis shows early churn exceeding forecasts. The project aims to:

- Measure churn magnitude and seasonal patterns
- Predict subscribers at risk of churning in the next month
- Explain the “why” behind churn in terms accessible to non-technical stakeholders

🛠 Tools & Environment
- Languages: Python 
- Framework: Apache Spark 3.4+ 
- AWS S3
- Databricks Free Edition
- Data Source: Wikimedia Pageviews - January 2025 

📂 Project Structure

```
├── notebooks/                  # Data engineering, analysis, modelling
├── src/                        # Helper functions, ETL scripts
├── dashboards/                 # Superset / Metabase / Redash dashboards
├── tests/                      # Unit tests
├── ENVIRONMENT.md              # Setup instructions
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

📋 Assignment Steps
1. Data Engineering (Spark)
Ingest raw data into bronze → silver → gold layers (Parquet or Delta)
Document schema, partitioning, and data-quality checks (late records, outliers, etc.)

2. Exploratory Analysis
Compute engagement KPIs: DAU, WAU, MAU, session length, content diversity
Identify at least three data quirks (e.g., seasonality, sparsity, leakage) and describe mitigation

3. Feature Engineering & Modelling
Define churn as a binary classification problem (clear churn window)
Train and justify one chosen algorithm
Perform hyper-parameter tuning (CV, Hyperopt, or Bayesian search)
Evaluate using ROC-AUC and PR-AUC
Use SHAP or permutation importance for explainability

4. BI Dashboard & Data Story
Build in Superset, Metabase, or Redash (dashboard = narrative)
Include: churn funnel, cohort heatmap, filtering, explainable model outputs
No slides — dashboard is the presentation

📦 Deliverables
Git repository with:
- Code & notebooks
- ENVIRONMENT.md for reproducibility
- Interactive Power BI dashboard 
- Analytical report 
