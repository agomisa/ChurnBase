# Project Documentation

## Table of Contents
0. [Links](#0-links)
1. [Overview](#1-overview)  
   1.1 [Mission](#11-mission)  
   1.2 [Business Problem & Objectives](#12-business-problem--objectives)  
   1.3 [Tools & Setup](#13-tools--setup)  
2. [Data](#2-data)  
3. [Architecture & Data Engineering (Spark)](#3-architecture--data-engineering-spark)  
   3.1 [Data Ingestion – AWS Lambda to S3](#31-data-ingestion--aws-lambda-to-s3)  
   3.2 [Bronze Layer – Raw Read from Volumes](#32-bronze-layer--raw-read-from-volumes)  
   3.3 [Silver Layer – Data Cleaning and Quality Checks](#33-silver-layer--data-cleaning-and-quality-checks)  
   3.4 [Gold Layer – Daily Aggregation](#34-gold-layer--daily-aggregation)  
4. [Exploratory Data Analysis](#4-exploratory-data-analysis)  
   4.1 [Engagement KPIs](#41-kpis-computed)  
   4.2 [Data quirks and mitigations](#42-data-quirks-and-mitigations)  
   4.3 [Missing dates for each domain](#43-missing-dates-for-each-domain)  
5. [Feature Engineering](#5-feature-engineering)  
   5.1 [Daily Series Patching (Gap Filling)](#51-daily-series-patching-gap-filling)  
   5.2 [Churn Definition](#52-churn-definition)  
   5.3 [Feature Set](#53-feature-set)  
6. [Preprocessing, Model Training & Hyperparameter Tuning](#6-preprocessing-model-training--hyperparameter-tuning)  
   6.1 [Preprocessing of Categorical and Temporal Features](#61-preprocessing-of-categorical-and-temporal-features)  
   6.2 [Model Selection & Hyperparameter Optimization Strategy](#62-model-selection--hyperparameter-optimization-strategy)  
   6.3 [Final Model Training](#63-final-model-training)  
7. [Model Evaluation](#7-model-evaluation)  
8. [Model Explainability](#8-model-explainability)  
9. [BI Dashboard](#9-bi-dashboard)  


## 0. Links
- [🔗 Interactive BI Report](https://app.powerbi.com/view?r=eyJrIjoiNzI0NGQ0NGQtYWI5Yi00ZTcyLWI4ZTktMmU5NzUxN2M0MTE2IiwidCI6IjNlMDUxM2Q2LTY4ZmEtNDE2ZS04ZGUxLTZjNWNkYzMxOWZmYSIsImMiOjR9)
- [📂 Project Repository](https://github.com/agomisa/ChurnBase)

## 1. Overview


### 1.1 Mission
The mission of this project is to design and implement an end-to-end data pipeline that ingests, processes, and analyzes **Wikimedia project view statistics**.  
The pipeline ensures reliable ingestion, applies robust data quality checks, and delivers clean, aggregated datasets for downstream analytics and reporting.

### 1.2 Business Problem & Objectives
Wikimedia pageview data provides valuable insights into content consumption patterns across domains and time. However, raw data is published in **semi-structured text files** and requires automated ingestion, cleaning, and aggregation before it can be used effectively.

**Objectives:**
- Automate the retrieval of Wikimedia project view data from the public dumps.
- Store ingested data securely and in a structured format for long-term analysis.
- Apply data quality validations to ensure analytical reliability.
- Aggregate daily view counts per domain to facilitate trend analysis and reporting.

### 1.3 Tools & Setup
- **AWS Lambda** – Serverless ingestion of raw files from Wikimedia dumps into Amazon S3.
- **Amazon S3** – Data lake storage for ingested raw files.
- **Databricks & Delta Live Tables (DLT)** – Stream processing, data cleaning, and aggregation.
- **Apache Spark** – Distributed data processing engine.
- **Python** – For Lambda and Spark transformation logic.


## 2. Data
The primary dataset consists of Wikimedia `projectviews` files, published daily on the Wikimedia dumps site.

**Each record contains:**
- `domain_code` – Wikimedia project domain identifier (e.g., `en.wikipedia`).
- `count_views` – Number of page views recorded.
- `timestamp` – Derived from the file name, representing the date and hour of the data.

**Format:**
- Files are ingested in plain text format, organized by year and month.
- The raw dataset is incrementally processed into **bronze**, **silver**, and **gold** layers to ensure both historical retention and analytical readiness.


## 3. Architecture & Data Engineering (Spark)
This project has two main components:

1. **External Ingestion Stage** – An AWS Lambda function (running outside Databricks) downloads raw Wikimedia `projectviews` files from the public dumps site and uploads them to **Amazon S3**.  
   This step is **decoupled** from the Databricks pipeline so it can be scheduled and scaled independently.

2. **Databricks Processing Stage** – A multi-layer **Delta Live Tables (DLT)** pipeline reads the ingested files from Unity Catalog Volumes, applies transformations, performs data quality checks, and writes curated datasets.  

**Note:** All DLT outputs are stored in the Databricks metastore, but are intended to be persisted in a Data Warehouse (e.g., **Amazon Redshift**) for long-term storage and analytical workloads.

The Delta Live Tables flow acts as an **orchestrated data pipeline** — Databricks automatically manages table dependencies, determines execution order, handles incremental processing, and applies defined data quality checks from the Bronze to the Gold layers.

![Schema Architecture](images\Architecture.png) 

### 3.1 Data Ingestion – AWS Lambda to S3
- Retrieves daily Wikimedia `projectviews` files from the official dumps URL.
- Filters out compressed `.gz` files.
- Uploads the raw data to an **Amazon S3** bucket (`dest-wikimedia`) preserving the year-month folder structure.


### 3.2 Bronze Layer – Raw Read from Volumes
- Reads raw `projectviews` text files from `/Volumes/projectviews/bronze/raw_files` as a **streaming source**.
- Filters empty records.
- Adds metadata columns `_ingested_at` and `_source_path` for traceability.


### 3.3 Silver Layer – Data Cleaning and Quality Checks
- Parses raw bronze data to extract:
  - `domain_code`
  - `count_views`
  - `event_timestamp`
- Stores only valid records in `silver_projectviews_clean`.
- Invalid records are sent to `silver_projectviews_quarantine` with a `dq_reason` column.
- A `silver_dq_reports` table is generated for each run containing JSON-formatted data quality metrics.


### 3.4 Gold Layer – Daily Aggregation
- `gold_daily_projectviews` aggregates cleaned silver-layer data at a daily level.
- For each `domain_code` and `event_date`, calculates `SUM(count_views)`.
- Partitioned by `event_date` for optimized queries.


## 4. Exploratory Data Analysis

Quantify engagement and inspect the raw signal before modeling to inform feature ideas and retention hypotheses.

### 4.1 Engagement KPIs

- **DAU / WAU / MAU (Views)**  
  - `DAU_views(t) = Σ_domain count_views(domain, t)`  
  - `WAU_views(w) = Σ_domain count_views(domain, week(t))`
  - `MAU_views(m) = Σ_domain count_views(domain, month(t))`  
  Implemented in `plot_engagement_kpis(df)` using Spark group-bys and Pandas plots.

- **Session-length (Proxy)**  
  
  The dataset contains only domain-level pageviews (no user or session identifiers).  
  To approximate engagement duration, a **streak length** metric was defined: consecutive active-day stretches by domain.  

  - `streak_len(domain, t)` = length of the current run of consecutive days where `count_views > 0`.  

  This metric serves as a proxy for domain persistence, indicating the durability of engagement without requiring individual session data.  
  In this dataset, all domains were active every day, so streaks are never broken - both the average and median streak lengths increase linearly from 1 to the total number of observed days.

- **Content Diversity (Domain Diversity)**
  
  Per day, is measure how concentrated views are across domains using the **Herfindahl–Hirschman Index (HHI)** and its complement **Diversity = 1 − HHI**:

  - `p_d = count_views(domain) / Σ_domain count_views`
  - `HHI = Σ_d p_d²`
  
  HHI close to 1 → traffic is dominated by a few domains.
  
  HHI close to 0 → traffic is evenly distributed.
  
  Diversity close to 1 → traffic is spread widely across domains.

  Conclusion: 

  In this dataset, HHI ~ 0.13–0.16 and Diversity ~ 0.84–0.86 suggest that traffic is relatively evenly distributed across domains.


### 4.2 Data quirks and mitigations

  **Seasonality** (weekday effects).  
   - *Observation:* weekday pattern in DAU; weekends slightly different.  
   - *Mitigation:* add calendar features (`dayofweek`, `is_weekend`, `month`, `quarter`) and evaluate per-weekday baselines.

  **Sparsity / completeness.**  
   - *Observation:* the function checks **zero-view days** and **missing days** by constructing the full date range.  
   - *Mitigation:* log and monitor gaps; exclude missing dates from rate-of-change calculations; use rolling features (e.g., `avg_views_past_3d`) to smooth sparse series.

  **Spikes / outliers.**  
   - *Observation:* detect spikes via `DAU > mean + 3·std`.  
   - *Mitigation:* robustify features with rolling averages/medians; consider winsorizing for feature creation (not labels).

  **Leakage risk.**  
   - *Observation:* forward-looking columns (`views_plus_*`, `min_views_future`) exist for **labeling**.  
   - *Mitigation:* removed from the training feature set, only historical features used for modeling.


![Risk Score Distribution](images\engagement_kpis.png)

### 4.3 Missing dates for each domain

During exploratory analysis, it was identified that some `domain_code` time series contained gaps, missing calendar days between the first and last observed dates.  
These missing dates were detected by comparing each `event_date` to the lagged previous date per domain.


## 5. Feature Engineering

### 5.1 Daily Series Patching (Gap Filling)

The feature engineering pipeline includes a **gap-filling step** to ensure each domain has a complete, contiguous daily time series.  
This process involves:

- Generating the full date range for each `domain_code`.
- Filling missing days with `count_views = 0`.
- Adding a new boolean feature:
  - `is_missing` - indicates whether the record was synthetically generated due to a gap.

**Importance:**
- **Stabilizes rolling features** - rolling averages (e.g., `avg_views_past_3d`) remain consistent across time.
- **Prevents label bias** - avoids misinterpreting missing dates as actual drops in engagement.

### 5.1 Churn Definition

A domain is **churned** on day `t` if, within the next 7 days, its daily views drop to ≤ 30% of the average from the previous 3 days.

### 5.2 Feature Set

The final feature set was engineered from the historical pageview time series at the domain level, using rolling window calculations and temporal lags. The key features include:

- **avg_views_past_3d**: Rolling average of the previous 3 day's view counts, excluding the current day. This metric smooths short-term fluctuations and provides a stable baseline for detecting sudden drops.

- **count_views (current day)**: The raw number of page views for the current day, used as a direct measure of engagement.

- **views_plus_1 … views_plus_n**: Forward-looking features representing the number of views in each of the next *n* days (only used during churn label generation, not as training features, to prevent data leakage).

- **min_views_future**: The minimum number of daily views observed in the *n*-day future window. Used exclusively for churn definition.

- **threshold**: Dynamic churn threshold calculated as `avg_views_past_3d × threshold_factor` (where `threshold_factor` was set to 0.3). This value adapts to each domain's recent performance level.

- **churn**: Binary target variable, set to 1 if `min_views_future ≤ threshold`, indicating a significant engagement drop in the near future.

Additional transformations for the model's input:
- **Date-derived features** (year, month, day, day_of_week, quarter, weekend flag) were later extracted from the `event_date` to capture temporal patterns.

To avoid target leakage, all future-looking columns (`views_plus_*`, `min_views_future`, and `threshold`) were removed before training, leaving only historical and non-leaking features in the final model.

### 6. Preprocessing, Model Training & Hyperparameter Tuning

#### 6.1 Preprocessing of Categorical and Temporal Features:
The preprocessing pipeline was designed to transform raw tabular data into a model-ready format while ensuring **data integrity** and **leakage prevention**.

Key steps:
- **Leakage prevention**: All columns containing future information (`views_plus_1` … `views_plus_7`, `min_views_future`) were removed to ensure the model is trained solely on historically available data at prediction time.
- **Temporal decomposition**: Datetime columns were converted into discrete components — year, month, day, day of week, quarter, and a weekend indicator — enabling the model to capture periodic and seasonal effects.
- **Categorical encoding**: Nominal variables, such as `domain_code`, were transformed into numerical representations using label encoding, preserving class distinctions without imposing ordinal relationships.
- **Preservation of interpretability**: For `domain_code`, an inverse mapping was stored to recover original identifiers during model explainability and reporting phases.


#### 6.2 Model Selection & Hyperparameter Optimization Strategy
LightGBM (Light Gradient Boosting Machine) was selected as the classification algorithm due to its:
- Computational efficiency on large-scale, high-dimensional datasets.
- Ability to model non-linear interactions without explicit feature transformations
- Robustness to missing values and categorical variables.
- Compatibility with model-agnostic explainability tools (e.g., SHAP, permutation importance).
- Proven effectiveness in imbalanced classification problems through parameters such as `scale_pos_weight`.


A systematic grid search was conducted in combination with **Stratified K-Fold Cross-Validation** (k=5) to ensure robust performance estimation across different dataset partitions.

Search space parameters included:
- `learning_rate` (gradient step size)
- `num_leaves` (tree complexity)
- `max_depth` (maximum tree depth)
- `min_data_in_leaf` (minimum samples per leaf)
- `feature_fraction` (fraction of features considered per iteration)
- `bagging_fraction` and `bagging_freq` (row sampling for regularization)

For **class imbalance mitigation**, the `scale_pos_weight` parameter was set as the ratio of negative to positive samples in the training set, ensuring balanced gradient updates.

The model with the highest **average precision (AP)** across folds was selected, as AP is a more informative metric than ROC-AUC for imbalanced binary classification tasks.


#### 6.3 Final Model Training
The best hyperparameters obtained from the search were used to train the final LightGBM model on the **entire training dataset**. The model outputs:
- **Risk scores** — predicted churn probabilities
- **Binary predictions** — based on a tuned decision threshold to maximize F1-score



## 7. Model Evaluation 

After introducing the `scale_pos_weight` parameter in LightGBM - computed as the ratio of negative to positive samples - the model performance metrics and score distributions changed significantly. This adjustment compensates for class imbalance by assigning more weight to the minority (churn) class during training.

### Risk Score Distribution
The updated risk score histogram shows a more uniform distribution across the probability range, compared to the previous skew toward lower scores. The chosen decision threshold (**0.668**) now lies in the upper range of the probability spectrum, reflecting the model’s recalibration toward detecting more positive churn cases.

![Risk Score Distribution](images\risk_score.png)

### Confusion Matrix

At the tuned threshold:
- **True Negatives (TN):** 46.048  
- **False Positives (FP):** 2.878  
- **False Negatives (FN):** 2.474  
- **True Positives (TP):** 3.939  

This indicates a better balance between precision and recall for the positive class, with reduced bias toward predicting non-churn.

![Confusion Matrix](images\confusion_matrix.png)

### ROC Curve & AUC
The ROC curve remains strong, with an **AUC of 0.915**. This indicates that the model effectively separates churn and non-churn cases across all possible thresholds, performing significantly better than random guessing (orange diagonal line).

![ROC Curve](images\AUC.png)

### Performance Summary

| Metric           | Class 0 (Non-Churn)         | Class 1 (Churn)            |
|------------------|-----------------------------|-----------------------------|
| Precision        | High, due to reduced FP     | Improved, benefiting from higher TP |
| Recall           | Slightly lower than before  | Higher, benefiting from reweighting |
| F1-Score         | Balanced improvement        | Balanced improvement        |

The recalibration via `scale_pos_weight` has enhanced the model’s ability to identify churned domains without excessively sacrificing precision, making it more suitable for use cases where catching potential churn is critical.

## 8. Model Explainability

To interpret the LightGBM model’s predictions, we used **SHAP (SHapley Additive exPlanations)**, a model-agnostic explainability framework. SHAP values quantify the contribution of each feature to the model’s prediction for each instance, enabling both **global** (feature importance) and **local** (per-observation) interpretability.

### Global Feature Importance
The **mean absolute SHAP value** plot shows that the top predictive features for churn risk are:

1. **`count_views`** – The strongest predictor, indicating that the total number of views on a given day is highly correlated with churn risk. Low values of this variable strongly increase churn probability.
2. **`event_date_day`** – The day of the month influences churn likelihood, possibly reflecting periodic user behavior.
3. **`avg_views_past_3d`** – The rolling average of views over the previous three days provides temporal context, capturing short-term engagement trends.
4. **`domain_code`** – Encoded domain identifier, capturing differences in user patterns across domains.
5. **`threshold`** – The computed churn threshold (average views × factor) helps separate low-activity domains from engaged ones.
6. Temporal indicators such as **`event_date_dayofweek`** and **`event_date_is_weekend`** have smaller but non-negligible contributions, reflecting different usage habits on weekends or specific weekdays.

![Global Feature](images\shap_summary.png)

### Feature Impact on Predictions
The **SHAP beeswarm plot** reveals how feature values influence churn predictions:
- **High `count_views` (red)** pushes predictions toward **non-churn** (negative SHAP values), while **low values (blue)** increase churn probability.
- **High `avg_views_past_3d`** similarly lowers churn risk, suggesting recent engagement is a strong retention signal.
- For **`event_date_day`**, certain days of the month appear associated with increased churn, possibly reflecting billing cycles or content release schedules.
- The **`threshold`** variable behaves as expected: lower thresholds (indicating overall low engagement) push churn probability higher.
- Temporal categorical features (**`dayofweek`**, **`is_weekend`**) show asymmetric effects, with weekends generally slightly reducing churn risk.

![Feature Impact](images\shap_summary2.png)

### Business Insight
These explainability results suggest that **recent engagement (current and past few days)** is the dominant driver of churn prediction, followed by periodic and domain-specific patterns.  
This provides actionable intelligence: retention efforts should focus on domains with **sharp recent activity drops**, especially during historically high-risk days of the month.

## 9. BI Dashboard

An interactive retention and churn analysis dashboard was built in Power BI to serve as the narrative layer for stakeholder presentations.
The dashboard consolidates the main analytical outputs from the churn prediction pipeline into intuitive visual components.

### Page 1  – Churn Overview
This view provides an End-of-Period (EOP) KPI summary, where all key figures are calculated at the close of the selected analysis period.  

- **Active Domains**: Total domains at the end of the analysis period.
- **Retained Domains**: Domains that remained active after the churn window.
- **Churned Domains**: Domains that fell below the churn threshold.
- **Retention Rate** and **Churn Rate** KPIs are calculated directly from these counts.

- **Time Series KPIs**  
  - **Retention Rate**: Shows day-to-day fluctuations in retention, highlighting seasonal or event-driven patterns.

  - **Actual Churn Rate**: Shows day-to-day fluctuations in churn, highlighting seasonal or event-driven patterns.

- **Cohort Heatmap (Join Day × Survival)**  
  Displays retention over time for domains grouped by their initial observation day (rather than month, due to the dataset’s limited time span).  
  Each cell represents the percentage of domains from the same daily cohort that remain active after a given number of days.

**Date Filters**  
Interactive controls allow narrowing the analysis to any specific time range, dynamically updating all KPIs and visualizations.

### Page 2 – Risk Score Analysis
Incorporates the predictive model outputs to compare estimated churn against actual observed churn:
- **Actual Churn vs Predicted Churn**: time series comparing real churn to the model’s predicted churn, enabling evaluation of alignment and deviations.

- **Risk Score Distribution**: histogram showing the distribution of predicted churn probabilities (`risk_score`) for all domains.  
The x-axis represents the churn probability (binned between 0 and 1), and the y-axis indicates the count of domains in each bin.  
The left-skewed shape, with a concentration of domains in lower probability bins, reflects the model’s tendency to assign low churn risk to the majority of domains, while still capturing a meaningful tail of higher-risk cases.


### Analysis of Results
- The EOP-based cards provide a consistent snapshot of performance at the end of the selected time frame, ensuring comparability across periods.  
- Retention trends show overall stability during the observed timeframe, with limited variation between days.  
- Churn rate peaks on certain days may signal event-driven changes in engagement, meriting further investigation.  
- The predictive model follows the overall churn trend, though peaks in predicted risk can be higher than observed churn, suggesting some overestimation in high-risk scenarios.  
- The distribution of risk scores is skewed toward lower risk, with a gradual tail toward higher probabilities, supporting targeted interventions on the most vulnerable segments.

