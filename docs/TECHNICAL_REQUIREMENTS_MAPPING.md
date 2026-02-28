# Technical Requirements Mapping

## 1. PySpark Data Engineering
- SparkSession tuning: adaptive execution, Kryo serializer, shuffle tuning, broadcast threshold, event log.
- Storage format: Parquet output for processed facts/features.
- Data validation: required-column checks, schema casting, null handling.
- Lineage columns: `source_file`, `ingest_ts`, `run_id`, `lineage_step`.
- Caching strategy: `persist(MEMORY_AND_DISK)` + `unpersist()` around reusable frames.
- DataFrame-first implementation for Catalyst optimization.

## 2. Distributed ML and Evaluation
- Spark MLlib models: Logistic Regression, Random Forest, GBT Classifier.
- scikit-learn baselines: logistic regression, random forest, gradient boosting.
- Preprocessing pipeline: imputation + categorical indexing/encoding + feature assembly.
- CV setup: 3-fold CrossValidator with bounded hyperparameter grids.
- Metrics: validation/test AUC, validation/test accuracy, training time.
- Statistical robustness: bootstrap 95% CI for AUC on test predictions.

## 3. Business and Scalability
- Business metrics: TP/FP/TN/FN, precision, recall, F1, expected profit proxy.
- Strong scaling: fixed workload, increased resources.
- Weak scaling: scaled workload with resources.
- Cost model: estimated USD/core-hour and cost per 10k rows.

## 4. Tableau Outputs
- Dashboard 1: data quality and segment monitoring.
- Dashboard 2: model comparison and runtime.
- Dashboard 3: business KPI and recommendation story.
- Dashboard 4: scalability/cost tradeoff.
