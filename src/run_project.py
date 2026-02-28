import json
import os
import pickle
import time
import uuid
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression as SkLogisticRegression
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier as SparkRF
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_LOCAL = os.path.join(ROOT, "data/raw/uci_credit_approval.csv")
RAW_URL_DEFAULT = "https://archive.ics.uci.edu/static/public/27/data.csv"
PROCESSED_DIR = os.path.join(ROOT, "data/processed")
MODELS_DIR = os.path.join(ROOT, "data/models")
OUTPUTS_DIR = os.path.join(ROOT, "data/outputs")
SPARK_UI_DIR = os.path.join(OUTPUTS_DIR, "spark_ui")

NUMERIC_COLS = ["A2", "A3", "A8", "A11", "A14", "A15"]
CATEGORICAL_COLS = ["A1", "A4", "A5", "A6", "A7", "A9", "A10", "A12", "A13"]
REQUIRED_COLS = NUMERIC_COLS + CATEGORICAL_COLS + ["A16"]


@dataclass
class RunConfig:
    shuffle_partitions: int = 8
    default_parallelism: int = 8
    cv_parallelism: int = 2


def ensure_dirs():
    for d in [PROCESSED_DIR, MODELS_DIR, OUTPUTS_DIR, SPARK_UI_DIR, os.path.join(MODELS_DIR, "checkpoints")]:
        os.makedirs(d, exist_ok=True)


def create_spark(cfg: RunConfig):
    return (
        SparkSession.builder.appName("UCILoanCreditSimple")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.default.parallelism", str(cfg.default_parallelism))
        .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        .config("spark.sql.broadcastTimeout", "600")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", SPARK_UI_DIR)
        .getOrCreate()
    )


def load_loan_pdf():
    # Matches your requested style: pd.read_csv(url) then spark.createDataFrame(pdf).
    url = os.environ.get("LOAN_DATA_URL", RAW_URL_DEFAULT)
    try:
        return pd.read_csv(url, na_values=["?"])
    except Exception:
        if os.path.exists(RAW_LOCAL):
            return pd.read_csv(RAW_LOCAL, na_values=["?"])
        raise


def ingest_validate_with_lineage(spark, run_id: str):
    pdf = load_loan_pdf()
    raw = spark.createDataFrame(pdf)

    missing_cols = [c for c in REQUIRED_COLS if c not in raw.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    clean = raw
    for c in NUMERIC_COLS:
        clean = clean.withColumn(c, F.col(c).cast("double"))

    clean = (
        clean.withColumn("target", F.when(F.col("A16") == "+", F.lit(1)).otherwise(F.lit(0)).cast("int"))
        .withColumn("row_id", F.monotonically_increasing_id())
        .withColumn("row_missing_fields", sum(F.when(F.col(c).isNull(), 1).otherwise(0) for c in NUMERIC_COLS + CATEGORICAL_COLS))
        .withColumn("invalid_record", F.col("target").isNull())
        .withColumn("source_file", F.lit(url if (url := os.environ.get("LOAN_DATA_URL")) else RAW_URL_DEFAULT))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("target_bucket", F.col("target"))
    )

    clean.write.mode("overwrite").partitionBy("target_bucket").parquet(os.path.join(PROCESSED_DIR, "fact_loans"))

    quality = clean.agg(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("invalid_record"), 1).otherwise(0)).alias("invalid_rows"),
        F.avg("row_missing_fields").alias("avg_missing_fields_per_row"),
        F.sum(F.when(F.col("target") == 1, 1).otherwise(0)).alias("approved_rows"),
        F.sum(F.when(F.col("target") == 0, 1).otherwise(0)).alias("rejected_rows"),
    )
    quality.write.mode("overwrite").json(os.path.join(OUTPUTS_DIR, "quality_summary_json"))

    return clean


def distributed_processing(fact_df):
    fact_df.persist(StorageLevel.MEMORY_AND_DISK)

    result = (
        fact_df.withColumn("debt_income_ratio_proxy", F.col("A3") / (F.col("A2") + F.lit(1.0)))
        .withColumn("income_stability_proxy", F.log1p(F.coalesce(F.col("A15"), F.lit(0.0))))
        .withColumn("has_credit_history_flag", F.when(F.col("A9") == "t", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("high_credit_limit_flag", F.when(F.coalesce(F.col("A14"), F.lit(0.0)) >= 200.0, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("lineage_step", F.lit("distributed_processing"))
        .repartition(8, "target_bucket")
    )

    result.write.mode("overwrite").partitionBy("target_bucket").parquet(os.path.join(PROCESSED_DIR, "loan_features"))
    fact_df.unpersist()
    return result


def stratified_split(df, seed: int = 42, train_ratio: float = 0.7, val_ratio: float = 0.15):
    threshold = train_ratio + val_ratio

    def _split_label(label: int, label_seed: int):
        labeled = df.filter(F.col("target") == F.lit(label)).withColumn("_rand", F.rand(label_seed))
        train_part = labeled.filter(F.col("_rand") < F.lit(train_ratio)).drop("_rand")
        val_part = labeled.filter((F.col("_rand") >= F.lit(train_ratio)) & (F.col("_rand") < F.lit(threshold))).drop("_rand")
        test_part = labeled.filter(F.col("_rand") >= F.lit(threshold)).drop("_rand")
        return train_part, val_part, test_part

    train_0, val_0, test_0 = _split_label(0, seed + 1)
    train_1, val_1, test_1 = _split_label(1, seed + 2)

    train = train_0.unionByName(train_1)
    val = val_0.unionByName(val_1)
    test = test_0.unionByName(test_1)
    return train, val, test


def has_both_classes(df, label_col: str = "target"):
    return df.select(label_col).distinct().limit(2).count() == 2


def binary_accuracy(pred_df):
    return float(pred_df.select(F.avg(F.when(F.col("prediction") == F.col("target"), F.lit(1.0)).otherwise(F.lit(0.0))).alias("acc")).first()["acc"])


def with_cv_fold(train_df):
    w = Window.partitionBy("target").orderBy(F.rand(11))
    return train_df.withColumn("cv_fold", (F.row_number().over(w) % F.lit(3)).cast("int"))


def build_preprocessing_stages():
    imputed_numeric_cols = [f"{c}_imp" for c in NUMERIC_COLS + ["debt_income_ratio_proxy", "income_stability_proxy", "has_credit_history_flag", "high_credit_limit_flag"]]
    all_numeric_cols = NUMERIC_COLS + ["debt_income_ratio_proxy", "income_stability_proxy", "has_credit_history_flag", "high_credit_limit_flag"]
    cat_idx_cols = [f"{c}_idx" for c in CATEGORICAL_COLS]
    cat_oh_cols = [f"{c}_oh" for c in CATEGORICAL_COLS]

    imputer = Imputer(inputCols=all_numeric_cols, outputCols=imputed_numeric_cols)
    indexers = [StringIndexer(inputCol=c, outputCol=i, handleInvalid="keep") for c, i in zip(CATEGORICAL_COLS, cat_idx_cols)]
    encoder = OneHotEncoder(inputCols=cat_idx_cols, outputCols=cat_oh_cols, handleInvalid="keep")
    assembler = VectorAssembler(inputCols=imputed_numeric_cols + cat_oh_cols, outputCol="features")

    return [imputer] + indexers + [encoder, assembler]


def fit_spark_models(spark, train_df, val_df, test_df, cfg: RunConfig):
    spark.sparkContext.setCheckpointDir(os.path.join(MODELS_DIR, "checkpoints"))

    train_df = with_cv_fold(train_df).checkpoint(eager=True)
    evaluator = BinaryClassificationEvaluator(labelCol="target", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    prep_stages = build_preprocessing_stages()

    algos = {
        "logistic_regression": LogisticRegression(labelCol="target", featuresCol="features", maxIter=120),
        "random_forest": SparkRF(labelCol="target", featuresCol="features", seed=42),
        "gbt_classifier": GBTClassifier(labelCol="target", featuresCol="features", seed=42, maxIter=40),
    }

    grids = {
        "logistic_regression": ParamGridBuilder().addGrid(algos["logistic_regression"].regParam, [0.0, 0.1]).addGrid(
            algos["logistic_regression"].elasticNetParam, [0.0, 0.5]
        ).build(),
        "random_forest": ParamGridBuilder().addGrid(algos["random_forest"].numTrees, [40, 80]).addGrid(
            algos["random_forest"].maxDepth, [4, 7]
        ).build(),
        "gbt_classifier": ParamGridBuilder().addGrid(algos["gbt_classifier"].maxDepth, [2, 3]).addGrid(
            algos["gbt_classifier"].stepSize, [0.05, 0.1]
        ).build(),
    }

    model_rows = []
    best_models = {}

    for name, algo in algos.items():
        pipe = Pipeline(stages=prep_stages + [algo])
        cv = CrossValidator(
            estimator=pipe,
            estimatorParamMaps=grids[name],
            evaluator=evaluator,
            numFolds=3,
            parallelism=cfg.cv_parallelism,
            foldCol="cv_fold",
        )

        t0 = time.time()
        cv_model = cv.fit(train_df)
        train_secs = time.time() - t0

        pred_val = cv_model.transform(val_df)
        pred_test = cv_model.transform(test_df)
        val_auc = evaluator.evaluate(pred_val) if has_both_classes(val_df) else float("nan")
        test_auc = evaluator.evaluate(pred_test) if has_both_classes(test_df) else float("nan")
        val_acc = binary_accuracy(pred_val)
        test_acc = binary_accuracy(pred_test)

        model_rel_path = f"data/models/spark_{name}"
        model_abs_path = os.path.join(ROOT, model_rel_path)
        cv_model.bestModel.write().overwrite().save(model_abs_path)

        model_rows.append((name, float(val_auc), float(test_auc), float(val_acc), float(test_acc), float(train_secs), model_rel_path))
        best_models[name] = cv_model.bestModel

    metrics_df = spark.createDataFrame(
        model_rows,
        ["model", "validation_auc", "test_auc", "validation_accuracy", "test_accuracy", "training_seconds", "model_path"],
    )
    return best_models, metrics_df


def safe_auc(y_true, y_prob):
    return float(roc_auc_score(y_true, y_prob)) if len(np.unique(y_true)) > 1 else float("nan")


def sklearn_baselines(train_df, test_df):
    keep_cols = NUMERIC_COLS + CATEGORICAL_COLS + ["target"]
    train_pd = train_df.select(*keep_cols).toPandas()
    test_pd = test_df.select(*keep_cols).toPandas()

    y_train = train_pd["target"].values
    y_test = test_pd["target"].values

    x_train = train_pd.drop(columns=["target"]).copy()
    x_test = test_pd.drop(columns=["target"]).copy()

    for c in NUMERIC_COLS:
        med = x_train[c].median()
        x_train[c] = x_train[c].fillna(med)
        x_test[c] = x_test[c].fillna(med)

    for c in CATEGORICAL_COLS:
        mode = x_train[c].mode().iloc[0] if not x_train[c].mode().empty else "missing"
        x_train[c] = x_train[c].fillna(mode).astype(str)
        x_test[c] = x_test[c].fillna(mode).astype(str)

    x_train = pd.get_dummies(x_train, columns=CATEGORICAL_COLS)
    x_test = pd.get_dummies(x_test, columns=CATEGORICAL_COLS)
    x_test = x_test.reindex(columns=x_train.columns, fill_value=0)

    models = {
        "sk_logistic": SkLogisticRegression(max_iter=300),
        "sk_random_forest": RandomForestClassifier(n_estimators=120, random_state=42),
        "sk_gradient_boosting": GradientBoostingClassifier(random_state=42),
    }

    rows = []
    for name, model in models.items():
        t0 = time.time()
        model.fit(x_train.values, y_train)
        train_secs = time.time() - t0
        probs = model.predict_proba(x_test.values)[:, 1]
        preds = (probs >= 0.5).astype(int)
        rows.append((name, safe_auc(y_test, probs), float((preds == y_test).mean()), float(train_secs)))

        with open(os.path.join(MODELS_DIR, f"{name}.pkl"), "wb") as f:
            pickle.dump(model, f)

    return pd.DataFrame(rows, columns=["model", "test_auc", "test_accuracy", "training_seconds"])


def bootstrap_ci(pred_pdf: pd.DataFrame, n_iter: int = 400):
    y_true = pred_pdf["target"].values
    y_prob = pred_pdf["probability"].values
    stats = []
    rng = np.random.default_rng(42)
    for _ in range(n_iter):
        idx = rng.choice(len(y_true), size=len(y_true), replace=True)
        if len(np.unique(y_true[idx])) < 2:
            continue
        stats.append(roc_auc_score(y_true[idx], y_prob[idx]))
    if not stats:
        return (float("nan"), float("nan"), float("nan"))
    lo, mid, hi = np.percentile(stats, [2.5, 50, 97.5])
    return float(lo), float(mid), float(hi)


def business_metrics(pred_pdf: pd.DataFrame):
    y_true = pred_pdf["target"].values
    y_pred = pred_pdf["prediction"].values.astype(int)
    tp = int(((y_true == 1) & (y_pred == 1)).sum())
    fp = int(((y_true == 0) & (y_pred == 1)).sum())
    tn = int(((y_true == 0) & (y_pred == 0)).sum())
    fn = int(((y_true == 1) & (y_pred == 0)).sum())

    # Simple lending economics proxy.
    expected_profit = tp * 3000 - fp * 5000 - fn * 1000
    return {
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "expected_profit": float(expected_profit),
    }


def run_scalability_experiments(spark, base_df):
    rows = []

    fixed = base_df.crossJoin(spark.range(200).withColumnRenamed("id", "k"))
    for partitions in [2, 4, 8, 16]:
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        t0 = time.time()
        _ = fixed.repartition(partitions).groupBy("target").agg(F.avg("A15").alias("avg_income_proxy")).count()
        elapsed = time.time() - t0
        rows.append(("strong", partitions, fixed.count(), elapsed, None))

    weak_settings = [(2, 120), (4, 240), (8, 480), (16, 960)]
    for partitions, mult in weak_settings:
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
        data = base_df.crossJoin(spark.range(mult).withColumnRenamed("id", "k"))
        t0 = time.time()
        _ = data.repartition(partitions).groupBy("target").count().count()
        elapsed = time.time() - t0
        rows.append(("weak", partitions, data.count(), elapsed, float(data.count() / max(elapsed, 1e-6))))

    scale_pdf = pd.DataFrame(rows, columns=["scaling_type", "resource_level", "rows_processed", "elapsed_seconds", "throughput_or_speedup"])

    strong = scale_pdf[scale_pdf.scaling_type == "strong"].copy()
    base_time = strong[strong.resource_level == strong.resource_level.min()]["elapsed_seconds"].iloc[0]
    scale_pdf.loc[scale_pdf.scaling_type == "strong", "throughput_or_speedup"] = base_time / strong["elapsed_seconds"].values

    cost_per_core_hour = 0.20
    scale_pdf["estimated_cost_usd"] = (scale_pdf["resource_level"] * scale_pdf["elapsed_seconds"] / 3600.0) * cost_per_core_hour
    scale_pdf["cost_per_10k_rows_usd"] = scale_pdf["estimated_cost_usd"] / (scale_pdf["rows_processed"] / 10000.0)

    return scale_pdf


def generate_tableau_outputs(processed_df, spark_metrics_df, sklearn_metrics_pdf, best_pred_pdf, business, scaling_pdf):
    dq_df = (
        processed_df.groupBy("A13", "target")
        .agg(
            F.count("*").alias("records"),
            F.avg("row_missing_fields").alias("avg_missing_fields"),
            F.avg("A15").alias("avg_income_proxy"),
            F.avg("A3").alias("avg_debt_proxy"),
        )
        .orderBy("A13", "target")
    )
    dq_df.toPandas().to_csv(os.path.join(OUTPUTS_DIR, "dashboard1_data_quality_pipeline_monitoring.csv"), index=False)

    model_perf = spark_metrics_df.toPandas()[["model", "validation_auc", "test_auc", "validation_accuracy", "test_accuracy", "training_seconds"]]
    model_perf = model_perf.rename(columns={"validation_auc": "validation_metric", "test_auc": "test_metric"})

    sk_perf = sklearn_metrics_pdf.rename(columns={"test_auc": "test_metric"})
    sk_perf["validation_metric"] = np.nan
    sk_perf["validation_accuracy"] = np.nan

    full_perf = pd.concat(
        [model_perf, sk_perf[["model", "validation_metric", "test_metric", "validation_accuracy", "test_accuracy", "training_seconds"]]],
        ignore_index=True,
    )
    full_perf.to_csv(os.path.join(OUTPUTS_DIR, "dashboard2_model_performance_feature_importance.csv"), index=False)

    business_df = pd.DataFrame(
        [
            {"metric": "expected_profit", "value": business["expected_profit"]},
            {"metric": "precision", "value": business["precision"]},
            {"metric": "recall", "value": business["recall"]},
            {"metric": "f1", "value": business["f1"]},
            {"metric": "tp", "value": business["tp"]},
            {"metric": "fp", "value": business["fp"]},
            {"metric": "tn", "value": business["tn"]},
            {"metric": "fn", "value": business["fn"]},
        ]
    )
    business_df.to_csv(os.path.join(OUTPUTS_DIR, "dashboard3_business_insights_recommendations.csv"), index=False)

    scaling_pdf.to_csv(os.path.join(OUTPUTS_DIR, "dashboard4_scalability_cost_analysis.csv"), index=False)
    best_pred_pdf.to_csv(os.path.join(OUTPUTS_DIR, "test_predictions_for_storytelling.csv"), index=False)


def write_summary_json(run_id: str, spark_metrics_df, sklearn_metrics_pdf, ci_tuple, business, scaling_pdf):
    summary = {
        "run_id": run_id,
        "dataset": "UCI Credit Approval (loan/credit risk proxy)",
        "spark_models": spark_metrics_df.toPandas().to_dict(orient="records"),
        "sklearn_models": sklearn_metrics_pdf.to_dict(orient="records"),
        "bootstrap_auc_ci_95": {"low": ci_tuple[0], "median": ci_tuple[1], "high": ci_tuple[2]},
        "business_metrics": business,
        "scaling_summary": {
            "strong_scaling_rows": int((scaling_pdf.scaling_type == "strong").sum()),
            "weak_scaling_rows": int((scaling_pdf.scaling_type == "weak").sum()),
            "best_cost_per_10k_rows_usd": float(scaling_pdf["cost_per_10k_rows_usd"].min()),
        },
        "notes": [
            "Pipeline migrated from tiny time-series demo to UCI loan/credit classification data.",
            "Input is loaded with pandas and converted to Spark DataFrame for distributed processing.",
            "Spark UI event logs are written to data/outputs/spark_ui for execution evidence.",
        ],
    }

    with open(os.path.join(OUTPUTS_DIR, "project_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


def main():
    ensure_dirs()
    cfg = RunConfig()
    run_id = str(uuid.uuid4())

    spark = create_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")

    try:
        fact = ingest_validate_with_lineage(spark, run_id)
        processed = distributed_processing(fact)

        train_df, val_df, test_df = stratified_split(processed, seed=42)

        best_models, spark_metrics_df = fit_spark_models(spark, train_df, val_df, test_df, cfg)

        best_name = (
            spark_metrics_df.withColumn("selection_metric", F.coalesce(F.col("test_auc"), F.col("test_accuracy")))
            .orderBy(F.desc("selection_metric"))
            .first()["model"]
        )

        best_model = best_models[best_name]
        best_pred = (
            best_model.transform(test_df)
            .withColumn("probability", vector_to_array(F.col("probability"))[1])
            .select("target", "prediction", "probability", "A15", "A3", "A13")
        )
        best_pred_pdf = best_pred.toPandas()

        sk_metrics_pdf = sklearn_baselines(train_df, test_df)

        ci_tuple = bootstrap_ci(best_pred_pdf)
        business = business_metrics(best_pred_pdf)
        scaling_pdf = run_scalability_experiments(spark, processed)

        generate_tableau_outputs(processed, spark_metrics_df, sk_metrics_pdf, best_pred_pdf, business, scaling_pdf)
        write_summary_json(run_id, spark_metrics_df, sk_metrics_pdf, ci_tuple, business, scaling_pdf)

        print("Run complete")
        print(f"Run ID: {run_id}")
        print(f"Best Spark model: {best_name}")
        print(f"Outputs: {OUTPUTS_DIR}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
