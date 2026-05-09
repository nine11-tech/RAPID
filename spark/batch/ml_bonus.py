"""
ml_bonus.py
RAPID — Sprint Bonus — ML Threat Detection
Task: Train a Random Forest classifier on historical cybersecurity logs
      stored LOCALLY, evaluate it, and save predictions LOCALLY.

Usage:
    spark-submit \
      --driver-memory 2g \
      --executor-memory 2g \
      ml_bonus.py

Input        : ./data/cybersecurity-threat-detection-logs.csv
Output       : ./data/predictions.parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os

# ── Constants ─────────────────────────────────────────────────────────────────

# Define local paths (relative to where you run spark-submit)
# Ensure the CSV file is placed at this location before running!
INPUT_PATH  = "./data/cybersecurity-threat-detection-logs.csv" 
OUTPUT_PATH = "./data/predictions.parquet"

CATEGORICAL_FEATURES = ["protocol", "action", "log_type"]
NUMERIC_FEATURES     = ["bytes_transferred"]
LABEL_COL            = "threat_label"
ALL_REQUIRED_COLS    = (
    CATEGORICAL_FEATURES
    + NUMERIC_FEATURES
    + [LABEL_COL, "timestamp", "source_ip", "dest_ip"]
)

# ── Spark Session ─────────────────────────────────────────────────────────────

# Removed HDFS configuration properties
spark = (
    SparkSession.builder
    .appName("RAPID-ML-ThreatDetection-Local")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 65)
print("  RAPID — ML Threat Detection (Random Forest)")
print(f"  Input  : {INPUT_PATH}")
print(f"  Output : {OUTPUT_PATH}")
print("=" * 65)

# ── 1. Load Data ──────────────────────────────────────────────────────────────
# InferSchema=false — we cast manually for correctness and speed.

print("\n>>> [1/5] Loading CSV data from Local Disk...")

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(INPUT_PATH)
)

total_raw = df_raw.count()
print(f"    Raw records loaded : {total_raw:,}")
print(f"    Columns            : {df_raw.columns}")

# ── 2. Preprocessing ──────────────────────────────────────────────────────────

print("\n>>> [2/5] Preprocessing...")

df = (
    df_raw
    # Cast bytes_transferred to Double — required by VectorAssembler
    .withColumn(
        "bytes_transferred",
        F.col("bytes_transferred").cast(DoubleType())
    )
    # Parse ISO timestamp: "2024-12-02T00:00:00" → TimestampType
    .withColumn(
        "timestamp",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    )
    # Keep only the columns we need
    .select(ALL_REQUIRED_COLS)
)

# Drop rows where any required column is null
df_clean = df.dropna(subset=ALL_REQUIRED_COLS)

dropped = total_raw - df_clean.count()
print(f"    Rows after null drop : {df_clean.count():,}  ({dropped:,} dropped)")

# Show label distribution
print("\n    Label distribution:")
df_clean.groupBy(LABEL_COL).count().orderBy("count", ascending=False).show()

# ── 3. Build the ML Pipeline ──────────────────────────────────────────────────

print(">>> [3/5] Building ML Pipeline...")

# StringIndexer for each categorical feature
protocol_idx = StringIndexer(
    inputCol="protocol", outputCol="protocol_idx", handleInvalid="keep"
)
action_idx = StringIndexer(
    inputCol="action",   outputCol="action_idx",   handleInvalid="keep"
)
log_type_idx = StringIndexer(
    inputCol="log_type", outputCol="log_type_idx", handleInvalid="keep"
)

# StringIndexer for the target label
label_idx = StringIndexer(
    inputCol=LABEL_COL,
    outputCol="label",
    handleInvalid="keep"
)

# VectorAssembler: merge all numeric + indexed categorical into one vector
assembler = VectorAssembler(
    inputCols=[
        "bytes_transferred",
        "protocol_idx",
        "action_idx",
        "log_type_idx",
    ],
    outputCol="features",
    handleInvalid="keep"
)

# Random Forest Classifier
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,                  # good accuracy/speed tradeoff
    maxDepth=10,                   # deep enough for complex patterns
    featureSubsetStrategy="sqrt",  # standard for classification tasks
    seed=42
)

# Full pipeline
pipeline = Pipeline(stages=[
    protocol_idx,
    action_idx,
    log_type_idx,
    label_idx,
    assembler,
    rf
])

# ── 4. Train / Test Split & Training ─────────────────────────────────────────

print(">>> [4/5] Splitting data 80/20 and training model...")

train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)
print(f"    Train rows : {train_df.count():,}")
print(f"    Test rows  : {test_df.count():,}")
print("    Training Random Forest (100 trees, maxDepth=10)...")

model = pipeline.fit(train_df)
print("    Training complete.")

# ── 5. Evaluate on Test Set ───────────────────────────────────────────────────

print("\n>>> [5/5] Evaluating model on test set...")

predictions = model.transform(test_df)

def evaluate(metric_name):
    return MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName=metric_name
    ).evaluate(predictions)

accuracy  = evaluate("accuracy")
f1        = evaluate("f1")
precision = evaluate("weightedPrecision")
recall    = evaluate("weightedRecall")

print("\n" + "=" * 65)
print("  MODEL EVALUATION RESULTS")
print("=" * 65)
print(f"  Accuracy           : {accuracy:.4f}  ({accuracy * 100:.2f}%)")
print(f"  F1-Score (weighted): {f1:.4f}")
print(f"  Precision          : {precision:.4f}")
print(f"  Recall             : {recall:.4f}")
print("=" * 65)

# Feature importances
rf_model      = model.stages[-1]
feature_names = ["bytes_transferred", "protocol_idx", "action_idx", "log_type_idx"]
print("\n  Feature Importances:")
for name, imp in zip(feature_names, rf_model.featureImportances):
    bar = "█" * int(imp * 40)
    print(f"    {name:<22} {bar:<42} {imp:.4f}")

# ── 6. Build Output DataFrame ─────────────────────────────────────────────────

label_indexer_model = model.stages[3]   # 4th stage = label_idx
index_to_label_map = F.create_map(*[
    item
    for pair in [
        (F.lit(float(i)), F.lit(label))
        for i, label in enumerate(label_indexer_model.labels)
    ]
    for item in pair
])

output_df = (
    predictions
    .withColumn("predicted_label", index_to_label_map[F.col("prediction")])
    .select(
        F.col("timestamp"),
        F.col("source_ip"),
        F.col("dest_ip"),
        F.col(LABEL_COL).alias("threat_label"),   # original true label string
        F.col("prediction"),                      # numeric prediction
        F.col("predicted_label"),                 # human-readable predicted label
        F.col("probability"),                     # confidence vector per class
    )
)

# ── 7. Save Predictions Locally ──────────────────────────────────────────────

print(f"\n>>> Saving predictions LOCALLY...")
print(f"    {os.path.abspath(OUTPUT_PATH)}")

output_df.write.mode("overwrite").parquet(OUTPUT_PATH)

saved_count = output_df.count()
print(f"    {saved_count:,} rows saved.")

# Sample output
print("\n>>> Sample predictions (10 rows):")
output_df.select(
    "source_ip", "threat_label", "predicted_label", "prediction"
).show(10, truncate=False)

# Confusion matrix summary
print(">>> Confusion summary (true vs predicted):")
output_df \
    .groupBy("threat_label", "predicted_label") \
    .count() \
    .orderBy("threat_label", F.col("count").desc()) \
    .show(truncate=False)

print("\n>>> ML Bonus complete! ✅")
spark.stop()