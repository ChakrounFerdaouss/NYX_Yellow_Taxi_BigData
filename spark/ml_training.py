"""
ml_training.py

Entraîne deux modèles de Machine Learning (régression linéaire et Random
Forest) pour prédire le prix d'une course de taxi (total_amount), à partir
des données nettoyées de la couche Silver.

Variables explicatives utilisées :
    trip_distance, passenger_count, pickup_hour, pickup_dayofweek,
    is_weekend, PULocationID, DOLocationID, payment_type

Évaluation : MAE, RMSE, R²

Lancement (depuis le conteneur spark-master) :
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/ml_training.py
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# --- Configuration ---------------------------------------------------------

SILVER_PATH = "hdfs://namenode:9000/data/silver/taxi"
MODEL_OUTPUT_PATH = "hdfs://namenode:9000/models/taxi_price"

TARGET_COL = "total_amount"

CATEGORICAL_COLS = ["PULocationID", "DOLocationID", "payment_type"]
NUMERIC_COLS = [
    "trip_distance", "passenger_count", "pickup_hour",
    "pickup_dayofweek", "is_weekend",
]

TRAIN_RATIO = 0.8
SEED = 42


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MLTraining-TaxiPricePrediction")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_feature_pipeline():
    """Construit les étapes d'indexation des catégorielles + assemblage des features."""
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL_COLS
    ]

    feature_cols = NUMERIC_COLS + [f"{c}_idx" for c in CATEGORICAL_COLS]

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip",
    )

    return indexers, assembler


def evaluate(predictions, model_name: str):
    evaluator = RegressionEvaluator(labelCol=TARGET_COL, predictionCol="prediction")

    mae = evaluator.setMetricName("mae").evaluate(predictions)
    rmse = evaluator.setMetricName("rmse").evaluate(predictions)
    r2 = evaluator.setMetricName("r2").evaluate(predictions)

    print(f"\n--- Résultats : {model_name} ---")
    print(f"  MAE  : {mae:.3f}")
    print(f"  RMSE : {rmse:.3f}")
    print(f"  R²   : {r2:.4f}")

    return {"model": model_name, "mae": mae, "rmse": rmse, "r2": r2}


def main():
    spark = build_spark_session()

    print(f"Lecture des données Silver depuis {SILVER_PATH} ...")
    df = spark.read.parquet(SILVER_PATH)

    # Cast is_weekend (bool) en int pour l'assembleur de features
    df = df.withColumn("is_weekend", F.col("is_weekend").cast("int"))

    required_cols = CATEGORICAL_COLS + NUMERIC_COLS + [TARGET_COL]
    df = df.dropna(subset=required_cols)

    print(f"Lignes utilisées pour l'entraînement : {df.count():,}")

    train_df, test_df = df.randomSplit([TRAIN_RATIO, 1 - TRAIN_RATIO], seed=SEED)
    print(f"Train : {train_df.count():,} lignes | Test : {test_df.count():,} lignes")

    indexers, assembler = build_feature_pipeline()

    results = []

    # --- Modèle 1 : Régression linéaire ------------------------------------
    lr = LinearRegression(labelCol=TARGET_COL, featuresCol="features")
    lr_pipeline = Pipeline(stages=indexers + [assembler, lr])
    print("\nEntraînement : Régression linéaire ...")
    lr_model = lr_pipeline.fit(train_df)
    lr_predictions = lr_model.transform(test_df)
    results.append(evaluate(lr_predictions, "Régression linéaire"))

    # --- Modèle 2 : Random Forest Regressor ---------------------------------
    rf = RandomForestRegressor(
        labelCol=TARGET_COL, featuresCol="features",
        numTrees=50, maxDepth=8, seed=SEED,
    )
    rf_pipeline = Pipeline(stages=indexers + [assembler, rf])
    print("\nEntraînement : Random Forest Regressor ...")
    rf_model = rf_pipeline.fit(train_df)
    rf_predictions = rf_model.transform(test_df)
    results.append(evaluate(rf_predictions, "Random Forest Regressor"))

    # --- Comparaison finale ---------------------------------------------------
    print("\n=== Comparaison des modèles ===")
    for r in sorted(results, key=lambda x: x["rmse"]):
        print(f"  {r['model']:<28} MAE={r['mae']:.3f}  RMSE={r['rmse']:.3f}  R²={r['r2']:.4f}")

    best_model_name = min(results, key=lambda x: x["rmse"])["model"]
    print(f"\nMeilleur modèle (RMSE le plus bas) : {best_model_name}")

    # --- Sauvegarde du meilleur modèle -----------------------------------------
    model_to_save = rf_model if best_model_name == "Random Forest Regressor" else lr_model
    print(f"Sauvegarde du modèle vers {MODEL_OUTPUT_PATH} ...")
    model_to_save.write().overwrite().save(MODEL_OUTPUT_PATH)

    print("Terminé.")
    spark.stop()


if __name__ == "__main__":
    main()