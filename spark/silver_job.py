"""
silver_job.py

Lit les fichiers Parquet bruts de la couche Bronze (HDFS), nettoie les
données et écrit le résultat dans la couche Silver (HDFS), au format
Parquet, partitionné par année/mois.

Traitements appliqués :
    - suppression des doublons
    - suppression des valeurs aberrantes (prix négatifs, distances
      nulles ou absurdes, dates incohérentes, passagers hors plage)
    - validation / cast des types
    - colonnes dérivées : pickup_hour, pickup_dayofweek, is_weekend,
      trip_duration_minutes, price_per_mile

Lancement (depuis le conteneur spark-master) :
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/silver_job.py
"""

from pyspark.sql import SparkSession, functions as F

# --- Configuration ------------------------------------------------------

BRONZE_PATH = "hdfs://namenode:9000/data/bronze/taxi"
SILVER_PATH = "hdfs://namenode:9000/data/silver/taxi"

# Bornes utilisées pour filtrer les valeurs aberrantes
MIN_FARE = 0.0
MAX_FARE = 500.0
MIN_DISTANCE = 0.1        # miles
MAX_DISTANCE = 100.0      # miles
MIN_PASSENGERS = 1
MAX_PASSENGERS = 6
MAX_DURATION_MINUTES = 180


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SilverJob-TaxiCleaning")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_bronze(spark: SparkSession):
    df = spark.read.parquet(BRONZE_PATH)

    # Le schéma officiel NYC TLC utilise tpep_pickup_datetime /
    # tpep_dropoff_datetime. On les renomme vers des noms plus génériques
    # pour la suite du pipeline.
    rename_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
    }
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    return df


def clean(df):
    # 1. Suppression des doublons stricts
    df = df.dropDuplicates()

    # 2. Suppression des lignes avec des valeurs nulles sur les colonnes clés
    required_cols = [
        "pickup_datetime", "dropoff_datetime", "trip_distance",
        "fare_amount", "total_amount", "passenger_count",
    ]
    df = df.dropna(subset=[c for c in required_cols if c in df.columns])

    # 3. Cast des types
    df = (
        df.withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
          .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
          .withColumn("trip_distance", F.col("trip_distance").cast("double"))
          .withColumn("fare_amount", F.col("fare_amount").cast("double"))
          .withColumn("total_amount", F.col("total_amount").cast("double"))
          .withColumn("tip_amount", F.col("tip_amount").cast("double"))
          .withColumn("tolls_amount", F.col("tolls_amount").cast("double"))
          .withColumn("passenger_count", F.col("passenger_count").cast("int"))
    )

    # 4. Cohérence temporelle : le drop-off doit être après le pickup
    df = df.filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))

    # 5. Colonnes dérivées
    df = (
        df.withColumn("pickup_hour", F.hour("pickup_datetime"))
          .withColumn("pickup_dayofweek", F.dayofweek("pickup_datetime"))  # 1=dimanche ... 7=samedi
          .withColumn("is_weekend", F.col("pickup_dayofweek").isin([1, 7]))
          .withColumn(
              "trip_duration_minutes",
              (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")) / 60.0
          )
          .withColumn(
              "price_per_mile",
              F.when(F.col("trip_distance") > 0, F.col("total_amount") / F.col("trip_distance"))
               .otherwise(None)
          )
    )

    # 6. Filtrage des valeurs aberrantes
    df = df.filter(
        (F.col("fare_amount") >= MIN_FARE) & (F.col("fare_amount") <= MAX_FARE) &
        (F.col("trip_distance") >= MIN_DISTANCE) & (F.col("trip_distance") <= MAX_DISTANCE) &
        (F.col("passenger_count") >= MIN_PASSENGERS) & (F.col("passenger_count") <= MAX_PASSENGERS) &
        (F.col("trip_duration_minutes") > 0) & (F.col("trip_duration_minutes") <= MAX_DURATION_MINUTES)
    )

    # 7. Colonnes de partitionnement
    df = (
        df.withColumn("year", F.year("pickup_datetime"))
          .withColumn("month", F.month("pickup_datetime"))
    )

    return df


def main():
    spark = build_spark_session()

    print(f"Lecture des données Bronze depuis {BRONZE_PATH} ...")
    df_bronze = load_bronze(spark)
    count_bronze = df_bronze.count()
    print(f"Lignes en entrée (Bronze) : {count_bronze:,}")

    df_silver = clean(df_bronze)
    count_silver = df_silver.count()
    print(f"Lignes en sortie (Silver) : {count_silver:,}")
    print(f"Lignes supprimées : {count_bronze - count_silver:,}")

    print(f"Écriture vers {SILVER_PATH} (partitionné par year/month) ...")
    (
        df_silver.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(SILVER_PATH)
    )

    print("Terminé.")
    spark.stop()


if __name__ == "__main__":
    main()