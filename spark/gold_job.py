"""
gold_job.py

Lit les données nettoyées de la couche Silver (HDFS), calcule les KPIs
métier, et les écrit dans MongoDB (couche Gold).

KPIs calculés :
    - prix moyen, nombre de courses, distance moyenne, durée moyenne
    - revenu journalier
    - top zones de départ / arrivée
    - prix moyen par heure / par jour
    - pourboire moyen

Lancement (depuis le conteneur spark-master) — nécessite le connecteur
MongoDB Spark, ajouté via --packages :

    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
        /opt/spark-apps/gold_job.py
"""

import os
from pyspark.sql import SparkSession, functions as F

# --- Configuration -------------------------------------------------------

SILVER_PATH = "hdfs://namenode:9000/data/silver/taxi"

MONGO_USER = os.environ.get("MONGO_USER", "taxi_user")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD", "taxi_pass")
MONGO_DB = os.environ.get("MONGO_DB", "taxi_gold")
MONGO_HOST = "mongodb:27017"

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}/{MONGO_DB}?authSource=admin"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("GoldJob-TaxiKPIs")
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def write_to_mongo(df, collection: str):
    """Écrit un DataFrame Spark dans une collection MongoDB (écrase la collection)."""
    (
        df.write
        .format("mongodb")
        .mode("overwrite")
        .option("collection", collection)
        .save()
    )
    print(f"  -> écrit dans la collection '{collection}' ({df.count()} document(s))")


def main():
    spark = build_spark_session()

    print(f"Lecture des données Silver depuis {SILVER_PATH} ...")
    df = spark.read.parquet(SILVER_PATH)
    df.cache()
    print(f"Lignes en entrée : {df.count():,}")

    # --- KPI 1 : synthèse globale ---------------------------------------
    print("\nKPI 1/9 : synthèse globale")
    global_summary = df.agg(
        F.avg("total_amount").alias("avg_price"),
        F.count("*").alias("nb_trips"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
        F.avg("tip_amount").alias("avg_tip"),
    )
    write_to_mongo(global_summary, "kpi_global_summary")

    # --- KPI 2 : revenu journalier ---------------------------------------
    print("KPI 2/9 : revenu journalier")
    daily_revenue = (
        df.withColumn("pickup_date", F.to_date("pickup_datetime"))
        .groupBy("pickup_date")
        .agg(
            F.sum("total_amount").alias("daily_revenue"),
            F.count("*").alias("nb_trips"),
        )
        .orderBy("pickup_date")
    )
    write_to_mongo(daily_revenue, "kpi_daily_revenue")

    # --- KPI 3 : top zones de départ -------------------------------------
    print("KPI 3/9 : top zones de départ")
    top_pickup_zones = (
        df.groupBy("PULocationID")
        .agg(F.count("*").alias("nb_trips"), F.avg("total_amount").alias("avg_price"))
        .orderBy(F.desc("nb_trips"))
        .limit(20)
    )
    write_to_mongo(top_pickup_zones, "kpi_top_pickup_zones")

    # --- KPI 4 : top zones d'arrivée --------------------------------------
    print("KPI 4/9 : top zones d'arrivée")
    top_dropoff_zones = (
        df.groupBy("DOLocationID")
        .agg(F.count("*").alias("nb_trips"), F.avg("total_amount").alias("avg_price"))
        .orderBy(F.desc("nb_trips"))
        .limit(20)
    )
    write_to_mongo(top_dropoff_zones, "kpi_top_dropoff_zones")

    # --- KPI 5 : prix moyen par heure --------------------------------------
    print("KPI 5/9 : prix moyen par heure")
    price_by_hour = (
        df.groupBy("pickup_hour")
        .agg(F.avg("total_amount").alias("avg_price"), F.count("*").alias("nb_trips"))
        .orderBy("pickup_hour")
    )
    write_to_mongo(price_by_hour, "kpi_price_by_hour")

    # --- KPI 6 : prix moyen par jour de la semaine -------------------------
    print("KPI 6/9 : prix moyen par jour de la semaine")
    price_by_day = (
        df.groupBy("pickup_dayofweek")
        .agg(F.avg("total_amount").alias("avg_price"), F.count("*").alias("nb_trips"))
        .orderBy("pickup_dayofweek")
    )
    write_to_mongo(price_by_day, "kpi_price_by_day")

    # --- KPI 7 : semaine vs week-end ----------------------------------------
    print("KPI 7/9 : semaine vs week-end")
    weekday_vs_weekend = (
        df.groupBy("is_weekend")
        .agg(
            F.avg("total_amount").alias("avg_price"),
            F.count("*").alias("nb_trips"),
            F.avg("tip_amount").alias("avg_tip"),
        )
    )
    write_to_mongo(weekday_vs_weekend, "kpi_weekday_vs_weekend")

    # --- KPI 8 : pourboire moyen par mode de paiement -----------------------
    print("KPI 8/9 : pourboire moyen par mode de paiement")
    tip_by_payment = (
        df.groupBy("payment_type")
        .agg(F.avg("tip_amount").alias("avg_tip"), F.count("*").alias("nb_trips"))
        .orderBy("payment_type")
    )
    write_to_mongo(tip_by_payment, "kpi_tip_by_payment")

    # --- KPI 9 : distance et durée moyennes par mois ------------------------
    print("KPI 9/9 : distance et durée moyennes par mois")
    monthly_stats = (
        df.groupBy("year", "month")
        .agg(
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
            F.count("*").alias("nb_trips"),
        )
        .orderBy("year", "month")
    )
    write_to_mongo(monthly_stats, "kpi_monthly_stats")

    print("\nTerminé. Tous les KPIs ont été écrits dans MongoDB.")
    spark.stop()


if __name__ == "__main__":
    main()