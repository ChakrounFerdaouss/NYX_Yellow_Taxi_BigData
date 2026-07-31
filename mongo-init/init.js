// init.js
// Exécuté automatiquement au premier démarrage du conteneur mongodb
// (monté dans /docker-entrypoint-initdb.d).
// Crée les index utiles pour la performance des requêtes sur les
// collections KPIs de la couche Gold (voir spark/gold_job.py).

db = db.getSiblingDB(process.env.MONGO_INITDB_DATABASE || "taxi_gold");

db.createCollection("kpi_global_summary");
db.createCollection("kpi_daily_revenue");
db.createCollection("kpi_top_pickup_zones");
db.createCollection("kpi_top_dropoff_zones");
db.createCollection("kpi_price_by_hour");
db.createCollection("kpi_price_by_day");
db.createCollection("kpi_weekday_vs_weekend");
db.createCollection("kpi_tip_by_payment");
db.createCollection("kpi_monthly_stats");

db.kpi_daily_revenue.createIndex({ pickup_date: 1 });
db.kpi_top_pickup_zones.createIndex({ PULocationID: 1 });
db.kpi_top_dropoff_zones.createIndex({ DOLocationID: 1 });
db.kpi_price_by_hour.createIndex({ pickup_hour: 1 });
db.kpi_price_by_day.createIndex({ pickup_dayofweek: 1 });
db.kpi_monthly_stats.createIndex({ year: 1, month: 1 });

print("Initialisation MongoDB terminée : collections et index créés.");