# Plateforme Big Data — Analyse et Prédiction du Prix des Courses de Taxi à New York

Projet académique (IPSSI, M2) : conception d'une plateforme Big Data basée sur une **architecture Medallion** (Bronze → Silver → Gold) pour ingérer, nettoyer, analyser et prédire le prix des courses de taxi new-yorkaises (NYC Yellow Taxi).

## Problématique

> Quels sont les principaux facteurs qui influencent le prix d'une course de taxi à New York, et dans quelle mesure peut-on prédire ce prix grâce au Machine Learning ?

## Objectifs

- Construire une architecture Data Lake / Data Warehouse
- Traiter plus de 5 Go de données
- Utiliser Apache Spark sur un cluster
- Mettre en place les couches Bronze, Silver et Gold
- Calculer des KPIs métier
- Réaliser une analyse exploratoire (EDA) et étudier les corrélations
- Construire un modèle de Machine Learning de prédiction de prix
- Déployer toute la plateforme sous Docker
- Monitorer les traitements avec Prometheus / Grafana

## Architecture

```
Sources                Bronze              Silver              Gold
──────────             ──────              ──────              ────
NYC Yellow Taxi   ──▶   Parquet brut  ──▶   Parquet nettoyé ──▶  KPIs
(Parquet)                (HDFS)              + colonnes           (MongoDB)
                                              dérivées
Open-Meteo API    ──▶   JSON brut
(météo)                  (HDFS)

Flux RSS/Reddit   ──▶   XML/JSON brut
(non structuré)          (HDFS)
```

- **Bronze** : données brutes, aucune transformation, stockées sur HDFS
- **Silver** : nettoyage (doublons, valeurs aberrantes, validation de schéma), colonnes dérivées (heure, jour, week-end, durée, prix/mile)
- **Gold** : KPIs agrégés, stockés dans **MongoDB**

## Sources de données

| Source | Format | Rôle |
|---|---|---|
| NYC Yellow Taxi Trip Records | Parquet (5-10 Go) | Source principale (structurée) |
| Open-Meteo API | JSON | Enrichissement météo |
| Flux RSS / Reddit transport NYC | XML / JSON | Source non structurée (exigence du sujet) |

Colonnes principales du dataset taxi : `pickup_datetime`, `dropoff_datetime`, `trip_distance`, `passenger_count`, `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`, `payment_type`, `PULocationID`, `DOLocationID`.

## Stack technique

| Composant | Rôle |
|---|---|
| Docker Compose | Orchestration de l'ensemble des services |
| Apache Spark (master + worker) | Traitement distribué des données |
| HDFS (namenode + datanode) | Stockage des couches Bronze et Silver |
| MongoDB + Mongo Express | Stockage et visualisation des KPIs (couche Gold) |
| Prometheus + Node Exporter | Collecte de métriques système |
| Grafana | Dashboards de monitoring |
| Python / PySpark | Scripts d'ingestion et de traitement |

## Structure du projet

```
taxi-bigdata/
├── docker-compose.yml
├── .env
├── prometheus.yml          (monitoring/)
├── README.md
│
├── data/
│   ├── bronze/             # fichiers Parquet téléchargés localement
│   ├── silver/
│   └── gold/
│
├── ingestion/
│   ├── upload_to_hdfs.py   # copie les Parquet locaux vers HDFS Bronze
│   ├── weather_api.py      # à venir
│   └── rss_fetch.py        # à venir
│
├── spark/
│   ├── silver_job.py       # nettoyage Bronze → Silver
│   ├── gold_job.py         # KPIs Silver → MongoDB 
│   └── ml_training.py      # entraînement des modèles ML 
│
├── notebooks/
│   └── EDA.ipynb           # analyse exploratoire 
│
├── dashboards/             # dashboards Grafana
└── logs/
```

## Prérequis

- Docker et Docker Compose installés
- Fichiers Parquet NYC Yellow Taxi téléchargés dans `data/bronze/`

## Démarrage

### 1. Lancer l'infrastructure

```bash
docker compose up -d
```

Interfaces disponibles :

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| HDFS NameNode UI | http://localhost:9870 |
| MongoDB | localhost:27017 |
| Mongo Express | http://localhost:8081 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |

### 2. Ingestion — couche Bronze

Placer les fichiers `.parquet` téléchargés dans `data/bronze/`, puis :

```bash
python ingestion/upload_to_hdfs.py
```

Vérification :

```bash
docker exec namenode hdfs dfs -ls /data/bronze/taxi
```

### 3. Nettoyage — couche Silver

```bash
docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/silver_job.py
```

Vérification :

```bash
docker exec namenode hdfs dfs -ls /data/silver/taxi
```

### 4. Analyse exploratoire (EDA)

Statistiques descriptives, distributions (prix, distances), répartition par heure/jour, analyse des valeurs manquantes et aberrantes, heatmap des corrélations.

### 5. Corrélations étudiées

- distance ↔ prix
- durée ↔ prix
- pluie ↔ prix
- température ↔ prix
- passagers ↔ prix
- pourboire ↔ prix

### 6. Machine Learning

Prédiction de `total_amount` (ou `fare_amount`) via régression linéaire et Random Forest Regressor, évaluation par MAE / RMSE / R².

### 7. Couche Gold — KPIs

Agrégations stockées dans MongoDB : prix moyen, nombre de courses, distance moyenne, durée moyenne, revenu journalier, top zones de départ/arrivée, prix moyen par heure/jour, pourboire moyen.

### 8. Monitoring

Dashboards Grafana : CPU, mémoire, disque, I/O, temps de traitement, lignes traitées, débit d'ingestion, lectures/écritures HDFS.


## Notes techniques

- Les images Spark utilisent `bitnamilegacy/spark:3.5` (Bitnami a réorganisé ses dépôts après son rachat par Broadcom ; les anciennes images `bitnami/*` ne sont plus disponibles).
- Le schéma NYC TLC utilise `tpep_pickup_datetime` / `tpep_dropoff_datetime` ; ces colonnes sont renommées en `pickup_datetime` / `dropoff_datetime` dans la couche Silver.
