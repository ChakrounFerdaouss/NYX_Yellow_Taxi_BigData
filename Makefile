.PHONY: help install up down restart ps logs \
        ingest-weather ingest-news ingest upload-hdfs \
        silver gold ml pipeline clean reset-mongo hdfs-ls-bronze hdfs-ls-silver

# ============================================================
# Aide
# ============================================================
help:
	@echo "Cibles disponibles :"
	@echo "  make install        - installe les dependances Python locales"
	@echo "  make up             - demarre toute l'infrastructure Docker"
	@echo "  make down           - arrete l'infrastructure"
	@echo "  make restart        - redemarre l'infrastructure"
	@echo "  make ps             - liste les conteneurs du projet"
	@echo "  make logs           - suit les logs de tous les conteneurs"
	@echo "  make ingest-weather - recupere les donnees meteo (Open-Meteo)"
	@echo "  make ingest-news    - recupere les flux RSS (non structure)"
	@echo "  make ingest         - lance toutes les ingestions externes"
	@echo "  make upload-hdfs    - copie les donnees Bronze locales vers HDFS"
	@echo "  make silver         - job Spark : Bronze -> Silver (nettoyage)"
	@echo "  make gold           - job Spark : Silver -> Gold (KPIs -> MongoDB)"
	@echo "  make ml             - entraine les modeles ML (prediction du prix)"
	@echo "  make pipeline       - enchaine ingest + upload-hdfs + silver + gold + ml"
	@echo "  make hdfs-ls-bronze - liste le contenu de la couche Bronze sur HDFS"
	@echo "  make hdfs-ls-silver - liste le contenu de la couche Silver sur HDFS"
	@echo "  make reset-mongo    - supprime le volume MongoDB (pour re-executer mongo-init)"
	@echo "  make clean          - arrete l'infra et supprime tous les volumes"

# ============================================================
# Setup
# ============================================================
install:
	pip install -r requirements.txt

# ============================================================
# Infrastructure Docker
# ============================================================
up:
	docker compose up -d

down:
	docker compose down

restart: down up

ps:
	docker compose ps

logs:
	docker compose logs -f

# ============================================================
# Ingestion (couche Bronze)
# ============================================================
ingest-weather:
	python ingestion/weather_api.py

ingest-news:
	python ingestion/rss_fetch.py

ingest: ingest-weather ingest-news

upload-hdfs:
	python ingestion/upload_to_hdfs.py

# ============================================================
# Traitements Spark
# ============================================================
silver:
	docker exec -u root -e HOME=/tmp -e HADOOP_USER_NAME=root spark-master spark-submit \
		--conf spark.jars.ivy=/tmp/.ivy2 \
		--master spark://spark-master:7077 \
		/opt/spark-apps/silver_job.py

gold:
	docker exec -u root -e HOME=/tmp -e HADOOP_USER_NAME=root spark-master spark-submit \
		--conf spark.jars.ivy=/tmp/.ivy2 \
		--master spark://spark-master:7077 \
		--packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
		/opt/spark-apps/gold_job.py

ml:
	docker exec -u root -e HOME=/tmp -e HADOOP_USER_NAME=root spark-master spark-submit \
		--conf spark.jars.ivy=/tmp/.ivy2 \
		--master spark://spark-master:7077 \
		/opt/spark-apps/ml_training.py

# ============================================================
# Pipeline complet
# ============================================================
pipeline: ingest upload-hdfs silver gold ml
	@echo "Pipeline complet termine : Bronze -> Silver -> Gold -> ML"

# ============================================================
# Utilitaires HDFS / MongoDB
# ============================================================
hdfs-ls-bronze:
	docker exec namenode hdfs dfs -ls -R /data/bronze

hdfs-ls-silver:
	docker exec namenode hdfs dfs -ls /data/silver/taxi

reset-mongo:
	docker compose stop mongodb
	docker compose rm -f mongodb
	docker volume rm $$(basename $$(pwd))_mongodb_data
	docker compose up -d mongodb mongo-express

# ============================================================
# Nettoyage complet (attention : supprime toutes les donnees)
# ============================================================
clean:
	docker compose down -v
