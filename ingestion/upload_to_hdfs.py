"""
upload_to_hdfs.py

Copie les fichiers Parquet des trajets taxi (NYC Yellow Taxi) depuis le
stockage local (data/bronze/) vers la couche Bronze sur HDFS.

Prérequis :
    - Le cluster HDFS (namenode + datanode) doit être démarré :
        docker compose up -d namenode datanode
    - Les fichiers .parquet doivent être placés dans ./data/bronze/ à la
      racine du projet (ce dossier est monté dans les conteneurs Spark
      via ./data:/opt/spark-data).

Usage :
    python ingestion/upload_to_hdfs.py
"""

import subprocess
import sys
from pathlib import Path

# --- Configuration ----------------------------------------------------

LOCAL_BRONZE_DIR = Path(__file__).resolve().parent.parent / "data" / "bronze" / "taxi"
HDFS_BRONZE_DIR = "/data/bronze/taxi"
NAMENODE_CONTAINER = "namenode"


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Exécute une commande et affiche ce qui est lancé."""
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, check=True, **kwargs)


def ensure_hdfs_dir(container: str, hdfs_path: str) -> None:
    """Crée le dossier HDFS cible s'il n'existe pas déjà."""
    run([
        "docker", "exec", container,
        "hdfs", "dfs", "-mkdir", "-p", hdfs_path,
    ])


def upload_file(container: str, local_file: Path, hdfs_dir: str) -> None:
    """Copie un fichier local dans le conteneur namenode, puis vers HDFS."""
    tmp_path_in_container = f"/tmp/{local_file.name}"

    # 1. Copier le fichier local dans le conteneur namenode
    run(["docker", "cp", str(local_file), f"{container}:{tmp_path_in_container}"])

    # 2. Charger le fichier vers HDFS (écrase si déjà présent)
    run([
        "docker", "exec", container,
        "hdfs", "dfs", "-put", "-f", tmp_path_in_container, hdfs_dir,
    ])

    # 3. Nettoyer le fichier temporaire dans le conteneur
    run(["docker", "exec", container, "rm", "-f", tmp_path_in_container])


def main() -> None:
    if not LOCAL_BRONZE_DIR.exists():
        print(f"[Erreur] Le dossier {LOCAL_BRONZE_DIR} n'existe pas.")
        sys.exit(1)

    parquet_files = sorted(LOCAL_BRONZE_DIR.glob("*.parquet"))

    if not parquet_files:
        print(f"[Erreur] Aucun fichier .parquet trouvé dans {LOCAL_BRONZE_DIR}.")
        sys.exit(1)

    print(f"{len(parquet_files)} fichier(s) Parquet détecté(s) dans {LOCAL_BRONZE_DIR}")

    ensure_hdfs_dir(NAMENODE_CONTAINER, HDFS_BRONZE_DIR)

    for i, f in enumerate(parquet_files, start=1):
        print(f"\n[{i}/{len(parquet_files)}] Envoi de {f.name} ...")
        upload_file(NAMENODE_CONTAINER, f, HDFS_BRONZE_DIR)

    print(f"\nTerminé. {len(parquet_files)} fichier(s) copié(s) vers HDFS : {HDFS_BRONZE_DIR}")
    print(f"Vérifie avec : docker exec {NAMENODE_CONTAINER} hdfs dfs -ls {HDFS_BRONZE_DIR}")


if __name__ == "__main__":
    main()