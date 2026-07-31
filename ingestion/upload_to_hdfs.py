"""
upload_to_hdfs.py

Copie l'ensemble des fichiers bruts de la couche Bronze (taxi, météo,
actualités RSS) depuis le stockage local vers HDFS. Généralisé pour
gérer plusieurs sources en une seule exécution.

Prérequis :
    - Le cluster HDFS (namenode + datanode) doit être démarré :
        docker compose up -d namenode datanode
    - Les fichiers sources doivent avoir été téléchargés/générés au
      préalable dans data/bronze/<source>/ (via download_taxi.py,
      weather_api.py, rss_fetch.py).

Usage :
    python ingestion/upload_to_hdfs.py
"""

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NAMENODE_CONTAINER = "namenode"


@dataclass
class BronzeSource:
    name: str
    local_dir: Path
    hdfs_dir: str
    pattern: str


# --- Sources Bronze à synchroniser vers HDFS -------------------------------
SOURCES = [
    BronzeSource(
        name="taxi",
        local_dir=PROJECT_ROOT / "data" / "bronze" / "taxi",
        hdfs_dir="/data/bronze/taxi",
        pattern="*.parquet",
    ),
    BronzeSource(
        name="weather",
        local_dir=PROJECT_ROOT / "data" / "bronze" / "weather",
        hdfs_dir="/data/bronze/weather",
        pattern="*.json",
    ),
    BronzeSource(
        name="news",
        local_dir=PROJECT_ROOT / "data" / "bronze" / "news",
        hdfs_dir="/data/bronze/news",
        pattern="*.xml",
    ),
]


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, check=True, **kwargs)


def ensure_hdfs_dir(container: str, hdfs_path: str) -> None:
    run(["docker", "exec", container, "hdfs", "dfs", "-mkdir", "-p", hdfs_path])


def upload_file(container: str, local_file: Path, hdfs_dir: str) -> None:
    tmp_path_in_container = f"/tmp/{local_file.name}"
    run(["docker", "cp", str(local_file), f"{container}:{tmp_path_in_container}"])
    run(["docker", "exec", container, "hdfs", "dfs", "-put", "-f", tmp_path_in_container, hdfs_dir])
    run(["docker", "exec", container, "rm", "-f", tmp_path_in_container])


def sync_source(source: BronzeSource) -> int:
    print(f"\n=== Source : {source.name} ===")

    if not source.local_dir.exists():
        print(f"  [Ignoré] {source.local_dir} n'existe pas encore.")
        return 0

    files = sorted(source.local_dir.glob(source.pattern))
    if not files:
        print(f"  [Ignoré] Aucun fichier '{source.pattern}' trouvé dans {source.local_dir}.")
        return 0

    print(f"  {len(files)} fichier(s) détecté(s).")
    ensure_hdfs_dir(NAMENODE_CONTAINER, source.hdfs_dir)

    for i, f in enumerate(files, start=1):
        print(f"  [{i}/{len(files)}] Envoi de {f.name} ...")
        upload_file(NAMENODE_CONTAINER, f, source.hdfs_dir)

    print(f"  -> {len(files)} fichier(s) copié(s) vers {source.hdfs_dir}")
    return len(files)


def main() -> None:
    total_uploaded = 0

    for source in SOURCES:
        total_uploaded += sync_source(source)

    print(f"\nTerminé. {total_uploaded} fichier(s) au total copié(s) vers HDFS (couche Bronze).")

    if total_uploaded == 0:
        print("[Attention] Aucun fichier n'a été trouvé pour aucune source.")
        sys.exit(1)


if __name__ == "__main__":
    main()