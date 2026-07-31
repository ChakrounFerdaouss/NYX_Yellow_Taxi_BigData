"""
rss_fetch.py

Récupère un ou plusieurs flux RSS d'actualités sur les transports à New
York, et écrit le contenu XML tel quel (aucune transformation, aucun
parsing) dans la couche Bronze. C'est la source de données NON
STRUCTURÉE du projet.

Toute la configuration passe par des variables d'environnement (.env) —
aucun paramètre en dur, aucun script shell.

Variables d'environnement utilisées (avec valeurs par défaut) :
    RSS_FEED_URLS      URLs des flux RSS, séparées par des virgules
    RSS_OUTPUT_DIR     dossier de sortie local (data/bronze/news)

Par défaut, utilise le flux Google News filtré sur "NYC transportation"
— un flux public, stable, toujours disponible, sans clé d'API requise.

Usage :
    python ingestion/rss_fetch.py
"""

import os
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Configuration ---------------------------------------------------------

DEFAULT_FEEDS = (
    "https://news.google.com/rss/search?q=NYC+transportation&hl=en-US&gl=US&ceid=US:en,"
    "https://news.google.com/rss/search?q=NYC+taxi&hl=en-US&gl=US&ceid=US:en"
)

RSS_FEED_URLS = [
    url.strip()
    for url in os.environ.get("RSS_FEED_URLS", DEFAULT_FEEDS).split(",")
    if url.strip()
]
OUTPUT_DIR = Path(os.environ.get("RSS_OUTPUT_DIR", "data/bronze/news"))


def slugify(url: str) -> str:
    """Construit un nom de fichier lisible à partir de l'URL du flux."""
    parsed = urlparse(url)
    query = re.sub(r"[^a-zA-Z0-9]+", "-", parsed.query or parsed.path)
    return re.sub(r"-+", "-", query).strip("-").lower() or "feed"


def fetch_feed(url: str) -> str:
    """Télécharge le contenu XML brut du flux RSS."""
    print(f"Requête RSS : {url}")
    response = requests.get(url, timeout=30, headers={"User-Agent": "taxi-bigdata-ingestion/1.0"})
    response.raise_for_status()
    return response.text


def save_raw(xml_content: str, output_dir: Path, url: str) -> Path:
    """Écrit le XML brut, sans parsing ni transformation."""
    output_dir.mkdir(parents=True, exist_ok=True)
    fetched_at = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"rss_{slugify(url)}_{fetched_at}.xml"
    filepath = output_dir / filename

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(xml_content)

    return filepath


def count_items(xml_content: str) -> int:
    """Compte grossièrement le nombre d'items (juste pour affichage/monitoring)."""
    return len(re.findall(r"<item[ >]", xml_content, flags=re.IGNORECASE))


def main():
    if not RSS_FEED_URLS:
        print("[Erreur] Aucune URL de flux RSS configurée (RSS_FEED_URLS).", file=sys.stderr)
        sys.exit(1)

    total_items = 0

    for url in RSS_FEED_URLS:
        try:
            xml_content = fetch_feed(url)
        except requests.RequestException as e:
            print(f"[Erreur] Échec de la requête pour {url} : {e}", file=sys.stderr)
            continue

        filepath = save_raw(xml_content, OUTPUT_DIR, url)
        nb_items = count_items(xml_content)
        total_items += nb_items
        file_size_kb = filepath.stat().st_size / 1024

        print(f"  -> écrit : {filepath} ({nb_items} items, {file_size_kb:.1f} Ko)")

    print(f"\nTerminé. {len(RSS_FEED_URLS)} flux traité(s), {total_items} item(s) au total.")


if __name__ == "__main__":
    main()