"""
run.py
------
Fusionne seed + scraping, exporte en Excel/CSV/JSON et Google Sheets.

Usage :
  python run.py               → fusion seed + scraping + export
  python run.py --seed-only   → export uniquement les données seed
  python run.py --scrape-only → uniquement les données scrapées
  python run.py --schedule    → tourne chaque semaine (lundi 7h)

────────────────────────────────────────────
CONFIGURATION GOOGLE SHEETS
Remplis ces 2 variables après le guide étapes 4-6.
────────────────────────────────────────────
"""

# ← Colle ici l'ID de ton Google Sheet (depuis l'URL)
GOOGLE_SHEET_ID = "1oJQTUGcjnZSmOl4W48KgbUyQlnbghR_yij6EIV-4bIs"

# ← Chemin vers ton fichier credentials.json
GOOGLE_CREDS = "credentials.json"

import sys
import logging
from datetime import datetime

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def export_to_google_sheets(events: list[dict]):
    if not GOOGLE_SHEET_ID:
        log.info("Google Sheets désactivé (GOOGLE_SHEET_ID vide). Export local uniquement.")
        return
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        log.warning("gspread non installé. Lance : pip install gspread google-auth")
        return
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(GOOGLE_CREDS, scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(GOOGLE_SHEET_ID).sheet1

        headers = [
            "Nom", "Lieu", "Date début", "Date fin", "Nb jours", "Week-end inclus",
            "Type événement", "Secteur", "Périodicité",
            "Visiteurs total", "Visiteurs / jour", "Nb places", "Taux remplissage",
            "Population", "Événement pro", "Importance",
            "Description", "Commentaire", "Source", "Date scraping"
        ]
        fields = [
            "nom", "lieu", "date_debut", "date_fin", "nb_jours", "weekend_inclus",
            "type_evenement", "secteur", "periodicite",
            "visiteurs_total", "visiteurs_par_jour", "nb_places", "taux_remplissage",
            "population", "evenement_pro", "importance",
            "description", "commentaire", "source", "date_scraping"
        ]

        rows = [headers]
        for ev in events:
            rows.append([str(ev.get(f) or "") for f in fields])

        sheet.clear()
        sheet.update(rows, value_input_option="RAW")
        sheet.format("A1:T1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.1, "green": 0.1, "blue": 0.18},
        })
        log.info(f"Google Sheets mis à jour — {len(events)} lignes")

    except FileNotFoundError:
        log.error(f"Fichier credentials introuvable : {GOOGLE_CREDS}")
    except Exception as e:
        log.error(f"Erreur Google Sheets : {e}")
        log.info("Les exports locaux (Excel, CSV) ont quand même été générés.")


def load_seed() -> list[dict]:
    from seed_data import SEED_EVENTS
    from scraper import parse_french_date
    parsed = []
    for ev in SEED_EVENTS:
        ev = dict(ev)
        for field in ["date_debut", "date_fin"]:
            val = ev.get(field)
            if isinstance(val, str):
                ev[field] = parse_french_date(val)
        parsed.append(ev)
    return parsed


def merge(seed: list[dict], scraped: list[dict]) -> list[dict]:
    import re
    def key(name):
        return re.sub(r"\W+", "", name.lower())[:20]
    seed_index = {key(ev["nom"]): ev for ev in seed}
    for sc_ev in scraped:
        k = key(sc_ev["nom"])
        if k in seed_index:
            for field, val in sc_ev.items():
                if val and not seed_index[k].get(field):
                    seed_index[k][field] = val
        else:
            seed_index[k] = sc_ev
    return list(seed_index.values())


def run(mode: str = "merge"):
    from scraper import enrich_event, deduplicate, export

    log.info(f"Mode : {mode}")
    seed_events, scraped_events = [], []

    if mode in ("merge", "seed-only"):
        seed_events = load_seed()
        log.info(f"Seed : {len(seed_events)} événements chargés")

    if mode in ("merge", "scrape-only"):
        from scraper import (scrape_viparis, scrape_eventseye,
                             scrape_eventseye_p2, scrape_sortiraparis)
        scraped_events += scrape_eventseye()
        scraped_events += scrape_eventseye_p2()
        scraped_events += scrape_viparis()
        scraped_events += scrape_sortiraparis()
        log.info(f"Scraping : {len(scraped_events)} événements bruts")

    if mode == "merge":
        all_events = merge(seed_events, scraped_events)
    elif mode == "seed-only":
        all_events = seed_events
    else:
        all_events = deduplicate(scraped_events)

    log.info(f"Total : {len(all_events)} événements")
    enriched = [enrich_event(ev) for ev in all_events]
    enriched.sort(key=lambda x: x.get("date_debut") or "9999-99-99")

    export(enriched)
    export_to_google_sheets(enriched)

    log.info("Terminé")
    return enriched


def run_scheduled():
    try:
        import schedule
        import time
    except ImportError:
        log.error("Installe schedule : pip install schedule")
        return

    def job():
        log.info(f"Scraping hebdomadaire — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        run("merge")

    schedule.every().monday.at("07:00").do(job)
    log.info("Scheduler démarré — prochain run : lundi 07:00")
    job()
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--schedule" in args:
        run_scheduled()
    elif "--seed-only" in args:
        run("seed-only")
    elif "--scrape-only" in args:
        run("scrape-only")
    else:
        run("merge")     
