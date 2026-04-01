import os
import sys
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "1oJQTUGcjnZSmOl4W48KgbUyQlnbghR_yij6EIV-4bIs")
GOOGLE_CREDS = "credentials.json"

log.info(f"SHEET ID lu : '{GOOGLE_SHEET_ID}'")


def export_to_google_sheets(events):
    if not GOOGLE_SHEET_ID:
        log.info("Google Sheets désactivé.")
        return
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        log.warning("gspread non installé.")
        return
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(GOOGLE_CREDS, scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(GOOGLE_SHEET_ID).sheet1
        headers = ["Nom","Lieu","Date début","Date fin","Nb jours","Week-end inclus","Type événement","Secteur","Périodicité","Visiteurs total","Visiteurs / jour","Nb places","Taux remplissage","Population","Événement pro","Importance","Description","Commentaire","Source","Date scraping"]
        fields = ["nom","lieu","date_debut","date_fin","nb_jours","weekend_inclus","type_evenement","secteur","periodicite","visiteurs_total","visiteurs_par_jour","nb_places","taux_remplissage","population","evenement_pro","importance","description","commentaire","source","date_scraping"]
        rows = [headers]
        for ev in events:
            rows.append([str(ev.get(f) or "") for f in fields])
        sheet.clear()
        sheet.update(rows, value_input_option="RAW")
        sheet.format("A1:T1", {"textFormat": {"bold": True}, "backgroundColor": {"red": 0.1, "green": 0.1, "blue": 0.18}})
        log.info(f"Google Sheets mis à jour — {len(events)} lignes")
    except FileNotFoundError:
        log.error("credentials.json introuvable")
    except Exception as e:
        log.error(f"Erreur Google Sheets : {e}")


def load_seed():
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


def merge(seed, scraped):
    import re
    def key(name): return re.sub(r"\W+", "", name.lower())[:20]
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


def run(mode="merge"):
    from scraper import enrich_event, deduplicate, export
    log.info(f"Mode : {mode}")
    seed_events, scraped_events = [], []
    if mode in ("merge", "seed-only"):
        seed_events = load_seed()
        log.info(f"Seed : {len(seed_events)} événements chargés")
    if mode in ("merge", "scrape-only"):
        from scraper import scrape_viparis, scrape_eventseye, scrape_eventseye_p2, scrape_sortiraparis
        scraped_events += scrape_eventseye()
        scraped_events += scrape_eventseye_p2()
        scraped_events += scrape_viparis()
        scraped_events += scrape_sortiraparis()
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


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--seed-only" in args:      run("seed-only")
    elif "--scrape-only" in args:  run("scrape-only")
    else:                          run("merge")
