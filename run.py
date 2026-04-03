import os
import sys
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

SHEET_ID = "1oJQTUGcjnZSmOl4W48KgbUyQlnbghR_yij6EIV-4bIs"
CREDS    = "credentials.json"

log.info(f"Démarrage — Sheet ID : {SHEET_ID[:15]}...")

def push_to_sheets(events):
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(CREDS, scopes=scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        headers = ["Nom","Lieu","Date début","Date fin","Nb jours","Week-end","Type","Secteur","Périodicité","Visiteurs total","Visiteurs/jour","Places","Taux remplissage","Population","Pro","Importance","Description","Commentaire","Source","Date scraping"]
        fields = ["nom","lieu","date_debut","date_fin","nb_jours","weekend_inclus","type_evenement","secteur","periodicite","visiteurs_total","visiteurs_par_jour","nb_places","taux_remplissage","population","evenement_pro","importance","description","commentaire","source","date_scraping"]
        rows = [headers] + [[str(ev.get(f) or "") for f in fields] for ev in events]
        sheet.clear()
        sheet.update(rows, value_input_option="RAW")
        sheet.format("A1:T1", {"textFormat":{"bold":True},"backgroundColor":{"red":0.1,"green":0.1,"blue":0.18}})
        log.info(f"Sheet mis à jour — {len(events)} lignes OK")
    except Exception as e:
        log.error(f"Erreur Sheet : {e}")

def load_seed():
    from seed_data import SEED_EVENTS
    from scraper import parse_french_date
    parsed = []
    for ev in SEED_EVENTS:
        ev = dict(ev)
        for f in ["date_debut","date_fin"]:
            if isinstance(ev.get(f), str):
                ev[f] = parse_french_date(ev[f])
        parsed.append(ev)
    return parsed

def run(mode="merge"):
    from scraper import enrich_event, deduplicate, export
    log.info(f"Mode : {mode}")
    seed_events = []
    if mode in ("merge","seed-only"):
        seed_events = load_seed()
        log.info(f"Seed : {len(seed_events)} événements chargés")
    all_events = seed_events
    log.info(f"Total : {len(all_events)} événements")
    enriched = [enrich_event(ev) for ev in all_events]
    enriched.sort(key=lambda x: x.get("date_debut") or "9999")
    export(enriched)
    push_to_sheets(enriched)
    log.info("Terminé")

if __name__ == "__main__":
    args = sys.argv[1:]
    if "--seed-only" in args: run("seed-only")
    else: run("merge")
