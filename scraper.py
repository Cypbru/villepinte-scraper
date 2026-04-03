import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import time
import re
import json
import os
import logging

log = logging.getLogger(__name__)

OUTPUT_DIR = "output"
OUTPUT_EXCEL = os.path.join(OUTPUT_DIR, "villepinte_events.xlsx")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "villepinte_events.csv")
OUTPUT_JSON = os.path.join(OUTPUT_DIR, "villepinte_events.json")

HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36","Accept-Language":"fr-FR,fr;q=0.9","Accept":"text/html,application/xhtml+xml"}

PRO_KEYWORDS = ["salon","congrès","b2b","industrie","professionnel","trade show","foire","conférence","corporate","summit"]
POPULATION_TAGS = {"jeunes":["manga","gaming","cosplay","japan expo"],"familles":["famille","enfant","loisirs"],"CSP++":["luxe","design","mode","textile","décoration"],"industriels":["industrie","logistique","btp","emballage"],"institutionnel":["défense","militaire","sécurité","naval"],"agroalimentaire":["alimentation","restauration","sial"]}
IMPORTANCE_RULES = {"high":["eurosatory","sial","japan expo","maison & objet","intermat","première vision","global industrie"],"med":["silmo","all4pack","euronaval","milipol","paris manga"]}

def get_page(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
        except Exception as e:
            log.warning(f"Tentative {attempt+1}/{retries} : {e}")
            time.sleep(2 * (attempt + 1))
    return None

def parse_french_date(text):
    mois = {"janvier":1,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,"juillet":7,"août":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12,"jan":1,"fév":2,"mar":3,"avr":4,"jun":6,"jul":7,"aoû":8,"sep":9,"oct":10,"nov":11,"déc":12}
    text = text.lower().strip()
    for pattern, fmt in [(r"\d{4}-\d{2}-\d{2}","%Y-%m-%d"),(r"\d{2}/\d{2}/\d{4}","%d/%m/%Y")]:
        m = re.search(pattern, text)
        if m:
            try: return datetime.strptime(m.group(0), fmt)
            except: pass
    m = re.search(r"(\d{1,2})\s+([a-zéûà]+)\.?\s+(\d{4})", text)
    if m:
        day, mon_str, year = m.groups()
        mon = mois.get(mon_str[:3]) or mois.get(mon_str)
        if mon:
            try: return datetime(int(year), mon, int(day))
            except: pass
    return None

def count_days(start, end): return max(1, (end-start).days+1)

def has_weekend(start, end):
    d = start
    while d <= end:
        if d.weekday() >= 5: return True
        d += timedelta(days=1)
    return False

def infer_population(name, secteur):
    text = (name+" "+secteur).lower()
    found = [t for t,kws in POPULATION_TAGS.items() if any(k in text for k in kws)]
    return ", ".join(found) if found else "Professionnels divers"

def infer_importance(name):
    n = name.lower()
    for imp, kws in IMPORTANCE_RULES.items():
        if any(k in n for k in kws): return imp
    return "low"

def is_professional(name, etype, secteur):
    return any(k in (name+" "+etype+" "+secteur).lower() for k in PRO_KEYWORDS)

def enrich_event(ev):
    start = ev.get("date_debut")
    end = ev.get("date_fin") or start
    nb_jours = count_days(start, end) if start and end else None
    weekend = has_weekend(start, end) if start and end else None
    vt = ev.get("visiteurs_total", 0)
    nom = ev.get("nom","")
    secteur = ev.get("secteur","")
    etype = ev.get("type_evenement","")
    return {
        "nom": nom, "lieu": ev.get("lieu","Paris Nord Villepinte"),
        "date_debut": start.strftime("%Y-%m-%d") if start else None,
        "date_fin": end.strftime("%Y-%m-%d") if end else None,
        "nb_jours": nb_jours, "weekend_inclus": "Oui" if weekend else "Non",
        "type_evenement": etype, "secteur": secteur,
        "visiteurs_total": vt,
        "visiteurs_par_jour": round(vt/nb_jours) if nb_jours else 0,
        "nb_places": ev.get("nb_places"),
        "taux_remplissage": f"{round(vt/ev['nb_places']*100)}%" if ev.get("nb_places") and vt else None,
        "population": ev.get("population") or infer_population(nom, secteur),
        "evenement_pro": "Oui" if is_professional(nom, etype, secteur) else "Non",
        "importance": ev.get("importance") or infer_importance(nom),
        "description": ev.get("description",""), "commentaire": ev.get("commentaire",""),
        "periodicite": ev.get("periodicite",""), "source": ev.get("source",""),
        "date_scraping": datetime.now().strftime("%Y-%m-%d"),
    }

def deduplicate(events):
    seen = {}
    for ev in events:
        key = re.sub(r"\W+","",ev["nom"].lower())[:20]
        if key not in seen: seen[key] = ev
        else:
            for f in ["description","periodicite","population","commentaire","visiteurs_total","nb_places","secteur"]:
                if not seen[key].get(f) and ev.get(f): seen[key][f] = ev[f]
    return list(seen.values())

def export(events):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not events: return
    df = pd.DataFrame(events)
    cols = ["nom","lieu","date_debut","date_fin","nb_jours","weekend_inclus","type_evenement","secteur","periodicite","visiteurs_total","visiteurs_par_jour","nb_places","taux_remplissage","population","evenement_pro","importance","description","commentaire","source","date_scraping"]
    for c in cols:
        if c not in df.columns: df[c] = None
    df = df[cols]
    writer = pd.ExcelWriter(OUTPUT_EXCEL, engine="xlsxwriter")
    df.to_excel(writer, index=False, sheet_name="Événements")
    wb = writer.book
    ws = writer.sheets["Événements"]
    hfmt = wb.add_format({"bold":True,"bg_color":"#1a1a2e","font_color":"#FFFFFF","border":1,"align":"center"})
    wfmt = wb.add_format({"text_wrap":True,"valign":"top"})
    col_widths = {"nom":28,"lieu":22,"date_debut":13,"date_fin":13,"nb_jours":8,"weekend_inclus":10,"type_evenement":18,"secteur":22,"periodicite":12,"visiteurs_total":14,"visiteurs_par_jour":13,"nb_places":12,"taux_remplissage":14,"population":32,"evenement_pro":10,"importance":12,"description":50,"commentaire":50,"source":18,"date_scraping":14}
    for i, col in enumerate(cols):
        ws.set_column(i, i, col_widths.get(col,15), wfmt if col in ["description","commentaire","population"] else None)
        ws.write(0, i, col.replace("_"," ").title(), hfmt)
    ws.freeze_panes(1, 0)
    ws.autofilter(0, 0, len(df), len(cols)-1)
    writer.close()
    log.info(f"Excel exporté : {OUTPUT_EXCEL}")
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    log.info(f"CSV exporté : {OUTPUT_CSV}")
    with open(OUTPUT_JSON,"w",encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"✅ Export terminé — {len(events)} événements")

def scrape_eventseye():
    log.info("Scraping EventsEye...")
    soup = get_page("https://www.eventseye.com/fairs/pl0_salons_paris_11.html")
    if not soup: return []
    events = []
    table = soup.find("table")
    if not table: return []
    for row in table.find_all("tr")[1:]:
        try:
            cells = row.find_all("td")
            if len(cells) < 3: continue
            nom = (cells[0].find("a") or cells[0]).get_text(strip=True)
            desc = (cells[0].find("em") or cells[0].find("i"))
            description = desc.get_text(strip=True) if desc else ""
            periodicite = cells[1].get_text(strip=True)
            date_text = cells[2].get_text(strip=True)
            dm = re.search(r"(\d{2}/\d{2}/\d{4})", date_text)
            daysm = re.search(r"(\d+)\s*jours?", date_text)
            start = parse_french_date(dm.group(1)) if dm else None
            nb_days = int(daysm.group(1)) if daysm else 1
            end = start + timedelta(days=nb_days-1) if start else None
            events.append({"nom":nom,"lieu":"Paris Nord Villepinte","date_debut":start,"date_fin":end,"type_evenement":"Expo Professionnelle","secteur":"","periodicite":periodicite,"description":description,"evenement_pro":"Oui","source":"EventsEye"})
        except: continue
    log.info(f"EventsEye : {len(events)} événements")
    return events

def scrape_eventseye_p2():
    log.info("Scraping EventsEye p2...")
    soup = get_page("https://www.eventseye.com/fairs/pl1_salons_paris_11.html")
    if not soup: return []
    events = []
    table = soup.find("table")
    if not table: return []
    for row in table.find_all("tr")[1:]:
        try:
            cells = row.find_all("td")
            if len(cells) < 3: continue
            nom = (cells[0].find("a") or cells[0]).get_text(strip=True)
            if not nom: continue
            periodicite = cells[1].get_text(strip=True)
            date_text = cells[2].get_text(strip=True)
            dm = re.search(r"(\d{2}/\d{2}/\d{4})", date_text)
            daysm = re.search(r"(\d+)\s*jours?", date_text)
            start = parse_french_date(dm.group(1)) if dm else None
            nb_days = int(daysm.group(1)) if daysm else 1
            end = start + timedelta(days=nb_days-1) if start else None
            events.append({"nom":nom,"lieu":"Paris Nord Villepinte","date_debut":start,"date_fin":end,"type_evenement":"Expo Professionnelle","periodicite":periodicite,"source":"EventsEye p2"})
        except: continue
    log.info(f"EventsEye p2 : {len(events)} événements")
    return events

def scrape_viparis():
    log.info("Scraping Viparis...")
    soup = get_page("https://www.viparis.com/nos-lieux/paris-nord-villepinte/agenda")
    if not soup: return []
    events = []
    raw_text = soup.get_text(" ", strip=True)
    pattern = re.compile(r"([A-ZÀ-Ü][A-ZÀ-Üa-zà-ü &']{3,50})\s+du\s+(\d{2}/\d{2}/\d{4})\s+au\s+(\d{2}/\d{2}/\d{4})")
    for m in pattern.finditer(raw_text):
        nom, d1, d2 = m.groups()
        events.append({"nom":nom.strip(),"lieu":"Paris Nord Villepinte","date_debut":parse_french_date(d1),"date_fin":parse_french_date(d2),"type_evenement":"","secteur":"","source":"Viparis"})
    log.info(f"Viparis : {len(events)} événements")
    return events

def scrape_sortiraparis():
    log.info("Scraping SortiràParis...")
    soup = get_page("https://www.sortiraparis.com/lieux/53151-parc-des-expositions-paris-nord-villepinte")
    if not soup: return []
    events = []
    for art in soup.find_all("article"):
        try:
            title_tag = art.find(["h2","h3","h4"])
            nom = title_tag.get_text(strip=True) if title_tag else ""
            if not nom or len(nom) < 4: continue
            text = art.get_text(" ", strip=True)
            dates = re.findall(r"\d{1,2}\s+[a-zéûà]+\s+\d{4}", text, re.I)
            start = parse_french_date(dates[0]) if dates else None
            end = parse_french_date(dates[1]) if len(dates) > 1 else start
            events.append({"nom":nom,"lieu":"Paris Nord Villepinte","date_debut":start,"date_fin":end,"type_evenement":"Expo Grand Public","secteur":"Loisirs","source":"SortiràParis"})
        except: continue
    log.info(f"SortiràParis : {len(events)} événements")
    return events
