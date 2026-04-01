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
OUTPUT_CSV   = os.path.join(OUTPUT_DIR, "villepinte_events.csv")
OUTPUT_JSON  = os.path.join(OUTPUT_DIR, "villepinte_events.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

PRO_KEYWORDS = ["salon","expo professionnelle","congrès","b2b","industrie","professionnel","trade show","business","foire","conférence","corporate","summit","forum"]

POPULATION_TAGS = {
    "jeunes": ["manga","gaming","jeux","pop culture","cosplay","sci-fi","japan expo"],
    "familles": ["famille","enfant","loisirs","grand public"],
    "CSP++": ["luxe","design","mode","textile","décoration","architecture"],
    "industriels": ["industrie","logistique","btp","construction","emballage","packaging"],
    "institutionnel": ["défense","militaire","sécurité","naval","police","état"],
    "agroalimentaire": ["alimentation","restauration","food","sial","gastronomie"],
}

IMPORTANCE_RULES = {
    "high": ["eurosatory","sial","japan expo","maison & objet","intermat","première vision","global industrie"],
    "med": ["silmo","all4pack","euronaval","milipol","paris manga"],
}

def get_page(url, retries=3, delay=2.0):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            return BeautifulSoup(r.text, "lxml")
        except requests.RequestException as e:
            log.warning(f"Tentative {attempt+1}/{retries} échouée pour {url} : {e}")
            time.sleep(delay * (attempt + 1))
    log.error(f"Impossible de récupérer {url}")
    return None

def parse_french_date(text):
    mois = {"janvier":1,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,"juillet":7,"août":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12,"jan":1,"fév":2,"mar":3,"avr":4,"jun":6,"jul":7,"aoû":8,"sep":9,"oct":10,"nov":11,"déc":12}
    text = text.lower().strip()
    for pattern, fmt in [(r"(\d{4})-(\d{2})-(\d{2})", "%Y-%m-%d"),(r"(\d{2})/(\d{2})/(\d{4})", "%d/%m/%Y")]:
        m = re.search(pattern, text)
        if m:
            try:
                return datetime.strptime(m.group(0), fmt)
            except ValueError:
                pass
    m = re.search(r"(\d{1,2})\s+([a-zéûà]+)\.?\s+(\d{4})", text)
    if m:
        day, mon_str, year = m.groups()
        mon = mois.get(mon_str[:3]) or mois.get(mon_str)
        if mon:
            try:
                return datetime(int(year), mon, int(day))
            except ValueError:
                pass
    return None

def count_days(start, end):
    return max(1, (end - start).days + 1)

def has_weekend(start, end):
    d = start
    while d <= end:
        if d.weekday() >= 5:
            return True
        d += timedelta(days=1)
    return False

def infer_population(name, secteur):
    text = (name + " " + secteur).lower()
    found = [tag for tag, keywords in POPULATION_TAGS.items() if any(k in text for k in keywords)]
    return ", ".join(found) if found else "Professionnels divers"

def infer_importance(name):
    name_lower = name.lower()
    for imp, keywords in IMPORTANCE_RULES.items():
        if any(k in name_lower for k in keywords):
            return imp
    return "low"

def is_professional(name, event_type, secteur):
    text = (name + " " + event_type + " " + secteur).lower()
    return any(k in text for k in PRO_KEYWORDS)

def enrich_event(ev):
    start = ev.get("date_debut")
    end = ev.get("date_fin") or start
    if start and end:
        nb_jours = count_days(start, end)
        weekend = has_weekend(start, end)
        visitors_total = ev.get("visiteurs_total", 0)
        visitors_jour = round(visitors_total / nb_jours) if nb_jours > 0 else 0
    else:
        nb_jours = None
        weekend = None
        visitors_total = ev.get("visiteurs_total", 0)
        visitors_jour = 0
    nom = ev.get("nom", "")
    secteur = ev.get("secteur", "")
    etype = ev.get("type_evenement", "")
    return {
        "nom": nom,
        "lieu": ev.get("lieu", "Paris Nord Villepinte"),
        "date_debut": start.strftime("%Y-%m-%d") if start else None,
        "date_fin": end.strftime("%Y-%m-%d") if end else None,
        "nb_jours": nb_jours,
        "weekend_inclus": "Oui" if weekend else "Non",
        "type_evenement": etype,
        "secteur": secteur,
        "visiteurs_total": visitors_total,
        "visiteurs_par_jour": visitors_jour,
        "nb_places": ev.get("nb_places", None),
        "taux_remplissage": f"{round(visitors_total / ev['nb_places'] * 100)}%" if ev.get("nb_places") and visitors_total else None,
        "population": ev.get("population") or infer_population(nom, secteur),
        "evenement_pro": "Oui" if is_professional(nom, etype, secteur) else "Non",
        "importance": ev.get("importance") or infer_importance(nom),
        "description": ev.get("description", ""),
        "commentaire": ev.get("commentaire", ""),
        "periodicite": ev.get("periodicite", ""),
        "source": ev.get("source", ""),
        "date_scraping": datetime.now().strftime("%Y-%m-%d"),
    }

def scrape_viparis():
    log.info("Scraping Viparis...")
    url = "https://www.viparis.com/nos-lieux/paris-nord-villepinte/agenda"
    soup = get_page(url)
    if not soup:
        return []
    events = []
    cards = soup.find_all(["article","div"], class_=re.compile(r"event|agenda|card", re.I))
    for card in cards:
        try:
            title_tag = card.find(["h2","h3","h4","strong","a"])
            nom = title_tag.get_text(strip=True) if title_tag else ""
            if not nom or len(nom) < 3:
                continue
            card_text = card.get_text(" ", strip=True)
            dates = re.findall(r"\d{2}/\d{2}/\d{4}", card_text)
            start, end = None, None
            if len(dates) >= 2:
                start = parse_french_date(dates[0])
                end = parse_french_date(dates[1])
            elif len(dates) == 1:
                start = end = parse_french_date(dates[0])
            type_tag = card.find(class_=re.compile(r"type|category|badge|tag", re.I))
            etype = type_tag.get_text(strip=True) if type_tag else ""
            events.append({"nom":nom,"lieu":"Paris Nord Villepinte","date_debut":start,"date_fin":end,"type_evenement":etype,"secteur":etype,"source":"Viparis"})
        except Exception:
            continue
    if len(events) < 3:
        raw_text = soup.get_text(" ", strip=True)
        pattern = re.compile(r"([A-ZÀ-Ü][A-ZÀ-Üa-zà-ü &']{3,50})\s+du\s+(\d{2}/\d{2}/\d{4})\s+au\s+(\d{2}/\d{2}/\d{4})")
        for m in pattern.finditer(raw_text):
            nom, d1, d2 = m.groups()
            events.append({"nom":nom.strip(),"lieu":"Paris Nord Villepinte","date_debut":parse_french_date(d1),"date_fin":parse_french_date(d2),"type_evenement":"","secteur":"","source":"Viparis (fallback)"})
    log.info(f"Viparis : {len(events)} événements trouvés")
    return events

def scrape_eventseye():
    log.info("Scraping EventsEye...")
    url = "https://www.eventseye.com/fairs/pl0_salons_paris_11.html"
    soup = get_page(url)
    if not soup:
        return []
    events = []
    table = soup.find("table")
    if not table:
        return []
    for row in table.find_all("tr")[1:]:
        try:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            name_tag = cells[0].find("a")
            nom = name_tag.get_text(strip=True) if name_tag else cells[0].get_text(strip=True)
            desc_tag = cells[0].find("em") or cells[0].find("i")
            description = desc_tag.get_text(strip=True) if desc_tag else ""
            periodicite = cells[1].get_text(strip=True)
            date_text = cells[2].get_text(strip=True)
            date_match = re.search(r"(\d{2}/\d{2}/\d{4})", date_text)
            days_match = re.search(r"(\d+)\s*jours?", date_text)
            start = parse_french_date(date_match.group(1)) if date_match else None
            nb_days = int(days_match.group(1)) if days_match else 1
            end = start + timedelta(days=nb_days - 1) if start else None
            if not start:
                month_match = re.search(r"([a-zéûà]+)\.?\s+(\d{4})", date_text.lower())
                if month_match:
                    start = parse_french_date(f"01 {month_match.group(1)} {month_match.group(2)}")
                    end = start + timedelta(days=nb_days - 1) if start else None
            events.append({"nom":nom,"lieu":"Paris Nord Villepinte","date_debut":start,"date_fin":end,"type_evenement":"Expo Professionnelle","secteur":"","periodicite":periodicite,"description":description,"evenement_pro":"Oui","source":"EventsEye"})
        except Exception:
            continue
    log.info(f"EventsEye : {len(events)} événements trouvés")
    return events

def scrape_eventseye_p2():
    log.info("Scraping EventsEye page 2...")
    url = "https://www.eventseye.com/fairs/pl1_salons_paris_11.html"
    soup = get_page(url)
    if not soup:
        return []
    events = []
    table = soup.find("table")
    if not table:
        return []
    for row in table.find_all("tr")[1:]:
        try:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            name_tag = cells[0].find("a")
            nom = name_tag.get_text(strip=True) if name_tag else ""
            desc_tag = cells[0].find("em")
            description = desc_tag.get_text(strip=True) if desc_tag else ""
            periodicite = cells[1].get_text(strip=True)
            date_text = cells[2].get_text(strip=True)
            date_match = re.search(r"(\d{2}/\d{2}/\d{4})", date_text)
            days_match = re.search(r"(\d+)\s*jours?", date_text)
            start = parse_french_date(date_match.group(1)) if date_match else None
            nb_days = int(days_match.group(1)) if days_match else 1
            end = start + timedelta(days=nb_days - 1) if start else None
            if nom:
                events.append({"nom":nom,"lieu":"Paris Nord Villepinte","date_debut":start,"date_fin":end,"type_evenement":"Expo Professionnelle","periodicite":periodicite,"description":description,"source":"EventsEye (p2)"})
        except Exception:
            continue
    log.info(f"EventsEye p2 : {len(events)} événements trouvés")
    return events

def scrape_sortiraparis():
    log.info("Scraping SortiràParis...")
    url = "https://www.sortiraparis.com/lieux/53151-parc-des-expositions-paris-nord-villepinte"
    soup = get_page(url)
    if not soup:
        return []
    events = []
    articles = soup.find_all("article") or soup.find_all("div", class_=re.compile(r"article|event|card", re.I))
    for art in articles:
        try:
            title_tag = art.find(["h2","h3","h4"])
            nom = title_tag.get_text(strip=True) if title_tag else ""
            if not nom or len(nom) < 4:
                continue
            text = art.get_text(" ", strip=True)
            dates = re.findall(r"\d{1,2}\s+[a-zéûà]+\s+\d{4}", text, re.I)
            start, end = None, None
            if len(dates) >= 2:
                start = parse_french_date(dates[0])
                end = parse_french_date(dates[1])
            elif len(dates) == 1:
                start = end = parse_french_date(dates[0])
            link_tag = art.find("a", href=True)
            detail_url = link_tag["href"] if link_tag else ""
            if detail_url and not detail_url.startswith("http"):
                detail_url = "https://www.sortiraparis.com" + detail_url
            events.append({"nom":nom,"lieu":"Paris Nord Villepinte","date_debut":start,"date_fin":end,"type_evenement":"Expo Grand Public","secteur":"Loisirs / Culture","source":"SortiràParis","description":detail_url})
        except Exception:
            continue
    log.info(f"SortiràParis : {len(events)} événements trouvés")
    return events

def deduplicate(events):
    seen = {}
    for ev in events:
        key = re.sub(r"\W+", "", ev["nom"].lower())[:20]
        if key not in seen:
            seen[key] = ev
        else:
            for field in ["description","periodicite","population","commentaire","visiteurs_total","nb_places","secteur"]:
                if not seen[key].get(field) and ev.get(field):
                    seen[key][field] = ev[field]
    return list(seen.values())

def export(events):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not events:
        log.warning("Aucun événement à exporter.")
        return
    df = pd.DataFrame(events)
    cols = ["nom","lieu","date_debut","date_fin","nb_jours","weekend_inclus","type_evenement","secteur","periodicite","visiteurs_total","visiteurs_par_jour","nb_places","taux_remplissage","population","evenement_pro","importance","description","commentaire","source","date_scraping"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    writer = pd.ExcelWriter(OUTPUT_EXCEL, engine="xlsxwriter")
    df.to_excel(writer, index=False, sheet_name="Événements")
    wb = writer.book
    ws = writer.sheets["Événements"]
    header_fmt = wb.add_format({"bold":True,"bg_color":"#1a1a2e","font_color":"#FFFFFF","border":1,"align":"center","valign":"vcenter"})
    wrap_fmt = wb.add_format({"text_wrap":True,"valign":"top"})
    high_fmt = wb.add_format({"bg_color":"#FFE0E0","font_color":"#A32D2D","bold":True})
    med_fmt = wb.add_format({"bg_color":"#FFF3CD","font_color":"#854F0B"})
    col_widths = {"nom":28,"lieu":22,"date_debut":13,"date_fin":13,"nb_jours":8,"weekend_inclus":10,"type_evenement":18,"secteur":22,"periodicite":12,"visiteurs_total":14,"visiteurs_par_jour":13,"nb_places":12,"taux_remplissage":14,"population":32,"evenement_pro":10,"importance":12,"description":50,"commentaire":50,"source":18,"date_scraping":14}
    for i, col in enumerate(cols):
        ws.set_column(i, i, col_widths.get(col, 15), wrap_fmt if col in ["description","commentaire","population"] else None)
        ws.write(0, i, col.replace("_"," ").title(), header_fmt)
    imp_col = cols.index("importance")
    for row_idx in range(1, len(df) + 1):
        val = df.iloc[row_idx - 1]["importance"]
        if val == "high":
            ws.write(row_idx, imp_col, val, high_fmt)
        elif val == "med":
            ws.write(row_idx, imp_col, val, med_fmt)
    ws.freeze_panes(1, 0)
    ws.autofilter(0, 0, len(df), len(cols) - 1)
    ws.set_row(0, 22)
    writer.close()
    log.info(f"Excel exporté : {OUTPUT_EXCEL}")
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    log.info(f"CSV exporté : {OUTPUT_CSV}")
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"JSON exporté : {OUTPUT_JSON}")
    log.info(f"✅ Export terminé — {len(events)} événements")
