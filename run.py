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
        headers = ["Nom","Lieu","Date début
