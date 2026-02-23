import os
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_client = None  # кеш клієнта, щоб не створювати кожен раз

def _get_client():
    global _client
    if _client is not None:
        return _client

    info = json.loads(os.environ["GOOGLE_SA_JSON"])
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    _client = gspread.authorize(creds)
    return _client

def append_result_row(surname: str, name: str, grp: str, score: int, total: int):
    """
    Пише рядок у перший аркуш таблиці.
    Колонки як у вас в Google Sheet:
    timestamp | Surname | Name | Group | Score | Total
    """
    sheet_id = os.environ["GSHEET_ID"]
    gc = _get_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.sheet1

    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    ws.append_row([ts, surname, name, grp, int(score), int(total)], value_input_option="USER_ENTERED")
