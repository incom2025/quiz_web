import os
import json
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

def _get_client():
    sa_json = os.getenv("GOOGLE_SA_JSON")
    if not sa_json:
        raise RuntimeError("GOOGLE_SA_JSON env var is missing")

    creds_info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)

def upsert_score_by_lesson(
    sheet_id: str,
    lesson_id: str,
    surname: str,
    name: str,
    grp: str,
    score: int,
    total: int,
    worksheet_name: str = None,
):
    gc = _get_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name) if worksheet_name else sh.sheet1

    # 1) Переконаємось, що перші 3 колонки — це Surname/Name/Group
    header = ws.row_values(1)
    if len(header) < 3 or header[:3] != ["Surname", "Name", "Group"]:
        # якщо шапки ще немає — створимо
        ws.update("A1:C1", [["Surname", "Name", "Group"]])
        header = ws.row_values(1)

    # 2) Знайдемо/створимо колонку під lesson_id
    header = ws.row_values(1)
    if lesson_id in header:
        col = header.index(lesson_id) + 1
    else:
        col = len(header) + 1
        ws.update_cell(1, col, lesson_id)

    # 3) Знайдемо рядок студента по (Surname, Name, Group)
    # беремо всі рядки A:C
    all_rows = ws.get_all_values()  # включно з header
    student_row = None
    for idx, row in enumerate(all_rows[1:], start=2):  # start=2 бо 1 — header
        a = (row[0] if len(row) > 0 else "").strip()
        b = (row[1] if len(row) > 1 else "").strip()
        c = (row[2] if len(row) > 2 else "").strip()
        if a == surname and b == name and c == grp:
            student_row = idx
            break

    # якщо немає — додаємо нового студента в кінець
    if student_row is None:
        ws.append_row([surname, name, grp], value_input_option="USER_ENTERED")
        student_row = ws.row_count  # last row (зазвичай так, але надійніше: знайти знову)
        # надійно: знайдемо останній непорожній рядок
        vals_a = ws.col_values(1)
        student_row = len(vals_a)

    # 4) Записуємо результат
    # варіант A: тільки бал (число)
    ws.update_cell(student_row, col, score)

    # варіант B: "бал/макс" (якщо хочете)
    # ws.update_cell(student_row, col, f"{score}/{total}")
