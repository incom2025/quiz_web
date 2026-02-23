import os
import json
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

BASE_COLS = ["Surname", "Name", "Group"]
TOTAL_COL_NAME = "TotalPoints"
ATTEND_COL_NAME = "Attendance"


def _get_client():
    sa_json = os.getenv("GOOGLE_SA_JSON")
    if not sa_json:
        raise RuntimeError("GOOGLE_SA_JSON env var is missing")

    creds_info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)


def _col_to_a1(col_num: int) -> str:
    """1 -> A, 2 -> B, 27 -> AA ..."""
    s = ""
    n = col_num
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _norm(x: str) -> str:
    return (x or "").strip().casefold()


def _ensure_header(ws):
    """Гарантує, що A1:C1 = Surname|Name|Group і додає TotalPoints/Attendance."""
    header = ws.row_values(1)

    # якщо пусто — створимо шапку
    if not header:
        ws.update("A1:C1", [BASE_COLS])
        header = ws.row_values(1)

    # якщо перші 3 колонки не такі — НЕ ламаємо автоматично (бо можна втратити дані),
    # але намагаємось "м'яко" привести: ставимо A1:C1 як треба.
    if len(header) < 3 or header[:3] != BASE_COLS:
        ws.update("A1:C1", [BASE_COLS])
        header = ws.row_values(1)

    # додамо TotalPoints, Attendance якщо їх нема
    header = ws.row_values(1)

    if TOTAL_COL_NAME not in header:
        ws.update_cell(1, len(header) + 1, TOTAL_COL_NAME)
        header = ws.row_values(1)

    if ATTEND_COL_NAME not in header:
        ws.update_cell(1, len(header) + 1, ATTEND_COL_NAME)
        header = ws.row_values(1)

    return header


def _find_student_row(ws, surname: str, name: str, grp: str) -> int | None:
    """Повертає номер рядка студента (2..N) або None."""
    all_rows = ws.get_all_values()
    for idx, row in enumerate(all_rows[1:], start=2):
        a = _norm(row[0] if len(row) > 0 else "")
        b = _norm(row[1] if len(row) > 1 else "")
        c = _norm(row[2] if len(row) > 2 else "")
        if a == _norm(surname) and b == _norm(name) and c == _norm(grp):
            return idx
    return None


def _append_student(ws, surname: str, name: str, grp: str) -> int:
    """Додає студента в кінець і повертає його рядок."""
    ws.append_row([surname, name, grp], value_input_option="USER_ENTERED")
    # надійно: останній непорожній рядок по колонці A
    vals_a = ws.col_values(1)
    return len(vals_a)


def _ensure_lesson_col(ws, header: list[str], lesson_id: str) -> tuple[list[str], int]:
    """
    Гарантує наявність колонки lesson_id.
    Повертає (оновлений header, col_index).
    """
    header = ws.row_values(1)

    # lesson-колонки повинні бути ПЕРЕД TotalPoints/Attendance.
    total_col = header.index(TOTAL_COL_NAME) + 1
    attend_col = header.index(ATTEND_COL_NAME) + 1

    if lesson_id in header:
        return header, header.index(lesson_id) + 1

    # вставляємо нову колонку перед TotalPoints
    insert_at = total_col  # позиція, куди вставити нову (на місце TotalPoints)
    ws.insert_cols([[""]], col=insert_at)
    ws.update_cell(1, insert_at, lesson_id)

    header = ws.row_values(1)
    return header, header.index(lesson_id) + 1


def _update_summary_formulas(ws, header: list[str], start_lesson_col: int = 4):
    """
    Оновлює формули TotalPoints/Attendance для всіх студентських рядків (від 2 до last_row).
    Lesson колонки: від D (4) до останньої lesson-колонки (перед TotalPoints).
    """
    header = ws.row_values(1)
    total_col = header.index(TOTAL_COL_NAME) + 1
    attend_col = header.index(ATTEND_COL_NAME) + 1

    last_row = len(ws.col_values(1))  # останній непорожній рядок у колонці A
    if last_row < 2:
        return

    # остання lesson-колонка = total_col - 1
    last_lesson_col = total_col - 1
    if last_lesson_col < start_lesson_col:
        return

    start_letter = _col_to_a1(start_lesson_col)
    end_letter = _col_to_a1(last_lesson_col)

    # робимо batch update для швидкості
    updates = []
    for r in range(2, last_row + 1):
        lesson_range = f"{start_letter}{r}:{end_letter}{r}"
        total_cell = f"{_col_to_a1(total_col)}{r}"
        attend_cell = f"{_col_to_a1(attend_col)}{r}"

        # TotalPoints: сума числових значень
        updates.append({"range": total_cell, "values": [[f"=SUM({lesson_range})"]]})

        # Attendance: кількість непорожніх клітинок (є запис)
        updates.append({"range": attend_cell, "values": [[f"=COUNTIF({lesson_range},\"<>\")"]]})

    ws.batch_update(updates, value_input_option="USER_ENTERED")


def upsert_score_by_lesson(
    sheet_id: str,
    lesson_id: str,
    surname: str,
    name: str,
    grp: str,
    score: int,
    total: int,
    worksheet_name: str = None,
    write_as_fraction: bool = False,  # якщо True -> "score/total"
):
    gc = _get_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name) if worksheet_name else sh.sheet1

    header = _ensure_header(ws)

    # 1) колонка заняття
    header, lesson_col = _ensure_lesson_col(ws, header, lesson_id)

    # 2) рядок студента
    student_row = _find_student_row(ws, surname, name, grp)
    if student_row is None:
        student_row = _append_student(ws, surname, name, grp)

    # 3) запис оцінки
    value = f"{score}/{total}" if write_as_fraction else score
    ws.update_cell(student_row, lesson_col, value)

    # 4) оновити формули підсумків
    _update_summary_formulas(ws, header, start_lesson_col=4)
