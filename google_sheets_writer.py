import json
import os
import threading
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

BASE_COLS = ["Surname", "Name", "Group"]
TOTAL_COL_NAME = "TotalPoints"
ATTEND_COL_NAME = "Attendance"

# Один процес quiz_web -> серіалізуємо записи в Google Sheets.
# Це захищає від ситуації, коли десятки студентів одночасно
# намагаються додати один і той самий рядок/стовпець.
_WRITE_LOCK = threading.RLock()

# Кешуємо важкі об'єкти Google API, щоб не створювати Credentials
# і gspread.Client для кожного студента.
_GC = None
_SHEETS: dict[str, object] = {}
_WORKSHEETS: dict[tuple[str, str], object] = {}


def _get_client():
    global _GC

    if _GC is not None:
        return _GC

    sa_json = os.getenv("GOOGLE_SA_JSON")
    if not sa_json:
        raise RuntimeError("GOOGLE_SA_JSON env var is missing")

    creds_info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=SCOPES,
    )
    _GC = gspread.authorize(creds)
    return _GC


def _get_spreadsheet(sheet_id: str):
    sh = _SHEETS.get(sheet_id)
    if sh is None:
        sh = _get_client().open_by_key(sheet_id)
        _SHEETS[sheet_id] = sh
    return sh


def _get_worksheet(sheet_id: str, worksheet_name: Optional[str]):
    cache_name = worksheet_name or "__FIRST_SHEET__"
    key = (sheet_id, cache_name)

    ws = _WORKSHEETS.get(key)
    if ws is not None:
        return ws

    sh = _get_spreadsheet(sheet_id)

    if worksheet_name:
        try:
            ws = sh.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(
                title=worksheet_name,
                rows=200,
                cols=40,
            )
    else:
        ws = sh.sheet1

    _WORKSHEETS[key] = ws
    return ws


def _col_to_a1(col_num: int) -> str:
    """1 -> A, 2 -> B, 27 -> AA."""
    s = ""
    n = col_num
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _norm(x: str) -> str:
    return (x or "").strip().casefold()


def _normalize_row(row: list[str], width: int) -> list[str]:
    if len(row) < width:
        return row + [""] * (width - len(row))
    return row[:width]


def _ensure_base_header(ws, rows: list[list[str]]) -> list[str]:
    """
    Гарантує:
      A = Surname
      B = Name
      C = Group
      ... lesson columns ...
      TotalPoints
      Attendance

    Повертає актуальний header.
    """
    header = rows[0][:] if rows else []

    # Якщо таблиця порожня або перші три колонки неправильні,
    # відновлюємо базову структуру.
    if len(header) < 3 or header[:3] != BASE_COLS:
        ws.update(
            range_name="A1:C1",
            values=[BASE_COLS],
            value_input_option="RAW",
        )
        if len(header) < 3:
            header = _normalize_row(header, 3)
        header[:3] = BASE_COLS

    # Додаємо службові колонки тільки якщо їх ще немає.
    if TOTAL_COL_NAME not in header:
        col = len(header) + 1
        ws.update_cell(1, col, TOTAL_COL_NAME)
        header.append(TOTAL_COL_NAME)

    if ATTEND_COL_NAME not in header:
        col = len(header) + 1
        ws.update_cell(1, col, ATTEND_COL_NAME)
        header.append(ATTEND_COL_NAME)

    return header


def _find_student_row(
    rows: list[list[str]],
    surname: str,
    name: str,
    grp: str,
) -> Optional[int]:
    """Повертає номер рядка студента (починаючи з 2) або None."""
    ns = _norm(surname)
    nn = _norm(name)
    ng = _norm(grp)

    for idx, row in enumerate(rows[1:], start=2):
        a = _norm(row[0] if len(row) > 0 else "")
        b = _norm(row[1] if len(row) > 1 else "")
        c = _norm(row[2] if len(row) > 2 else "")

        if a == ns and b == nn and c == ng:
            return idx

    return None


def _ensure_capacity(ws, required_row: int, required_col: int):
    """Розширює аркуш лише коли це реально потрібно."""
    if required_row > ws.row_count:
        ws.add_rows(max(50, required_row - ws.row_count))

    if required_col > ws.col_count:
        ws.add_cols(max(10, required_col - ws.col_count))


def _ensure_lesson_col(
    ws,
    header: list[str],
    lesson_id: str,
) -> tuple[list[str], int, bool]:
    """
    Гарантує колонку lesson_id перед TotalPoints.

    Повертає:
      (оновлений_header, номер_колонки, чи_було_створено_нову_колонку)
    """
    if lesson_id in header:
        return header, header.index(lesson_id) + 1, False

    total_col = header.index(TOTAL_COL_NAME) + 1

    # Вставляємо урок безпосередньо перед TotalPoints.
    ws.insert_cols([[""]], col=total_col)
    ws.update_cell(1, total_col, lesson_id)

    header = header[:]
    header.insert(total_col - 1, lesson_id)

    return header, total_col, True


def _summary_formulas(
    row_num: int,
    total_col: int,
    attend_col: int,
    start_lesson_col: int = 4,
) -> tuple[str, str]:
    """Формули TotalPoints та Attendance для одного студента."""
    last_lesson_col = total_col - 1

    if last_lesson_col < start_lesson_col:
        return "0", "0"

    start_letter = _col_to_a1(start_lesson_col)
    end_letter = _col_to_a1(last_lesson_col)
    lesson_range = f"{start_letter}{row_num}:{end_letter}{row_num}"

    total_formula = f"=SUM({lesson_range})"
    attendance_formula = f'=COUNTIF({lesson_range},"<>")'
    return total_formula, attendance_formula


def _refresh_all_summary_formulas(
    ws,
    last_row: int,
    header: list[str],
    start_lesson_col: int = 4,
):
    """
    Оновлює формули ВСІХ студентів тільки тоді,
    коли з'явилася нова колонка заняття.

    У старій версії це виконувалось після КОЖНОГО студента,
    що створювало багато зайвих Google Sheets API операцій.
    """
    if last_row < 2:
        return

    total_col = header.index(TOTAL_COL_NAME) + 1
    attend_col = header.index(ATTEND_COL_NAME) + 1

    updates = []
    for r in range(2, last_row + 1):
        total_formula, attendance_formula = _summary_formulas(
            r,
            total_col,
            attend_col,
            start_lesson_col=start_lesson_col,
        )
        updates.extend(
            [
                {
                    "range": f"{_col_to_a1(total_col)}{r}",
                    "values": [[total_formula]],
                },
                {
                    "range": f"{_col_to_a1(attend_col)}{r}",
                    "values": [[attendance_formula]],
                },
            ]
        )

    if updates:
        ws.batch_update(
            updates,
            value_input_option="USER_ENTERED",
        )


def upsert_score_by_lesson(
    sheet_id: str,
    lesson_id: str,
    surname: str,
    name: str,
    grp: str,
    score: int,
    total: int,
    worksheet_name: str = None,
    write_as_fraction: bool = False,
):
    """
    Записує/оновлює оцінку студента за конкретне заняття.

    Оптимізовано для масового тестування:
      - повторно використовує gspread.Client;
      - повторно використовує Spreadsheet/Worksheet;
      - серіалізує конкурентні записи;
      - читає аркуш один раз на операцію;
      - не перераховує формули всіх студентів після кожного submit;
      - записує дані одного студента одним batch_update.
    """
    if not sheet_id:
        raise ValueError("sheet_id is required")
    if not lesson_id:
        raise ValueError("lesson_id is required")

    surname = (surname or "").strip()
    name = (name or "").strip()
    grp = (grp or "").strip()

    with _WRITE_LOCK:
        ws = _get_worksheet(sheet_id, worksheet_name)

        # Один read замість серії row_values()/col_values()/get_all_values().
        rows = ws.get_all_values()
        header = _ensure_base_header(ws, rows)

        # Після можливої корекції header синхронізуємо локальний rows[0].
        if rows:
            rows[0] = header[:]
        else:
            rows = [header[:]]

        header, lesson_col, lesson_created = _ensure_lesson_col(
            ws,
            header,
            lesson_id,
        )

        # Після insert_cols у локальних рядках теж "вставляємо" порожню клітинку,
        # щоб індекси відповідали актуальному аркушу.
        if lesson_created:
            insert_idx = lesson_col - 1
            for i in range(len(rows)):
                row = rows[i][:]
                if len(row) < insert_idx:
                    row.extend([""] * (insert_idx - len(row)))
                row.insert(insert_idx, "")
                rows[i] = row
            rows[0] = header[:]

        student_row = _find_student_row(
            rows,
            surname=surname,
            name=name,
            grp=grp,
        )

        if student_row is None:
            student_row = len(rows) + 1

        total_col = header.index(TOTAL_COL_NAME) + 1
        attend_col = header.index(ATTEND_COL_NAME) + 1

        max_col = max(lesson_col, total_col, attend_col, 3)
        _ensure_capacity(
            ws,
            required_row=student_row,
            required_col=max_col,
        )

        value = f"{score}/{total}" if write_as_fraction else score
        total_formula, attendance_formula = _summary_formulas(
            student_row,
            total_col,
            attend_col,
            start_lesson_col=4,
        )

        updates = []

        # Якщо студент новий — записуємо його ПІБ/групу.
        if student_row > len(rows):
            updates.append(
                {
                    "range": f"A{student_row}:C{student_row}",
                    "values": [[surname, name, grp]],
                }
            )

        # Оцінка + дві підсумкові формули.
        updates.extend(
            [
                {
                    "range": f"{_col_to_a1(lesson_col)}{student_row}",
                    "values": [[value]],
                },
                {
                    "range": f"{_col_to_a1(total_col)}{student_row}",
                    "values": [[total_formula]],
                },
                {
                    "range": f"{_col_to_a1(attend_col)}{student_row}",
                    "values": [[attendance_formula]],
                },
            ]
        )

        ws.batch_update(
            updates,
            value_input_option="USER_ENTERED",
        )

        # Якщо це перший результат нового заняття, один раз відновлюємо
        # формули інших студентів з урахуванням нової lesson-колонки.
        if lesson_created:
            last_row = max(len(rows), student_row)
            _refresh_all_summary_formulas(
                ws,
                last_row=last_row,
                header=header,
                start_lesson_col=4,
            )

        return {
            "ok": True,
            "row": student_row,
            "lesson_col": lesson_col,
            "score": score,
            "total": total,
        }


# =========================
# Постійна конфігурація застосунку
# =========================
SYSTEM_CONFIG_WORKSHEET = "SYSTEM_CONFIG"


def save_system_config(
    sheet_id: str,
    config: dict,
    worksheet_name: str = SYSTEM_CONFIG_WORKSHEET,
):
    """
    Повністю синхронізує службовий аркуш SYSTEM_CONFIG з поточною
    конфігурацією адміністратора.
    """
    if not sheet_id:
        raise ValueError("sheet_id is required")

    clean_config = {
        str(key).strip(): "" if value is None else str(value)
        for key, value in config.items()
        if str(key).strip()
    }

    with _WRITE_LOCK:
        ws = _get_worksheet(sheet_id, worksheet_name)
        rows = [["key", "value"]]
        rows.extend([[key, value] for key, value in clean_config.items()])

        _ensure_capacity(
            ws,
            required_row=max(2, len(rows)),
            required_col=2,
        )

        # Очищаємо лише A:B, щоб видалені/перейменовані параметри
        # не залишалися в SYSTEM_CONFIG.
        ws.batch_clear(["A:B"])
        ws.update(
            range_name=f"A1:B{len(rows)}",
            values=rows,
            value_input_option="RAW",
        )

    return {"ok": True, "count": len(clean_config)}


def load_system_config(
    sheet_id: str,
    worksheet_name: str = SYSTEM_CONFIG_WORKSHEET,
) -> dict[str, str]:
    """
    Читає SYSTEM_CONFIG. Якщо аркуша ще немає, повертає порожній словник.
    Саме порожній результат дозволяє main.py використати локальний SQLite
    або початкові значення при першому запуску.
    """
    if not sheet_id:
        return {}

    with _WRITE_LOCK:
        sh = _get_spreadsheet(sheet_id)
        try:
            ws = sh.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            return {}

        # Додаємо в кеш уже знайдений аркуш.
        _WORKSHEETS[(sheet_id, worksheet_name)] = ws
        rows = ws.get_all_values()

    result: dict[str, str] = {}
    for row in rows:
        if not row:
            continue

        key = (row[0] or "").strip()
        if not key or key.casefold() == "key":
            continue

        value = row[1] if len(row) > 1 else ""
        result[key] = str(value)

    return result
