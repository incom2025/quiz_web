import os
import csv
import random
import secrets
import sqlite3
from datetime import datetime
from typing import Dict, List, Any, Optional
from contextlib import asynccontextmanager

import pytz
from fastapi import FastAPI, Request, Form, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

# Google Sheets writer (ваш файл поруч)
from google_sheets_writer import upsert_score_by_lesson


# =========================
# Налаштування
# =========================
CSV_FILE = os.getenv("CSV_FILE", "questions_lecture2_50_quizweb_letters.csv")
DB_FILE = os.getenv("DB_FILE", "results.db")
TEST_DURATION_SECONDS = int(os.getenv("TEST_DURATION_SECONDS", str(7 * 60)))
QUESTIONS_PER_TEST = int(os.getenv("QUESTIONS_PER_TEST", "10"))
ADMIN_KEY = os.getenv("ADMIN_KEY", "my-secret-key")

# Google Sheets env
GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
GSHEET_WORKSHEET = os.getenv("GSHEET_WORKSHEET", "").strip()  # напр. "Matrix"
LESSON_ID_ENV = os.getenv("LESSON_ID", "").strip()  # optional


# =========================
# База даних
# =========================
def db_init():
    with sqlite3.connect(DB_FILE) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS results(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                surname TEXT NOT NULL,
                name TEXT NOT NULL,
                grp TEXT NOT NULL,
                score INTEGER NOT NULL,
                total INTEGER NOT NULL
            )
        """)
        # settings: зберігаємо lesson_id (та інші налаштування)
        con.execute("""
            CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        con.commit()


def db_insert_result(surname: str, name: str, grp: str, score: int, total: int):
    with sqlite3.connect(DB_FILE) as con:
        con.execute(
            "INSERT INTO results(ts, surname, name, grp, score, total) VALUES(?,?,?,?,?,?)",
            (datetime.now().isoformat(timespec="seconds"), surname, name, grp, score, total),
        )
        con.commit()


def db_set_setting(key: str, value: str):
    with sqlite3.connect(DB_FILE) as con:
        con.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        con.commit()


def db_get_setting(key: str) -> Optional[str]:
    with sqlite3.connect(DB_FILE) as con:
        row = con.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None


def db_delete_setting(key: str):
    with sqlite3.connect(DB_FILE) as con:
        con.execute("DELETE FROM settings WHERE key = ?", (key,))
        con.commit()


def export_results_to_xlsx(xlsx_path: str = "results.xlsx") -> str:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = "Results"
    ws.append(["Timestamp", "Прізвище", "Ім'я", "Група", "Бали", "Всього"])

    with sqlite3.connect(DB_FILE) as con:
        rows = con.execute(
            "SELECT ts, surname, name, grp, score, total FROM results ORDER BY id DESC"
        ).fetchall()

    for r in rows:
        ws.append(list(r))

    wb.save(xlsx_path)
    return xlsx_path


# =========================
# Lifespan (startup/shutdown)
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    db_init()
    yield


app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

SESSIONS: Dict[str, Dict[str, Any]] = {}


# =========================
# Доступ за часом (кожний четвер 16:15–16:30, Europe/Kyiv)
# =========================
def is_test_time() -> bool:
    tz = pytz.timezone("Europe/Kyiv")
    now = datetime.now(tz)

    # 3 = четвер (понеділок = 0)
    if now.weekday() != 3:
        return False

    current_minutes = now.hour * 60 + now.minute
    start_minutes = 16 * 60 + 15   # 16:15
    end_minutes = 16 * 60 + 30     # 16:30

    return start_minutes <= current_minutes <= end_minutes


@app.middleware("http")
async def restrict_time(request: Request, call_next):
    # Завжди дозволяємо технічні та адмін-ендпоінти
    if request.url.path == "/ping" or request.url.path == "/routes" or request.url.path.startswith("/admin"):
        return await call_next(request)

    # Блокуємо все інше поза вікном часу
    if not is_test_time():
        return HTMLResponse(
            """
            <h2>Тест зараз недоступний</h2>
            <p>Доступ відкривається кожного четверга з 16:15 до 16:30 (за київським часом).</p>
            """,
            status_code=403,
        )

    return await call_next(request)


# =========================
# Допоміжне
# =========================
def _get_lesson_id() -> str:
    # 1) lesson_id, який виставили через /admin/set_lesson (в SQLite)
    db_val = db_get_setting("lesson_id")
    if db_val and db_val.strip():
        return db_val.strip()

    # 2) optional env LESSON_ID
    if LESSON_ID_ENV:
        return LESSON_ID_ENV

    # 3) fallback: сьогоднішня дата
    return datetime.now().strftime("%Y-%m-%d")


def _sheets_enabled() -> bool:
    return bool(GSHEET_ID and GSHEET_WORKSHEET)


def _admin_check(key: str):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")


# =========================
# Debug endpoints
# =========================
@app.get("/ping", response_class=PlainTextResponse)
def ping():
    return "ok"


@app.get("/routes")
def routes():
    out = []
    for r in app.routes:
        methods = ",".join(sorted(getattr(r, "methods", []) or []))
        out.append({
            "path": getattr(r, "path", ""),
            "methods": methods,
            "name": getattr(r, "name", "")
        })
    return out


# =========================
# Робота з питаннями
# =========================
def load_questions_from_csv(path: str) -> List[dict]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Не знайдено файл з питаннями: {path}")

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"Question", "A", "B", "C", "D", "Prav_vid"}

        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(
                f"CSV має містити колонки: {', '.join(sorted(required))}. "
                f"Зараз є: {reader.fieldnames}"
            )

        questions = []
        for row in reader:
            pv = (row.get("Prav_vid") or "").strip().upper()
            if pv not in {"A", "B", "C", "D"}:
                continue

            q = {
                "Question": (row.get("Question") or "").strip(),
                "A": (row.get("A") or "").strip(),
                "B": (row.get("B") or "").strip(),
                "C": (row.get("C") or "").strip(),
                "D": (row.get("D") or "").strip(),
                "Prav_vid": pv,
            }

            if q["Question"] and all(q[k] for k in ["A", "B", "C", "D"]):
                questions.append(q)

        if len(questions) < QUESTIONS_PER_TEST:
            raise ValueError(
                f"У CSV замало коректних питань: {len(questions)}. "
                f"Потрібно щонайменше {QUESTIONS_PER_TEST}."
            )

        return questions


# =========================
# Маршрути
# =========================
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "duration_sec": TEST_DURATION_SECONDS,
        "questions_per_test": QUESTIONS_PER_TEST
    })


@app.post("/start")
def start(
    surname: str = Form(...),
    name: str = Form(...),
    grp: str = Form(...),
):
    surname = surname.strip()
    name = name.strip()
    grp = grp.strip()

    if not surname or not name or not grp:
        return RedirectResponse("/", status_code=303)

    all_q = load_questions_from_csv(CSV_FILE)
    picked = random.sample(all_q, QUESTIONS_PER_TEST)

    session_id = secrets.token_urlsafe(16)
    SESSIONS[session_id] = {
        "surname": surname,
        "name": name,
        "grp": grp,
        "questions": picked,
        "started": datetime.now().timestamp(),
    }

    return RedirectResponse(f"/quiz/{session_id}", status_code=303)


@app.get("/quiz/{session_id}", response_class=HTMLResponse)
def quiz(request: Request, session_id: str):
    sess = SESSIONS.get(session_id)
    if not sess:
        return RedirectResponse("/", status_code=303)

    elapsed = int(datetime.now().timestamp() - sess["started"])
    remaining = max(0, TEST_DURATION_SECONDS - elapsed)

    return templates.TemplateResponse("quiz.html", {
        "request": request,
        "session_id": session_id,
        "surname": sess["surname"],
        "name": sess["name"],
        "grp": sess["grp"],
        "questions": sess["questions"],
        "remaining": remaining,
    })


@app.post("/submit/{session_id}", response_class=HTMLResponse)
async def submit(request: Request, session_id: str):
    sess = SESSIONS.get(session_id)
    if not sess:
        return RedirectResponse("/", status_code=303)

    form = await request.form()
    questions = sess["questions"]
    score = 0

    for i, q in enumerate(questions):
        a = (form.get(f"q{i}") or "").strip().upper()
        if a == q["Prav_vid"]:
            score += 1

    # 1) SQLite (локально на Render)
    db_insert_result(sess["surname"], sess["name"], sess["grp"], score, len(questions))

    # 2) Google Sheets Matrix (75 рядків, кожний урок = колонка)
    if _sheets_enabled():
        try:
            lesson_id = _get_lesson_id()
            upsert_score_by_lesson(
                sheet_id=GSHEET_ID,
                lesson_id=lesson_id,
                surname=sess["surname"],
                name=sess["name"],
                grp=sess["grp"],
                score=score,
                total=len(questions),
                worksheet_name=GSHEET_WORKSHEET,
            )
        except Exception as e:
            # не валимо тест студенту, просто лог в консоль Render
            print(f"[Sheets] write failed: {type(e).__name__}: {e}")
    else:
        print("[Sheets] disabled: GSHEET_ID or GSHEET_WORKSHEET missing")

    SESSIONS.pop(session_id, None)

    return templates.TemplateResponse("result.html", {
        "request": request,
        "score": score,
        "total": len(questions),
    })


# =========================
# Admin endpoints
# =========================
@app.get("/admin/set_lesson")
def admin_set_lesson(
    key: str = Query(...),
    lesson: str = Query(...),
):
    _admin_check(key)

    lesson = lesson.strip()
    if not lesson:
        raise HTTPException(status_code=400, detail="lesson is empty")

    db_set_setting("lesson_id", lesson)
    return {"ok": True, "lesson_id": lesson}


@app.get("/admin/set_lesson_today")
def admin_set_lesson_today(key: str = Query(...)):
    _admin_check(key)

    lesson = datetime.now().strftime("%Y-%m-%d")
    db_set_setting("lesson_id", lesson)
    return {"ok": True, "lesson_id": lesson}


@app.get("/admin/get_lesson")
def admin_get_lesson(key: str = Query(...)):
    _admin_check(key)

    return {
        "lesson_id_db": db_get_setting("lesson_id"),
        "lesson_id_env": LESSON_ID_ENV,
        "lesson_id_effective": _get_lesson_id(),
        "worksheet": GSHEET_WORKSHEET,
        "gsheet_id_set": bool(GSHEET_ID),
        "sheets_enabled": _sheets_enabled(),
    }


@app.get("/admin/clear_lesson")
def admin_clear_lesson(key: str = Query(...)):
    _admin_check(key)

    db_delete_setting("lesson_id")
    return {"ok": True, "lesson_id_db": None}


@app.get("/admin/export")
def admin_export(key: str = Query(...)):
    _admin_check(key)

    path = export_results_to_xlsx("results.xlsx")
    return FileResponse(path, filename="results.xlsx")



