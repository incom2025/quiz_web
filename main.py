import os
import csv
import random
import secrets
import sqlite3
from datetime import datetime
from typing import Dict, List, Any, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Form, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates


# =========================
# Налаштування
# =========================
CSV_FILE = os.getenv("CSV_FILE", "questions.csv")
DB_FILE = os.getenv("DB_FILE", "results.db")

TEST_DURATION_SECONDS = int(os.getenv("TEST_DURATION_SECONDS", str(7 * 60)))
QUESTIONS_PER_TEST = int(os.getenv("QUESTIONS_PER_TEST", "10"))
ADMIN_KEY = os.getenv("ADMIN_KEY", "my-secret-key")

# Google Sheets (матриця)
GSHEET_ID = os.getenv("GSHEET_ID")                 # напр. 1k5zx1lwacPISJ5...
GSHEET_WORKSHEET = os.getenv("GSHEET_WORKSHEET")   # напр. Matrix
LESSON_ID_ENV = os.getenv("LESSON_ID")             # напр. 2026-02-24 або "Lesson_01"


# =========================
# База даних (резерв)
# =========================
def db_init():
    with sqlite3.connect(DB_FILE) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS results(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                lesson_id TEXT NOT NULL,
                surname TEXT NOT NULL,
                name TEXT NOT NULL,
                grp TEXT NOT NULL,
                score INTEGER NOT NULL,
                total INTEGER NOT NULL
            )
        """)
        con.commit()


def db_insert_result(lesson_id: str, surname: str, name: str, grp: str, score: int, total: int):
    with sqlite3.connect(DB_FILE) as con:
        con.execute(
            "INSERT INTO results(ts, lesson_id, surname, name, grp, score, total) VALUES(?,?,?,?,?,?,?)",
            (datetime.now().isoformat(timespec="seconds"), lesson_id, surname, name, grp, score, total),
        )
        con.commit()


def export_results_to_xlsx(xlsx_path: str = "results.xlsx") -> str:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = "Results"
    ws.append(["Timestamp", "Lesson", "Прізвище", "Ім'я", "Група", "Бали", "Всього"])

    with sqlite3.connect(DB_FILE) as con:
        rows = con.execute(
            "SELECT ts, lesson_id, surname, name, grp, score, total FROM results ORDER BY id DESC"
        ).fetchall()

    for r in rows:
        ws.append(list(r))

    wb.save(xlsx_path)
    return xlsx_path


# =========================
# Lifespan
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    db_init()
    yield


app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

SESSIONS: Dict[str, Dict[str, Any]] = {}


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
# Питання
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


def _lesson_id_default() -> str:
    # Якщо ви не передали lesson_id — робимо його як поточну дату
    return datetime.now().strftime("%Y-%m-%d")


def _safe_log(msg: str):
    # Render logs читає stdout
    print(msg, flush=True)


def write_to_google_matrix(
    lesson_id: str,
    surname: str,
    name: str,
    grp: str,
    score: int,
    total: int,
):
    """
    Пише у Google Sheet: 1 студент = 1 рядок, lesson_id = новий стовпець.
    """
    if not GSHEET_ID:
        _safe_log("GSHEET_ID is missing -> skip Google write")
        return

    try:
        from google_sheets_writer import upsert_score_by_lesson

        upsert_score_by_lesson(
            sheet_id=GSHEET_ID,
            lesson_id=lesson_id,
            surname=surname,
            name=name,
            grp=grp,
            score=score,
            total=total,
            worksheet_name=GSHEET_WORKSHEET,  # напр. "Matrix"
        )
        _safe_log(f"Google write OK: {surname} {name} {grp} lesson={lesson_id} score={score}/{total}")

    except Exception as e:
        # Важливо: не валимо тест студенту через Sheets
        _safe_log(f"Google write FAILED: {type(e).__name__}: {e}")


# =========================
# Маршрути
# =========================
@app.get("/", response_class=HTMLResponse)
def index(request: Request, lesson: Optional[str] = None):
    # lesson можна задавати так: https://.../?lesson=2026-02-24
    lesson_id = (lesson or LESSON_ID_ENV or _lesson_id_default()).strip()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "duration_sec": TEST_DURATION_SECONDS,
        "questions_per_test": QUESTIONS_PER_TEST,
        "lesson_id": lesson_id,  # якщо в шаблоні захочете показати/передати hidden input
    })


@app.post("/start")
def start(
    request: Request,
    surname: str = Form(...),
    name: str = Form(...),
    grp: str = Form(...),
    lesson_id: Optional[str] = Form(None),  # якщо додасте поле в index.html (або hidden)
):
    surname = surname.strip()
    name = name.strip()
    grp = grp.strip()

    if not surname or not name or not grp:
        return RedirectResponse("/", status_code=303)

    # lesson_id: форма -> env -> today
    if lesson_id:
        lid = lesson_id.strip()
    else:
        # можна ще читати з query: /start?lesson=...
        q_lesson = request.query_params.get("lesson")
        lid = (q_lesson or LESSON_ID_ENV or _lesson_id_default()).strip()

    all_q = load_questions_from_csv(CSV_FILE)
    picked = random.sample(all_q, QUESTIONS_PER_TEST)

    session_id = secrets.token_urlsafe(16)
    SESSIONS[session_id] = {
        "surname": surname,
        "name": name,
        "grp": grp,
        "lesson_id": lid,
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
        "lesson_id": sess.get("lesson_id"),
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

    lesson_id = (sess.get("lesson_id") or LESSON_ID_ENV or _lesson_id_default()).strip()
    total = len(questions)

    # 1) пишемо в SQLite (резерв)
    db_insert_result(lesson_id, sess["surname"], sess["name"], sess["grp"], score, total)

    # 2) пишемо в Google Sheets (матриця)
    write_to_google_matrix(
        lesson_id=lesson_id,
        surname=sess["surname"],
        name=sess["name"],
        grp=sess["grp"],
        score=score,
        total=total,
    )

    SESSIONS.pop(session_id, None)

    return templates.TemplateResponse("result.html", {
        "request": request,
        "score": score,
        "total": total,
    })


@app.get("/admin/export")
def admin_export(key: str = Query(...)):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    path = export_results_to_xlsx("results.xlsx")
    return FileResponse(path, filename="results.xlsx")


