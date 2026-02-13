import os
import csv
import random
import secrets
import sqlite3
from datetime import datetime
from typing import Dict, List, Any

from fastapi import FastAPI, Request, Form, HTTPException
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

app = FastAPI()  # не вимикаємо docs — /docs має працювати
templates = Jinja2Templates(directory="templates")

# Сесії у памʼяті (для 1 інстансу достатньо)
SESSIONS: Dict[str, Dict[str, Any]] = {}


# =========================
# Helpers
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
        con.commit()


def db_insert_result(surname: str, name: str, grp: str, score: int, total: int):
    with sqlite3.connect(DB_FILE) as con:
        con.execute(
            "INSERT INTO results(ts, surname, name, grp, score, total) VALUES(?,?,?,?,?,?)",
            (datetime.now().isoformat(timespec="seconds"), surname, name, grp, score, total),
        )
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


@app.on_event("startup")
def startup():
    db_init()


# =========================
# Debug endpoints (щоб перевіряти, що це ТОЙ додаток)
# =========================
@app.get("/ping", response_class=PlainTextResponse)
def ping():
    return "OK"


# =========================
# Маршрути тесту
# =========================
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "duration_sec": TEST_DURATION_SECONDS,
        "questions_per_test": QUESTIONS_PER_TEST,
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
def submit(request: Request, session_id: str, answers: List[str] = Form(default=[])):
    sess = SESSIONS.get(session_id)
    if not sess:
        return RedirectResponse("/", status_code=303)

    questions = sess["questions"]
    score = 0

    for i, q in enumerate(questions):
        a = (answers[i] if i < len(answers) else "").strip().upper()
        if a == q["Prav_vid"]:
            score += 1

    db_insert_result(sess["surname"], sess["name"], sess["grp"], score, len(questions))
    SESSIONS.pop(session_id, None)

    return templates.TemplateResponse("result.html", {
        "request": request,
        "score": score,
        "total": len(questions),
    })


# =========================
# Адмін експорт
# =========================
@app.get("/admin/export")
def admin_export(key: str):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    path = export_results_to_xlsx("results.xlsx")
    return FileResponse(path, filename="results.xlsx")
