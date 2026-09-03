import os
import csv
import random
import secrets
import sqlite3
import threading
import asyncio
import time
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Any, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Form, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

from google_sheets_writer import upsert_score_by_lesson


# =========================
# Налаштування за замовчуванням
# =========================
CSV_FILE = os.getenv("CSV_FILE", "questions_lecture2_50_quizweb_letters.csv")
DB_FILE = os.getenv("DB_FILE", "results.db")
TEST_DURATION_SECONDS = int(os.getenv("TEST_DURATION_SECONDS", str(7 * 60)))
QUESTIONS_PER_TEST = int(os.getenv("QUESTIONS_PER_TEST", "10"))
ADMIN_KEY = os.getenv("ADMIN_KEY", "my-secret-key")

GSHEET_ID = os.getenv("GSHEET_ID", "").strip()
LESSON_ID_ENV = os.getenv("LESSON_ID", "").strip()

# Оптимізація для малих Render-інстансів
CONFIG_CACHE_SECONDS = float(os.getenv("CONFIG_CACHE_SECONDS", "2"))
SESSION_GRACE_SECONDS = int(os.getenv("SESSION_GRACE_SECONDS", str(15 * 60)))
SQLITE_BUSY_TIMEOUT_MS = int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "10000"))

KYIV_TZ = ZoneInfo("Europe/Kyiv")


def now_kyiv() -> datetime:
    return datetime.now(KYIV_TZ).replace(tzinfo=None)


# =========================
# База даних
# =========================
def db_connect():
    """SQLite-підключення з очікуванням блокування під час масових submit-запитів."""
    con = sqlite3.connect(DB_FILE, timeout=max(1.0, SQLITE_BUSY_TIMEOUT_MS / 1000.0))
    con.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    return con


def add_column_if_not_exists(con, table_name, column_name, column_type):
    columns = con.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing_columns = [col[1] for col in columns]

    if column_name not in existing_columns:
        con.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        )


def db_init():
    with db_connect() as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
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

        con.execute("""
            CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS test_config(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                academic_year TEXT NOT NULL,
                semester TEXT NOT NULL,
                discipline_name TEXT NOT NULL,
                lecture_number TEXT NOT NULL,
                test_date TEXT NOT NULL,
                weekday TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                duration_minutes INTEGER NOT NULL,
                questions_count INTEGER NOT NULL,
                csv_file TEXT NOT NULL,
                results_db TEXT NOT NULL,
                worksheet_name TEXT NOT NULL,
                teams_group TEXT,
                test_link_name TEXT,
                lesson_id TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)

        add_column_if_not_exists(con, "results", "discipline_name", "TEXT")
        add_column_if_not_exists(con, "results", "lecture_number", "TEXT")
        add_column_if_not_exists(con, "results", "academic_year", "TEXT")
        add_column_if_not_exists(con, "results", "semester", "TEXT")
        add_column_if_not_exists(con, "results", "worksheet_name", "TEXT")
        add_column_if_not_exists(con, "results", "lesson_id", "TEXT")
        add_column_if_not_exists(con, "results", "test_start_time", "TEXT")
        add_column_if_not_exists(con, "results", "test_end_time", "TEXT")

        add_column_if_not_exists(con, "test_config", "end_time", "TEXT")

        con.commit()


def db_set_setting(key: str, value: str):
    with db_connect() as con:
        con.execute(
            """
            INSERT INTO settings(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
        con.commit()
    try:
        _invalidate_config_cache()
    except NameError:
        pass


def db_get_setting(key: str) -> Optional[str]:
    with db_connect() as con:
        row = con.execute(
            "SELECT value FROM settings WHERE key = ?",
            (key,)
        ).fetchone()
        return row[0] if row else None


def db_delete_setting(key: str):
    with db_connect() as con:
        con.execute("DELETE FROM settings WHERE key = ?", (key,))
        con.commit()
    try:
        _invalidate_config_cache()
    except NameError:
        pass


def db_insert_test_config(config: Dict[str, Any]):
    with db_connect() as con:
        con.execute("""
            INSERT INTO test_config(
                academic_year,
                semester,
                discipline_name,
                lecture_number,
                test_date,
                weekday,
                start_time,
                end_time,
                duration_minutes,
                questions_count,
                csv_file,
                results_db,
                worksheet_name,
                teams_group,
                test_link_name,
                lesson_id,
                created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            config["academic_year"],
            config["semester"],
            config["discipline_name"],
            config["lecture_number"],
            config["test_date"],
            config["weekday"],
            config["start_time"],
            config["end_time"],
            config["duration_minutes"],
            config["questions_count"],
            config["csv_file"],
            config["results_db"],
            config["worksheet_name"],
            config["teams_group"],
            config["test_link_name"],
            config["lesson_id"],
            now_kyiv().isoformat(timespec="seconds"),
        ))
        con.commit()


def db_insert_result(
    surname: str,
    name: str,
    grp: str,
    score: int,
    total: int,
    config: Dict[str, str],
):
    with db_connect() as con:
        con.execute("""
            INSERT INTO results(
                ts,
                surname,
                name,
                grp,
                score,
                total,
                discipline_name,
                lecture_number,
                academic_year,
                semester,
                worksheet_name,
                lesson_id,
                test_start_time,
                test_end_time
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_kyiv().isoformat(timespec="seconds"),
            surname,
            name,
            grp,
            score,
            total,
            config["discipline_name"],
            config["lecture_number"],
            config["academic_year"],
            config["semester"],
            config["worksheet_name"],
            config["lesson_id"],
            config["start_time"],
            config["end_time"],
        ))
        con.commit()


def export_results_to_xlsx(xlsx_path: str = "results.xlsx") -> str:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = "Results"

    ws.append([
        "Timestamp",
        "Прізвище",
        "Ім'я",
        "Група",
        "Бали",
        "Всього",
        "Дисципліна",
        "Лекція",
        "Навчальний рік",
        "Семестр",
        "Лист результатів",
        "Lesson ID",
        "Початок сеансу",
        "Кінець сеансу",
    ])

    with db_connect() as con:
        rows = con.execute("""
            SELECT
                ts,
                surname,
                name,
                grp,
                score,
                total,
                discipline_name,
                lecture_number,
                academic_year,
                semester,
                worksheet_name,
                lesson_id,
                test_start_time,
                test_end_time
            FROM results
            ORDER BY id DESC
        """).fetchall()

    for row in rows:
        ws.append(list(row))

    wb.save(xlsx_path)
    return xlsx_path


# =========================
# Кеші та обмеження ресурсів
# =========================
_CONFIG_CACHE: Dict[str, Any] = {"value": None, "loaded_at": 0.0}
_CONFIG_LOCK = threading.Lock()
_QUESTION_CACHE: Dict[str, Dict[str, Any]] = {}
_QUESTION_CACHE_LOCK = threading.Lock()
_SHEETS_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="gsheets")


# =========================
# Lifespan
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    db_init()
    try:
        yield
    finally:
        _SHEETS_EXECUTOR.shutdown(wait=False, cancel_futures=False)


app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

SESSIONS: Dict[str, Dict[str, Any]] = {}


# =========================
# Допоміжні функції
# =========================
def _admin_check(key: str):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")


def _safe_name(value: str) -> str:
    value = value.strip()
    value = value.replace("/", "-")
    value = value.replace("\\", "-")
    value = value.replace(":", "-")
    value = value.replace("*", "")
    value = value.replace("?", "")
    value = value.replace("[", "(")
    value = value.replace("]", ")")
    return value


def make_worksheet_name(
    academic_year: str,
    semester: str,
    discipline_name: str
) -> str:
    discipline = _safe_name(discipline_name)
    return f"{academic_year}_{semester}_семестр_{discipline}"


def make_lesson_id(
    academic_year: str,
    semester: str,
    discipline_name: str,
    lecture_number: str
) -> str:
    discipline = _safe_name(discipline_name).replace(" ", "_")
    return f"{academic_year}_sem_{semester}_{discipline}_lecture_{lecture_number}"


def _invalidate_config_cache():
    with _CONFIG_LOCK:
        _CONFIG_CACHE["value"] = None
        _CONFIG_CACHE["loaded_at"] = 0.0


def _read_all_settings() -> Dict[str, str]:
    with db_connect() as con:
        rows = con.execute("SELECT key, value FROM settings").fetchall()
    return {str(k): str(v) for k, v in rows}


def get_current_config() -> Dict[str, str]:
    now_mono = time.monotonic()
    with _CONFIG_LOCK:
        cached = _CONFIG_CACHE.get("value")
        loaded_at = float(_CONFIG_CACHE.get("loaded_at") or 0.0)
        if cached is not None and now_mono - loaded_at < CONFIG_CACHE_SECONDS:
            return dict(cached)

    settings = _read_all_settings()
    academic_year = settings.get("academic_year") or "2025-2026"
    semester = settings.get("semester") or "2"
    discipline_name = settings.get("discipline_name") or "Професійний Python"
    lecture_number = settings.get("lecture_number") or "1"
    worksheet_name = settings.get("worksheet_name") or make_worksheet_name(academic_year, semester, discipline_name)
    lesson_id = settings.get("lesson_id") or make_lesson_id(academic_year, semester, discipline_name, lecture_number)

    config = {
        "academic_year": academic_year,
        "semester": semester,
        "discipline_name": discipline_name,
        "lecture_number": lecture_number,
        "test_date": settings.get("test_date") or now_kyiv().strftime("%Y-%m-%d"),
        "weekday": settings.get("weekday") or "Понеділок",
        "start_time": settings.get("start_time") or "09:00",
        "end_time": settings.get("end_time") or "10:00",
        "duration_minutes": settings.get("duration_minutes") or str(TEST_DURATION_SECONDS // 60),
        "questions_count": settings.get("questions_count") or str(QUESTIONS_PER_TEST),
        "csv_file": settings.get("csv_file") or CSV_FILE,
        "results_db": settings.get("results_db") or DB_FILE,
        "worksheet_name": worksheet_name,
        "teams_group": settings.get("teams_group") or "",
        "test_link_name": settings.get("test_link_name") or "",
        "lesson_id": lesson_id,
    }
    with _CONFIG_LOCK:
        _CONFIG_CACHE["value"] = dict(config)
        _CONFIG_CACHE["loaded_at"] = now_mono
    return config


def _get_lesson_id() -> str:
    config = get_current_config()
    if config["lesson_id"]:
        return config["lesson_id"]

    if LESSON_ID_ENV:
        return LESSON_ID_ENV

    return now_kyiv().strftime("%Y-%m-%d")


def _sheets_enabled() -> bool:
    config = get_current_config()
    worksheet = config.get("worksheet_name", "")
    return bool(GSHEET_ID and worksheet)


def get_questions_per_test() -> int:
    config = get_current_config()
    try:
        return int(config["questions_count"])
    except ValueError:
        return QUESTIONS_PER_TEST


def get_test_duration_seconds() -> int:
    config = get_current_config()
    try:
        return int(config["duration_minutes"]) * 60
    except ValueError:
        return TEST_DURATION_SECONDS


def get_csv_file() -> str:
    config = get_current_config()
    return config["csv_file"] or CSV_FILE


def is_testing_session_open(config: Dict[str, str]) -> bool:
    now = now_kyiv()

    session_start = datetime.strptime(
        f"{config['test_date']} {config['start_time']}",
        "%Y-%m-%d %H:%M"
    )

    session_end = datetime.strptime(
        f"{config['test_date']} {config['end_time']}",
        "%Y-%m-%d %H:%M"
    )

    print("KYIV NOW =", now)
    print("SESSION START =", session_start)
    print("SESSION END =", session_end)

    return session_start <= now <= session_end


def validate_session_time_config(
    start_time: str,
    end_time: str,
    duration_minutes: int
):
    start_dt = datetime.strptime(start_time, "%H:%M")
    end_dt = datetime.strptime(end_time, "%H:%M")

    if end_dt <= start_dt:
        raise HTTPException(
            status_code=400,
            detail="Час завершення сеансу тестування має бути більшим за час початку."
        )

    session_minutes = int((end_dt - start_dt).total_seconds() // 60)

    if session_minutes < duration_minutes:
        raise HTTPException(
            status_code=400,
            detail="Проміжок між початком і завершенням сеансу має бути не менший за тривалість тесту."
        )


def student_already_passed(
    surname: str,
    name: str,
    grp: str,
    lesson_id: str
) -> bool:
    with db_connect() as con:
        row = con.execute("""
            SELECT id FROM results
            WHERE lower(trim(surname)) = lower(trim(?))
              AND lower(trim(name)) = lower(trim(?))
              AND lower(trim(grp)) = lower(trim(?))
              AND lesson_id = ?
            LIMIT 1
        """, (
            surname,
            name,
            grp,
            lesson_id
        )).fetchone()

    return row is not None


# =========================
# Debug endpoints
# =========================
@app.get("/ping", response_class=PlainTextResponse)
def ping():
    return "ok"


@app.get("/routes")
def routes():
    out = []
    for route in app.routes:
        methods = ",".join(sorted(getattr(route, "methods", []) or []))
        out.append({
            "path": getattr(route, "path", ""),
            "methods": methods,
            "name": getattr(route, "name", "")
        })
    return out


# =========================
# Робота з питаннями
# =========================
def load_questions_from_csv(path: str, questions_per_test: int) -> List[dict]:
    """Читає CSV один раз і перевикористовує банк, доки файл не зміниться."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Не знайдено файл з питаннями: {path}")

    stat = os.stat(path)
    signature = (stat.st_mtime_ns, stat.st_size)
    with _QUESTION_CACHE_LOCK:
        cached = _QUESTION_CACHE.get(path)
        if cached and cached.get("signature") == signature:
            questions = cached["questions"]
            if len(questions) < questions_per_test:
                raise ValueError(f"У CSV замало коректних питань: {len(questions)}. Потрібно щонайменше {questions_per_test}.")
            return questions

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"Question", "A", "B", "C", "D", "Prav_vid"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(f"CSV має містити колонки: {', '.join(sorted(required))}. Зараз є: {reader.fieldnames}")

        questions: List[dict] = []
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

    if len(questions) < questions_per_test:
        raise ValueError(f"У CSV замало коректних питань: {len(questions)}. Потрібно щонайменше {questions_per_test}.")

    with _QUESTION_CACHE_LOCK:
        _QUESTION_CACHE[path] = {"signature": signature, "questions": questions}
    return questions


def cleanup_expired_sessions() -> int:
    """Звільняє RAM від сесій студентів, які покинули тест і не натиснули 'Завершити'."""
    now_ts = now_kyiv().timestamp()
    expired = []
    for sid, sess in list(SESSIONS.items()):
        try:
            duration = int(sess["config"]["duration_minutes"]) * 60
        except Exception:
            duration = TEST_DURATION_SECONDS
        if now_ts - float(sess.get("started", now_ts)) > duration + SESSION_GRACE_SECONDS:
            expired.append(sid)
    for sid in expired:
        SESSIONS.pop(sid, None)
    return len(expired)


# =========================
# Основні маршрути тестування
# =========================
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    config = get_current_config()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "duration_sec": get_test_duration_seconds(),
        "questions_per_test": get_questions_per_test(),
        "config": config,
    })


@app.post("/start")
def start(
    surname: str = Form(...),
    name: str = Form(...),
    grp: str = Form(...),
):
    cleanup_expired_sessions()

    surname = surname.strip()
    name = name.strip()
    grp = grp.strip()

    if not surname or not name or not grp:
        return RedirectResponse("/", status_code=303)

    config = get_current_config()

    if not is_testing_session_open(config):
        raise HTTPException(
            status_code=403,
            detail=(
                "Тестування доступне лише у визначений лектором час: "
                f"{config['test_date']} з {config['start_time']} до {config['end_time']}."
            )
        )

    if student_already_passed(surname, name, grp, config["lesson_id"]):
        raise HTTPException(
            status_code=403,
            detail="Ви вже проходили це тестування. Повторна спроба заборонена."
        )

    questions_per_test = get_questions_per_test()
    csv_file = get_csv_file()

    all_q = load_questions_from_csv(csv_file, questions_per_test)
    picked = random.sample(all_q, questions_per_test)

    session_id = secrets.token_urlsafe(16)

    SESSIONS[session_id] = {
        "surname": surname,
        "name": name,
        "grp": grp,
        "questions": picked,
        "started": now_kyiv().timestamp(),
        "config": config,
    }

    return RedirectResponse(f"/quiz/{session_id}", status_code=303)


@app.get("/quiz/{session_id}", response_class=HTMLResponse)
def quiz(request: Request, session_id: str):
    cleanup_expired_sessions()
    sess = SESSIONS.get(session_id)

    if not sess:
        return RedirectResponse("/", status_code=303)

    duration = int(sess["config"]["duration_minutes"]) * 60
    elapsed = int(now_kyiv().timestamp() - sess["started"])
    remaining = max(0, duration - elapsed)

    return templates.TemplateResponse("quiz.html", {
        "request": request,
        "session_id": session_id,
        "surname": sess["surname"],
        "name": sess["name"],
        "grp": sess["grp"],
        "questions": sess["questions"],
        "remaining": remaining,
        "config": sess["config"],
    })


@app.post("/submit/{session_id}", response_class=HTMLResponse)
async def submit(request: Request, session_id: str):
    cleanup_expired_sessions()
    sess = SESSIONS.get(session_id)

    if not sess:
        return RedirectResponse("/", status_code=303)
    if sess.get("submitting"):
        raise HTTPException(status_code=409, detail="Результат уже обробляється.")
    sess["submitting"] = True

    form = await request.form()
    questions = sess["questions"]
    score = 0

    for i, q in enumerate(questions):
        answer = (form.get(f"q{i}") or "").strip().upper()

        if answer == q["Prav_vid"]:
            score += 1

    config = sess["config"]

    db_insert_result(
        sess["surname"],
        sess["name"],
        sess["grp"],
        score,
        len(questions),
        config,
    )

    if _sheets_enabled():
        try:
            # Один Google Sheets запис одночасно. Інші submit не блокують event loop Uvicorn.
            loop = asyncio.get_running_loop()
            job = partial(
                upsert_score_by_lesson,
                sheet_id=GSHEET_ID,
                lesson_id=config["test_date"],
                surname=sess["surname"],
                name=sess["name"],
                grp=sess["grp"],
                score=score,
                total=len(questions),
                worksheet_name=config["worksheet_name"],
            )
            await loop.run_in_executor(_SHEETS_EXECUTOR, job)
        except Exception as e:
            print(f"[Sheets] write failed: {type(e).__name__}: {e}")
    else:
        print("[Sheets] disabled: GSHEET_ID or worksheet_name missing")

    SESSIONS.pop(session_id, None)

    return templates.TemplateResponse("result.html", {
        "request": request,
        "score": score,
        "total": len(questions),
        "config": config,
    })


# =========================
# Панель лектора
# =========================
@app.get("/admin/config", response_class=HTMLResponse)
def admin_config_page(request: Request, key: str = Query(...)):
    _admin_check(key)

    settings = get_current_config()

    return templates.TemplateResponse("admin_config.html", {
        "request": request,
        "settings": settings,
        "key": key,
    })


@app.post("/admin/config/save", response_class=HTMLResponse)
async def admin_config_save(
    request: Request,
    key: str = Form(...),
    academic_year: str = Form(...),
    semester: str = Form(...),
    discipline_name: str = Form(...),
    lecture_number: str = Form(...),
    test_date: str = Form(...),
    weekday: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
    duration_minutes: int = Form(...),
    questions_count: int = Form(...),
    csv_file: str = Form(...),
    results_db: str = Form(...),
    teams_group: str = Form(""),
    test_link_name: str = Form(""),
):
    _admin_check(key)

    academic_year = academic_year.strip()
    semester = semester.strip()
    discipline_name = discipline_name.strip()
    lecture_number = lecture_number.strip()
    test_date = test_date.strip()
    weekday = weekday.strip()
    start_time = start_time.strip()
    end_time = end_time.strip()
    csv_file = csv_file.strip()
    results_db = results_db.strip()
    teams_group = teams_group.strip()
    test_link_name = test_link_name.strip()

    validate_session_time_config(
        start_time=start_time,
        end_time=end_time,
        duration_minutes=duration_minutes,
    )

    worksheet_name = make_worksheet_name(
        academic_year,
        semester,
        discipline_name
    )

    lesson_id = make_lesson_id(
        academic_year,
        semester,
        discipline_name,
        lecture_number
    )

    config = {
        "academic_year": academic_year,
        "semester": semester,
        "discipline_name": discipline_name,
        "lecture_number": lecture_number,
        "test_date": test_date,
        "weekday": weekday,
        "start_time": start_time,
        "end_time": end_time,
        "duration_minutes": str(duration_minutes),
        "questions_count": str(questions_count),
        "csv_file": csv_file,
        "results_db": results_db,
        "worksheet_name": worksheet_name,
        "teams_group": teams_group,
        "test_link_name": test_link_name,
        "lesson_id": lesson_id,
    }

    for key_name, value in config.items():
        db_set_setting(key_name, str(value))

    with _QUESTION_CACHE_LOCK:
        _QUESTION_CACHE.clear()

    db_insert_test_config({
        "academic_year": academic_year,
        "semester": semester,
        "discipline_name": discipline_name,
        "lecture_number": lecture_number,
        "test_date": test_date,
        "weekday": weekday,
        "start_time": start_time,
        "end_time": end_time,
        "duration_minutes": duration_minutes,
        "questions_count": questions_count,
        "csv_file": csv_file,
        "results_db": results_db,
        "worksheet_name": worksheet_name,
        "teams_group": teams_group,
        "test_link_name": test_link_name,
        "lesson_id": lesson_id,
    })

    return templates.TemplateResponse("admin_config_saved.html", {
        "request": request,
        "config": config,
        "key": key,
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

    lesson = now_kyiv().strftime("%Y-%m-%d")
    db_set_setting("lesson_id", lesson)

    return {"ok": True, "lesson_id": lesson}


@app.get("/admin/get_lesson")
def admin_get_lesson(key: str = Query(...)):
    _admin_check(key)

    config = get_current_config()

    return {
        "kyiv_now": now_kyiv().isoformat(timespec="seconds"),
        "lesson_id_db": db_get_setting("lesson_id"),
        "lesson_id_env": LESSON_ID_ENV,
        "lesson_id_effective": _get_lesson_id(),
        "worksheet_name": config["worksheet_name"],
        "gsheet_id_set": bool(GSHEET_ID),
        "sheets_enabled": _sheets_enabled(),
        "config": config,
    }


@app.get("/admin/clear_lesson")
def admin_clear_lesson(key: str = Query(...)):
    _admin_check(key)

    db_delete_setting("lesson_id")

    return {"ok": True, "lesson_id_db": None}


@app.get("/admin/health")
def admin_health(key: str = Query(...)):
    _admin_check(key)
    cleanup_expired_sessions()
    return {
        "ok": True,
        "active_sessions": len(SESSIONS),
        "question_cache_files": len(_QUESTION_CACHE),
        "config_cached": _CONFIG_CACHE.get("value") is not None,
        "kyiv_now": now_kyiv().isoformat(timespec="seconds"),
    }


@app.get("/admin/export")
def admin_export(key: str = Query(...)):
    _admin_check(key)

    path = export_results_to_xlsx("results.xlsx")

    return FileResponse(path, filename="results.xlsx")
