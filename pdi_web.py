#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
# อัปเดตส่วนนี้เพื่อปลดล็อกขีดจำกัดการอัปโหลดไฟล์/ฟิลด์จำนวนมาก
os.environ["STARLETTE_MAX_FORM_FIELDS"] = "10000"

import json
import sqlite3
import hashlib
import asyncio
import pathlib
import re
from datetime import datetime
from typing import List, Tuple, Optional, Dict

from fastapi import FastAPI, UploadFile, File, Form, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from telegram import Update
from telegram.ext import ContextTypes

CONFIG: dict = {}

def load_inv_hashes(path: str = None):
    try:
        if not path:
            path = os.path.join(os.path.dirname(__file__), "INVP.json")
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            return set(data.get("hashes", []))
    except Exception:
        return set()

def load_config() -> None:
    global CONFIG
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            CONFIG = json.load(f)
    except Exception:
        CONFIG = {}

def DB_PATH() -> str:
    return CONFIG.get("pdi_db", CONFIG.get("db_path", "stock.db"))

def UPLOAD_ROOT() -> str:
    return CONFIG.get("damage_upload_dir", "uploads_damage")

def ensure_pdi_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS pdi_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vin_no TEXT NOT NULL, id_van TEXT,
        status TEXT DEFAULT 'pending', percent_ok INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS pdi_steps (step_code TEXT PRIMARY KEY, step_name TEXT NOT NULL, seq INTEGER NOT NULL)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS pdi_results (
        job_id INTEGER NOT NULL, step_code TEXT NOT NULL, status TEXT NOT NULL,
        note TEXT, at TEXT, PRIMARY KEY(job_id, step_code)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS pdi_locks(
        vin_no TEXT PRIMARY KEY, job_id INTEGER, is_locked INTEGER NOT NULL DEFAULT 1,
        from_slot TEXT, to_slot TEXT, locked_at TEXT, unlocked_at TEXT
    )""")
    conn.commit()

def ensure_damage_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS damage_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vin_no TEXT NOT NULL UNIQUE,
        id_van TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        description TEXT,
        file_path1 TEXT,
        file_path2 TEXT,
        created_at TEXT NOT NULL,
        completed_at TEXT,
        is_locked INTEGER NOT NULL DEFAULT 1
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS damage_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        details TEXT,
        at TEXT NOT NULL
    )""")
    conn.commit()

def ensure_battery_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS battery_checks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vin_no TEXT NOT NULL,
        check_at TEXT NOT NULL,
        volt12_status TEXT,
        volt12_note TEXT,
        volt12_file1 TEXT,
        volt12_file2 TEXT,
        hivol_status TEXT,
        hivol_percent INTEGER,
        hivol_note TEXT,
        hivol_file1 TEXT,
        hivol_file2 TEXT
    )""")
    conn.commit()
    
def ensure_vdci_report_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS vdci_report_pairs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vin_no TEXT NOT NULL,
        
        before_file_path TEXT,
        before_report_time TEXT,
        before_dtc_summary TEXT,

        after_file_path TEXT,
        after_report_time TEXT,
        after_dtc_summary TEXT,

        created_at TEXT NOT NULL
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS vdci_report_images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pair_id INTEGER NOT NULL,
        file_path TEXT NOT NULL,
        uploaded_at TEXT NOT NULL,
        FOREIGN KEY (pair_id) REFERENCES vdci_report_pairs(id)
    )
    """)
    conn.commit()


def ensure_vehicle_columns(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ALTER TABLE vehicles ADD COLUMN slot TEXT")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE vehicles ADD COLUMN status TEXT")
    except Exception:
        pass
    conn.commit()

def ensure_inventory(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS inventory(
        vin_no TEXT PRIMARY KEY,
        id_van TEXT,
        in_stock INTEGER NOT NULL DEFAULT 1,
        updated_at TEXT
    )""")
    conn.commit()

def _try_lookup_both(conn: sqlite3.Connection, tbl: str, vv: str) -> Tuple[str, str]:
    try:
        r = conn.execute(f"SELECT vin_no, id_van FROM {tbl} WHERE vin_no=?", (vv,)).fetchone()
        if r:
            return (r[0], r[1] or "")
    except Exception:
        pass
    try:
        r = conn.execute(f"SELECT vin_no, id_van FROM {tbl} WHERE id_van=?", (vv,)).fetchone()
        if r:
            return (r[0], r[1] or "")
    except Exception:
        pass
    return ("", "")

def resolve_vin_idvan(token: str) -> Tuple[str, str]:
    vv = (token or "").strip()
    if not vv:
        return "", ""
    conn = sqlite3.connect(DB_PATH())
    try:
        ensure_vehicle_columns(conn)
        vin, idv = _try_lookup_both(conn, "vehicles", vv)
        if vin:
            return (vin, idv)
        for tbl in ("stocks", "stock", "car_stock"):
            vin, idv = _try_lookup_both(conn, tbl, vv)
            if vin:
                return (vin, idv)
        ensure_inventory(conn)
        vin, idv = _try_lookup_both(conn, "inventory", vv)
        if vin:
            return (vin, idv)
        return "", ""
    finally:
        conn.close()

def candidate_search(q: str, limit: int = 20) -> List[Tuple[str, str, str]]:
    q = (q or "").strip()
    if not q:
        return []
    like = f"%{q}%"
    seen = set()
    out: List[Tuple[str, str, str]] = []
    conn = sqlite3.connect(DB_PATH())
    try:
        conn.row_factory = sqlite3.Row
        ensure_vehicle_columns(conn)
        try:
            rows = conn.execute("""
                SELECT vin_no, COALESCE(id_van,'') AS id_van, COALESCE(model,'') AS model
                FROM vehicles
                WHERE vin_no LIKE ? OR id_van LIKE ?
                ORDER BY vin_no LIMIT ?
            """, (like, like, limit)).fetchall()
            for r in rows:
                if r["vin_no"] in seen:
                    continue
                seen.add(r["vin_no"])
                out.append((r["vin_no"], r["id_van"], r["model"]))
        except Exception:
            pass
        for tbl in ("stocks", "stock", "car_stock"):
            if len(out) >= limit:
                break
            try:
                rows = conn.execute(f"""
                    SELECT vin_no, COALESCE(id_van,'') AS id_van, COALESCE(model,'') AS model
                    FROM {tbl}
                    WHERE vin_no LIKE ? OR id_van LIKE ?
                    LIMIT ?
                """, (like, like, limit)).fetchall()
                for r in rows:
                    if r["vin_no"] in seen:
                        continue
                    seen.add(r["vin_no"])
                    out.append((r["vin_no"], r["id_van"], r["model"]))
                    if len(out) >= limit:
                        break
            except Exception:
                continue
        if len(out) < limit:
            ensure_inventory(conn)
            try:
                rows = conn.execute("""
                    SELECT vin_no, COALESCE(id_van,'') AS id_van, '' AS model
                    FROM inventory
                    WHERE vin_no LIKE ? OR id_van LIKE ?
                    LIMIT ?
                """, (like, like, limit)).fetchall()
                for r in rows:
                    if r["vin_no"] in seen:
                        continue
                    seen.add(r["vin_no"])
                    out.append((r["vin_no"], r["id_van"], r["model"]))
            except Exception:
                pass
    finally:
        conn.close()
    return out[:limit]

def log_damage_action(conn: sqlite3.Connection, report_id: int, action: str, details: str = ""):
    at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO damage_logs (report_id, action, details, at) VALUES (?, ?, ?, ?)",
        (report_id, action, details, at)
    )

def calculate_duration(start_str: str, end_str: Optional[str]) -> str:
    if not end_str:
        return "-"
    try:
        start_dt = datetime.fromisoformat(start_str)
        end_dt = datetime.fromisoformat(end_str)
        delta = end_dt - start_dt
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        parts = []
        if days > 0:
            parts.append(f"{days} วัน")
        if hours > 0:
            parts.append(f"{hours} ชั่วโมง")
        if minutes > 0:
            parts.append(f"{minutes} นาที")
        return " ".join(parts) if parts else "ทันที"
    except Exception:
        return "-"

def get_model(vin_no: str) -> Optional[str]:
    conn = sqlite3.connect(DB_PATH())
    try:
        conn.row_factory = sqlite3.Row
        for tbl in ("vehicles", "stocks", "stock", "car_stock"):
            try:
                row = conn.execute(f"SELECT model FROM {tbl} WHERE vin_no=?", (vin_no,)).fetchone()
                if row and row[0]: return str(row[0])
            except Exception: pass
        return None
    finally:
        conn.close()
        
def get_queue() -> List[Tuple]:
    conn = sqlite3.connect(DB_PATH())
    try:
        ensure_pdi_tables(conn)
        rows = conn.execute(
            """
            SELECT j.id, j.vin_no, j.id_van, j.status, j.percent_ok,
                   COALESCE(l.locked_at, '') AS locked_at
            FROM pdi_jobs j
            LEFT JOIN pdi_locks l ON l.vin_no = j.vin_no AND l.is_locked = 1
            WHERE j.status != 'complete'
            ORDER BY datetime(COALESCE(j.updated_at, j.created_at)) DESC
            """
        ).fetchall()
        return rows
    finally:
        conn.close()
        
def queue_hash() -> str:
    q = get_queue()
    material = "|".join(f"{jid}:{vin}:{idv}:{st}:{pct}:{lk}" for (jid, vin, idv, st, pct, lk) in q)
    return hashlib.sha1(material.encode("utf-8")).hexdigest()
    
def get_color(vin_no: str) -> Optional[str]:
    conn = sqlite3.connect(DB_PATH())
    try:
        conn.row_factory = sqlite3.Row
        for tbl in ("vehicles", "stocks", "stock", "car_stock"):
            for col in ("exterior_color", "color"):
                try:
                    row = conn.execute(f"SELECT {col} FROM {tbl} WHERE vin_no=?", (vin_no,)).fetchone()
                    if row and row[0]: return str(row[0])
                except Exception: pass
        return None
    finally:
        conn.close()

def base_layout(title: str, body: str, active_tab: str = "pdi") -> HTMLResponse:
    tabs = f"""
    <div style="margin-bottom:16px">
      <a class="tab {'active' if active_tab=='pdi' else ''}" href="/">PDI Queue</a>
      <a class="tab {'active' if active_tab=='damage' else ''}" href="/damage">งานแจ้งซ่อม</a>
      <a class="tab {'active' if active_tab=='battery' else ''}" href="/battery">งานแบตเตอรี่</a>
      <a class="tab {'active' if active_tab=='vdci' else ''}" href="/vdci">Add Report VDCI</a>
      <a class="tab {'active' if active_tab=='all_vdci' else ''}" href="/vdci/all">Batch Upload VDCI</a>
    </div>
    """
    return HTMLResponse(
        f"""<!doctype html>
<html lang="th"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{title}</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:24px;background:#fafafa}} h1,h2{{margin:0 0 12px}} .card{{border:1px solid #eee;border-radius:10px;padding:16px;margin:12px 0;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.03)}} .small{{color:#666;font-size:12px}} .badge{{display:inline-block;padding:2px 8px;border-radius:999px;background:#111;color:#fff;font-size:12px}} .btn{{display:inline-block;padding:6px 10px;border-radius:6px;border:1px solid #ddd;background:#f7f7f7;text-decoration:none;color:#111;cursor:pointer}} .btn.ok{{background:#e8f6ec;border-color:#bfe6cd}} .btn.ng{{background:#fdecea;border-color:#f5c2bf}} .btn.warn{{background:#fff2e6;border-color:#ffd6a6}} table{{width:100%;border-collapse:collapse;margin-top:8px}} th,td{{padding:8px;border-bottom:1px solid #eee;text-align:left;vertical-align:top}} thead th{{background:#fafafa;font-weight:600}} .tab{{display:inline-block;padding:8px 12px;border-radius:8px;border:1px solid #ddd;background:#fff;margin-right:8px;text-decoration:none;color:#111}} .tab.active{{background:#111;color:#fff;border-color:#111}} input[type=text],input[type=number],textarea{{width:95%;padding:8px;border-radius:6px;border:1px solid #ddd}} textarea{{min-height:80px}}
ul.dd{{list-style:none;margin:6px 0 0;padding:0;border:1px solid #eee;border-radius:8px;background:#fff;max-width:520px;box-shadow:0 2px 12px rgba(0,0,0,.06)}}
ul.dd li{{padding:8px 12px;border-bottom:1px solid #f1f1f1;cursor:pointer}}
ul.dd li:last-child{{border-bottom:none}}
ul.dd li:hover{{background:#f7f7f7}}
.compare-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
.dtc-fixed {{ color: red; text-decoration: line-through; }}
.dtc-new {{ color: green; }}
.drop-zone {{ border: 2px dashed #ccc; border-radius: 10px; padding: 20px; text-align: center; margin-bottom: 10px; background: #f9f9f9;}}
.drop-zone.dragover {{ border-color: #333; background: #eee; }}
.image-gallery {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 10px; margin-top: 10px; }}
.image-container {{ position: relative; }}
.image-container img {{ width: 100%; height: auto; border-radius: 6px; }}
.delete-btn {{ position: absolute; top: 5px; right: 5px; background: rgba(0,0,0,0.6); color: white; border: none; border-radius: 50%; width: 24px; height: 24px; cursor: pointer; }}
</style>
</head><body>
{tabs}
{body}
</body></html>
"""
    )
    
def render_queue_rows_html() -> str:
    q = get_queue()
    rows = []
    for jid, vin, idv, st, pct, lk in q:
        mdl = get_model(vin)
        col = get_color(vin)
        rows.append(
            "<tr>"
            f"<td><a class='btn' href='/job/{jid}'>เปิด</a></td>"
            f"<td>{vin}</td><td>{idv or '-'}</td>"
            f"<td>{mdl or '-'}</td><td>{col or '-'}</td>"
            f"<td><span class='badge'>{pct}%</span></td>"
            f"<td>{st}</td><td class='small'>{lk or ''}</td>"
            "</tr>"
        )
    if not rows:
        rows.append("<tr><td colspan='8' class='small'>ยังไม่มีคิวจาก /PDMO</td></tr>")
    return "".join(rows)

def mark_step(job_id: int, step_code: str, is_ok: bool, note: str = "") -> None:
    conn = sqlite3.connect(DB_PATH())
    try:
        ensure_pdi_tables(conn)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        status = "ok" if is_ok else "ng"
        conn.execute(
            """
            INSERT INTO pdi_results(job_id, step_code, status, note, at)
            VALUES (?,?,?,?,?)
            ON CONFLICT(job_id, step_code) DO UPDATE SET
                status=excluded.status, note=excluded.note, at=excluded.at
            """,
            (job_id, step_code, status, note, now),
        )

        total = conn.execute("SELECT COUNT(*) FROM pdi_steps").fetchone()[0] or 0
        ok_count = conn.execute("SELECT COUNT(*) FROM pdi_results WHERE job_id=? AND status='ok'", (job_id,)).fetchone()[0] or 0

        st = "in_progress"
        pct = int(round((ok_count / total) * 100)) if total else 0
        if total and ok_count == total:
            st, pct = "complete", 100
        
        conn.execute("UPDATE pdi_jobs SET status=?, percent_ok=?, updated_at=? WHERE id=?", (st, pct, now, job_id))
        conn.commit()
    finally:
        conn.close()

def _parse_html_report(content_str: str) -> dict:
    vin_match = re.search(r'<font size="4">VIN</font></td><td[^>]+><font size="4" color="blue">([^<]+)</font>', content_str)
    vin_no = vin_match.group(1).strip() if vin_match else None
    
    report_time_match = re.search(r'<font size="4">Report recording time</font></td><td><font size="4" color="blue">([^<]+)</font>', content_str)
    report_time = report_time_match.group(1).strip() if report_time_match else None
    
    parsed_time = None
    if report_time:
        try:
            parsed_time = datetime.strptime(report_time, "%Y.%m.%d  %H:%M:%S")
        except ValueError:
            try:
                parsed_time = datetime.strptime(report_time, "%Y.%m.%d %H:%M:%S")
            except ValueError:
                parsed_time = None

    dtc_summary = []
    dtc_table_match = re.search(r'<h2 align="center"[^>]+>ECUDTCInfo</h2>.*?<table.*?>(.*?)</table>', content_str, re.DOTALL)
    if dtc_table_match:
        table_content = dtc_table_match.group(1)
        dtc_rows = re.findall(r'<tr><td><font color="blue">\d+</font></td><td><font color="blue">([^<]+)</font></td><td><font color="blue">([^<]+)</font></td><td align = "left"><font color="blue">([^<]+)</font></td><td><font color="blue">([^<]+)</font>', table_content)
        for row in dtc_rows:
            dtc_summary.append({
                "ecu": row[0].strip(),
                "dtc": row[1].strip(),
                "description": row[2].strip(),
                "state": row[3].strip()
            })
            
    return {
        "vin_no": vin_no,
        "report_time_str": report_time,
        "report_datetime": parsed_time,
        "dtc_summary": dtc_summary
    }

def _save_and_log_vdci_pair(
    conn: sqlite3.Connection,
    vin_no: str, 
    before_info: dict, 
    before_bytes: bytes,
    after_info: dict,
    after_bytes: bytes
) -> str:
    
    vin_dir = pathlib.Path(UPLOAD_ROOT()) / vin_no
    vin_dir.mkdir(parents=True, exist_ok=True)
    
    ts_before = before_info['report_datetime'].strftime('%Y%m%d_%H%M%S')
    before_fname = f"vdci_report_{ts_before}_before.html"
    before_fpath = vin_dir / before_fname
    with open(before_fpath, "wb") as buffer:
        buffer.write(before_bytes)

    ts_after = after_info['report_datetime'].strftime('%Y%m%d_%H%M%S')
    after_fname = f"vdci_report_{ts_after}_after.html"
    after_fpath = vin_dir / after_fname
    with open(after_fpath, "wb") as buffer:
        buffer.write(after_bytes)

    conn.execute(
        """
        INSERT INTO vdci_report_pairs (
            vin_no, 
            before_file_path, before_report_time, before_dtc_summary,
            after_file_path, after_report_time, after_dtc_summary,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            vin_no,
            f"{vin_no}/{before_fname}", before_info['report_time_str'], json.dumps(before_info['dtc_summary']),
            f"{vin_no}/{after_fname}", after_info['report_time_str'], json.dumps(after_info['dtc_summary']),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
    )
    return f"{vin_no}/{before_fname}", f"{vin_no}/{after_fname}"

def _get_idvan_for_vin(conn: sqlite3.Connection, vin_no: str) -> str:
    try:
        r = conn.execute("SELECT id_van FROM vehicles WHERE vin_no = ?", (vin_no,)).fetchone()
        return r[0] if (r and r[0]) else ""
    except Exception:
        return ""

def _mark_pdi_complete_for_vin(conn: sqlite3.Connection, vin_no: str) -> None:
    if not vin_no:
        return
    
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        job_id_row = conn.execute(
            "SELECT id FROM pdi_jobs WHERE vin_no = ? AND status != 'complete' ORDER BY id DESC LIMIT 1",
            (vin_no,)
        ).fetchone()
        
        if job_id_row:
            job_id = job_id_row[0]
            conn.execute(
                "UPDATE pdi_jobs SET status='complete', percent_ok=100, updated_at=? WHERE id=?",
                (now, job_id)
            )
        else:
            id_van = _get_idvan_for_vin(conn, vin_no)
            
            conn.execute(
                """
                INSERT INTO pdi_jobs (vin_no, id_van, status, percent_ok, created_at, updated_at)
                VALUES (?, ?, 'complete', 100, ?, ?)
                """,
                (vin_no, id_van, now, now)
            )
            
            conn.execute(
                """
                INSERT INTO pdi_locks (vin_no, is_locked, unlocked_at) 
                VALUES (?, 0, ?)
                ON CONFLICT(vin_no) DO UPDATE SET
                    is_locked = 0,
                    unlocked_at = excluded.unlocked_at
                """,
                (vin_no, now)
            )

    except Exception as e:
        print(f"Error in _mark_pdi_complete_for_vin: {e}") 
        raise e

def create_app() -> FastAPI:
    load_config()
    app = FastAPI(title="PDI & Damage Report Web")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    db_conn = sqlite3.connect(DB_PATH())
    try:
        ensure_pdi_tables(db_conn)
        ensure_damage_tables(db_conn)
        ensure_battery_tables(db_conn)
        ensure_vdci_report_tables(db_conn)
    finally:
        db_conn.close()

    pathlib.Path(UPLOAD_ROOT()).mkdir(parents=True, exist_ok=True)
    app.mount("/uploads_damage", StaticFiles(directory=UPLOAD_ROOT()), name="uploads_damage")

    secret_key = os.environ.get("PDI_SESSION_SECRET", "pdi_default_dev_secret_change_me")
    try:
        app.add_middleware(SessionMiddleware, secret_key=secret_key, same_site="lax", https_only=False)
    except Exception:
        pass

    ALLOWED_HASHES = load_inv_hashes()
    PUBLIC_PATHS = {"/login", "/logout", "/openapi.json", "/docs", "/redoc", "/healthz"}

    LOGIN_HTML = """<!doctype html>
    <html>
      <head><meta charset="utf-8"><title>PDI Login</title></head>
      <body style="font-family: Arial, Helvetica, sans-serif; padding:40px; max-width:600px;">
        <h2>PDI Web - Login</h2>
        <form method="post" action="/login">
          <label>Password: <input type="password" name="password" autofocus></label>
          <button type="submit">Login</button>
        </form>
      </body>
    </html>"""

    @app.get("/healthz")
    async def healthz():
        return JSONResponse({"ok": True, "app": "pdi_web"})

    @app.get("/login")
    async def login_get():
        return HTMLResponse(LOGIN_HTML.replace("", ""))

    @app.post("/login")
    async def login_post(password: str = Form(...)):
        h = hashlib.sha256(password.encode("utf-8")).hexdigest()
        if h in ALLOWED_HASHES:
            resp = RedirectResponse(url="/", status_code=302)
            resp.set_cookie("pdi_auth_session", h, httponly=True, samesite="lax")
            return resp
        return HTMLResponse(
            LOGIN_HTML.replace("", "<p style='color:red'>รหัสผ่านไม่ถูกต้อง</p>"),
            status_code=401
        )

    @app.get("/logout")
    async def logout():
        res = RedirectResponse(url="/login")
        try:
            res.delete_cookie("pdi_auth_session")
            res.delete_cookie("session")
        except Exception:
            pass
        return res

    @app.middleware("http")
    async def _pdi_auth_guard(request: Request, call_next):
        path = request.url.path or "/"
        if path in PUBLIC_PATHS or any(path.startswith(p + "/") for p in PUBLIC_PATHS):
            return await call_next(request)
        try:
            if request.session.get("pdi_auth_ok"):
                return await call_next(request)
        except Exception:
            pass
        try:
            c = request.cookies.get("pdi_auth_session")
            if c and c in ALLOWED_HASHES:
                try:
                    request.session["pdi_auth_ok"] = True
                except Exception:
                    pass
                return await call_next(request)
        except Exception:
            pass
        return RedirectResponse(url="/login")

    LOGOUT_HTML_SNIPPET = """
    <a href="/logout" title="Logout" style="
        position:fixed; top:10px; right:12px; z-index:2147483647;
        display:inline-block; padding:8px 12px; text-decoration:none;
        border:1px solid #c33; border-radius:6px; background:#fff; color:#c33;
        font-family:Arial, Helvetica, sans-serif; font-size:14px;
        box-shadow:0 1px 3px rgba(0,0,0,.15);
    ">Logout</a>
    """

    @app.middleware("http")
    async def _inject_logout_button(request: Request, call_next):
        resp = await call_next(request)
        try:
            authed = False
            try:
                authed = bool(request.session.get("pdi_auth_ok"))
            except Exception:
                authed = False
            try:
                ck = request.cookies.get("pdi_auth_session")
                if ck and ck in ALLOWED_HASHES:
                    authed = True
            except Exception:
                pass
            if not authed:
                return resp
            ct = resp.headers.get("content-type", "")
            if "text/html" not in ct.lower():
                return resp
            body_bytes = b""
            if hasattr(resp, "body_iterator") and resp.body_iterator is not None:
                async for chunk in resp.body_iterator:
                    body_bytes += chunk
            else:
                try:
                    body_bytes = resp.body
                except Exception:
                    body_bytes = b""
            if not body_bytes:
                return resp
            try:
                html = body_bytes.decode("utf-8", errors="ignore")
            except Exception:
                return resp
            if "</body>" in html.lower():
                lower = html.lower()
                idx = lower.rfind("</body>")
                if idx >= 0:
                    html = html[:idx] + LOGOUT_HTML_SNIPPET + html[idx:]
            from fastapi.responses import HTMLResponse
            headers = dict(resp.headers)
            headers.pop("content-length", None)
            new_resp = HTMLResponse(content=html, status_code=resp.status_code, headers=headers)
            for header, value in resp.raw_headers:
                if header.decode("latin1").lower() == "set-cookie":
                    new_resp.raw_headers.append((header, value))
            return new_resp
        except Exception:
            return resp

    @app.get("/api/search")
    def api_search_endpoint(q: str):
        items = [{"vin_no": a, "id_van": b, "model": c}
                 for (a, b, c) in candidate_search(q, limit=20)]
        return JSONResponse(items)

    @app.get("/", response_class=HTMLResponse)
    def home():
        body = f"""
        <div class="card">
          <h1>PDI Queue</h1>
          <table>
            <thead><tr>
              <th></th><th>VIN</th><th>ID VAN</th><th>MODEL</th><th>EXTERIOR</th><th>%</th><th>สถานะ</th><th>เริ่ม (lock)</th>
            </tr></thead>
            <tbody id="queue-body">{render_queue_rows_html()}</tbody>
          </table>
        </div>
        <script>
        (function() {{
            var es = new EventSource('/events');
            var tbody = document.getElementById('queue-body');
            es.addEventListener('update', function(ev) {{ tbody.innerHTML = ev.data; }});
        }})();
        </script>
        """
        return base_layout("PDI Queue", body, active_tab="pdi")

    @app.get("/job/{job_id}", response_class=HTMLResponse)
    def job_pdi(job_id: int):
        conn = sqlite3.connect(DB_PATH())
        try:
            r = conn.execute("SELECT id, vin_no, id_van, status, percent_ok FROM pdi_jobs WHERE id=?", (job_id,)).fetchone()
            if not r: return base_layout("ไม่พบงาน", "<div class='card'>ไม่พบงาน</div>")
            jid, vin, idv, st, pct = r

            steps = conn.execute("SELECT step_code, step_name, seq FROM pdi_steps ORDER BY seq ASC").fetchall()
            results = { row[0]: (row[1], row[2]) for row in conn.execute("SELECT step_code, status, at FROM pdi_results WHERE job_id=?", (job_id,)).fetchall() }
        finally:
            conn.close()

        grid = []
        for code, name, seq in steps:
            res, at = results.get(code, ("-", ""))
            btns = f"<button data-step='{code}' data-val='1' class='btn ok js-pdi-mark'>OK</button> <button data-step='{code}' data-val='0' class='btn ng js-pdi-mark'>NG</button>"
            grid.append(f"<tr><td>{seq:02d}</td><td>{code}</td><td>{name}</td><td class='js-pdi-status-{code}'>{res.upper()}</td><td class='small'>{at or ''}</td><td>{btns}</td></tr>")

        body = f"""
        <div class="card">
          <h2>PDI: {vin} | {idv or '-'}</h2>
          <p>สถานะ: <b>{st}</b> | <span class='badge'>{pct}%</span></p>
          <table>
            <thead><tr><th>#</th><th>STEP</th><th>รายการ</th><th>ผล</th><th>เวลา</th><th></th></tr></thead>
            <tbody>{''.join(grid)}</tbody>
          </table>
          <p><a class="btn" href="/">← กลับ</a></p>
        </div>
        <script>
        (function() {{
          document.querySelectorAll('.js-pdi-mark').forEach(btn=>{{
            btn.addEventListener('click', function() {{
              var step = this.getAttribute('data-step');
              var ok = this.getAttribute('data-val') === '1';
              fetch('/api/pdi/{jid}/mark', {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify({{step:step, ok:ok}})}})
              .then(r=>r.json()).then(res=>{{ if(res.ok) window.location.reload(); }});
            }});
          }});
        }})();
        </script>
        """
        return base_layout(f"PDI {vin}", body, active_tab="pdi")

    @app.post("/api/pdi/{job_id}/mark")
    async def api_pdi_mark(job_id:int, req: Request):
        data = await req.json()
        mark_step(job_id, data.get("step"), bool(data.get("ok")))
        return JSONResponse({"ok": True})

    @app.get("/events")
    async def sse_events():
        async def event_generator():
            last_hash = None
            while True:
                await asyncio.sleep(2)
                new_hash = queue_hash()
                if new_hash != last_hash:
                    yield f"event: update\ndata: {render_queue_rows_html()}\n\n"
                    last_hash = new_hash
        return StreamingResponse(event_generator(), media_type="text/event-stream")

    @app.get("/damage", response_class=HTMLResponse)
    def damage_home():
        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            pending = conn.execute("SELECT * FROM damage_reports WHERE status = 'pending' ORDER BY created_at DESC").fetchall()
            completed = conn.execute("SELECT * FROM damage_reports WHERE status = 'completed' ORDER BY completed_at DESC LIMIT 50").fetchall()
        finally:
            conn.close()

        def render_rows(reports: list) -> str:
            rows = []
            for r in reports:
                duration = calculate_duration(r['created_at'], r['completed_at'])
                rows.append(f"""
                <tr>
                    <td><a href="/damage/job/{r['id']}" class="btn">เปิด</a></td>
                    <td>{r['vin_no']}</td>
                    <td>{get_model(r['vin_no']) or '-'}</td>
                    <td><span class="badge">{r['status']}</span></td>
                    <td class="small">{datetime.fromisoformat(r['created_at']).strftime('%d-%m-%Y %H:%M')}</td>
                    <td class="small">{datetime.fromisoformat(r['completed_at']).strftime('%d-%m-%Y %H:%M') if r['completed_at'] else '-'}</td>
                    <td class="small">{duration}</td>
                </tr>
                """)
            return "".join(rows) if rows else "<tr><td colspan='7' class='small'>ไม่มีรายการ</td></tr>"

        body = f"""
        <div class="card">
            <h2>สร้างงานแจ้งซ่อม/ความเสียหาย</h2>
            <form method="post" action="/damage/create">
                <input name="token" placeholder="ค้นหา VIN หรือ ID VAN เพื่อสร้างงาน..." required style="width:300px"/>
                <button type="submit" class="btn">สร้าง</button>
            </form>
        </div>
        <div class="card">
            <h2>รายการที่ยังไม่เสร็จ</h2>
            <table>
                <thead><tr><th></th><th>VIN</th><th>Model</th><th>สถานะ</th><th>เวลาสร้าง</th><th>เวลาเสร็จ</th><th>ระยะเวลา</th></tr></thead>
                <tbody>{render_rows(pending)}</tbody>
            </table>
        </div>
        <div class="card">
            <h2>รายการที่เสร็จแล้ว (ล่าสุด 50 รายการ)</h2>
            <table>
                <thead><tr><th></th><th>VIN</th><th>Model</th><th>สถานะ</th><th>เวลาสร้าง</th><th>เวลาเสร็จ</th><th>ระยะเวลา</th></tr></thead>
                <tbody>{render_rows(completed)}</tbody>
            </table>
        </div>
        """
        return base_layout("งานแจ้งซ่อม", body, active_tab="damage")
    
    @app.get("/battery", response_class=HTMLResponse)
    def battery_home():
        body = f"""
        <div class="card">
            <h2>🔋 จัดการแบตเตอรี่</h2>
            <input id="q_battery" type="text" placeholder="ค้นหา VIN / ID VAN เพื่อจัดการแบตเตอรี่..." style="width:300px"/>
            <ul id="dd_battery" class="dd" style="display:none"></ul>
        </div>
        <script>
        (function(){{
          var q = document.getElementById('q_battery');
          var dd = document.getElementById('dd_battery');
          var timer = null;
          q.addEventListener('input', function(){{
            var v = q.value.trim();
            if(timer) clearTimeout(timer);
            if(v.length < 2){{ dd.style.display='none'; dd.innerHTML=''; return; }}
            timer = setTimeout(function(){{
              fetch('/api/search?q='+encodeURIComponent(v))
                .then(r=>r.json()).then(items=>{{
                  if(!items || !items.length){{ dd.style.display='none'; dd.innerHTML=''; return; }}
                  dd.innerHTML = items.map(function(it){{
                    var vin = it.vin_no || ''; var idv = it.id_van || ''; var mdl = it.model || '';
                    return '<li data-vin="'+vin+'"><b>'+vin+'</b> &nbsp; <span class="small">'+(idv||'-')+'</span> &nbsp; <span class="badge">'+(mdl||'-')+'</span></li>';
                  }}).join('');
                  dd.style.display='block';
                  dd.querySelectorAll('li').forEach(function(li){{
                    li.onclick = function(){{ location.href='/battery/manage?token='+encodeURIComponent(this.getAttribute('data-vin')); }};
                  }});
                }});
            }}, 200);
          }});
        }})();
        </script>
        """
        return base_layout("งานแบตเตอรี่", body, active_tab="battery")

    @app.get("/vdci", response_class=HTMLResponse)
    def vdci_home():
        body = f"""
        <div class="card">
            <h2>VDCI Report</h2>
            <p>ค้นหารถยนต์เพื่อเพิ่มหรือดูไฟล์ VDCI Report</p>
            <input id="q_vdci" type="text" placeholder="ค้นหา VIN / ID VAN..." style="width:300px"/>
            <ul id="dd_vdci" class="dd" style="display:none"></ul>
        </div>
        <script>
        (function(){{
          var q = document.getElementById('q_vdci');
          var dd = document.getElementById('dd_vdci');
          var timer = null;
          q.addEventListener('input', function(){{
            var v = q.value.trim();
            if(timer) clearTimeout(timer);
            if(v.length < 2){{ dd.style.display='none'; dd.innerHTML=''; return; }}
            timer = setTimeout(function(){{
              fetch('/api/search?q='+encodeURIComponent(v))
                .then(r=>r.json()).then(items=>{{
                  if(!items || !items.length){{ dd.style.display='none'; dd.innerHTML=''; return; }}
                  dd.innerHTML = items.map(function(it){{
                    var vin = it.vin_no || ''; var idv = it.id_van || ''; var mdl = it.model || '';
                    return '<li data-vin="'+vin+'"><b>'+vin+'</b> &nbsp; <span class="small">'+(idv||'-')+'</span> &nbsp; <span class="badge">'+(mdl||'-')+'</span></li>';
                  }}).join('');
                  dd.style.display='block';
                  dd.querySelectorAll('li').forEach(function(li){{
                    li.onclick = function(){{ location.href='/vdci/manage?token='+encodeURIComponent(this.getAttribute('data-vin')); }};
                  }});
                }});
            }}, 200);
          }});
        }})();
        </script>
        """
        return base_layout("VDCI Report", body, active_tab="vdci")

    @app.get("/vdci/manage", response_class=HTMLResponse)
    def vdci_manage_report(token: str = Query(...)):
        vin, idv = resolve_vin_idvan(token)
        if not vin:
            return base_layout("ไม่พบรถ", "<div class='card'>ไม่พบรถในระบบ</div>", active_tab="vdci")

        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            report_pairs = conn.execute("SELECT * FROM vdci_report_pairs WHERE vin_no = ? ORDER BY id DESC", (vin,)).fetchall()
        finally:
            conn.close()

        report_rows_html = []
        if report_pairs:
            for pair in report_pairs:
                report_rows_html.append(f"""
                <tr>
                    <td class="small">{pair['created_at']}</td>
                    <td><a href="/uploads_damage/{pair['before_file_path']}" target="_blank" class="btn">ดูไฟล์ก่อนแก้</a></td>
                    <td><a href="/uploads_damage/{pair['after_file_path']}" target="_blank" class="btn">ดูไฟล์หลังแก้</a></td>
                    <td><a href="/vdci/images/{pair['id']}" class="btn">🖼️ จัดการรูปภาพ</a></td>
                    <td><a href="/vdci/compare/{pair['id']}" class="btn warn">เปรียบเทียบ</a></td>
                </tr>
                """)
        else:
            report_rows_html.append("<tr><td colspan='5' class='small'>ยังไม่มีการอัปโหลดไฟล์รีพอร์ตสำหรับรถคันนี้</td></tr>")

        body = f"""
        <div class="card">
            <h2>จัดการ VDCI Report: {vin}</h2>
            <p>ID VAN: {idv or '-'}</p>
            <p>Model: {get_model(vin) or '-'}</p>
        </div>
        <div class="card">
            <h3>อัปโหลดไฟล์ VDCI Report (ก่อนและหลัง)</h3>
            <form action="/api/vdci/upload" method="post" enctype="multipart/form-data">
                <input type="hidden" name="vin_no" value="{vin}">
                <p>ไฟล์ที่ 1: <input type="file" name="report_file1" accept=".html" required></p>
                <p>ไฟล์ที่ 2: <input type="file" name="report_file2" accept=".html" required></p>
                <p class="small">ระบบจะตรวจสอบเวลาจากในไฟล์เพื่อระบุไฟล์ "ก่อน" และ "หลัง" โดยอัตโนมัติ</p>
                <button type="submit" class="btn ok">อัปโหลด</button>
            </form>
        </div>
        <div class="card">
            <h3>ประวัติการรีพอร์ต</h3>
            <table>
                <thead>
                    <tr>
                        <th>เวลาอัปโหลด</th>
                        <th>ไฟล์ก่อนแก้</th>
                        <th>ไฟล์หลังแก้</th>
                        <th>รูปภาพ</th>
                        <th>เปรียบเทียบ</th>
                    </tr>
                </thead>
                <tbody>{''.join(report_rows_html)}</tbody>
            </table>
        </div>
        <p><a href="/vdci" class="btn">กลับไปหน้าค้นหา</a></p>
        """
        return base_layout(f"VDCI Report: {vin}", body, active_tab="vdci")
        
    @app.get("/vdci/compare/{pair_id}", response_class=HTMLResponse)
    def vdci_compare_page(pair_id: int):
        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            pair = conn.execute("SELECT * FROM vdci_report_pairs WHERE id = ?", (pair_id,)).fetchone()
            if not pair:
                raise HTTPException(status_code=404, detail="Report pair not found")
        finally:
            conn.close()
            
        vin = pair['vin_no']
        before_dtcs_json = json.loads(pair['before_dtc_summary'] or '[]')
        after_dtcs_json = json.loads(pair['after_dtc_summary'] or '[]')

        before_codes = {f"{d['ecu']}:{d['dtc']}" for d in before_dtcs_json}
        after_codes = {f"{d['ecu']}:{d['dtc']}" for d in after_dtcs_json}

        fixed_dtcs = before_codes - after_codes
        new_dtcs = after_codes - before_codes
        remaining_dtcs = before_codes.intersection(after_codes)

        def render_dtc_list(dtc_list, dtc_set, css_class=""):
            html = "<ul>"
            found = False
            for d in dtc_list:
                code = f"{d['ecu']}:{d['dtc']}"
                if code in dtc_set:
                    html += f"<li class='{css_class}'><b>{d['dtc']}</b> ({d['ecu']})<br><span class='small'>{d['description']} [{d['state']}]</span></li>"
                    found = True
            if not found:
                html += "<li>-</li>"
            html += "</ul>"
            return html

        before_html = f"<h4>ก่อนแก้ไข ({pair['before_report_time']}) <a href='/uploads_damage/{pair['before_file_path']}' target='_blank' class='btn small'>ดูไฟล์เต็ม</a></h4>"
        before_html += "<h5>Fixed DTCs:</h5>"
        before_html += render_dtc_list(before_dtcs_json, fixed_dtcs, "dtc-fixed")
        before_html += "<h5>Remaining DTCs:</h5>"
        before_html += render_dtc_list(before_dtcs_json, remaining_dtcs)

        after_html = f"<h4>หลังแก้ไข ({pair['after_report_time']}) <a href='/uploads_damage/{pair['after_file_path']}' target='_blank' class='btn small'>ดูไฟล์เต็ม</a></h4>"
        after_html += "<h5>New DTCs:</h5>"
        after_html += render_dtc_list(after_dtcs_json, new_dtcs, "dtc-new")
        after_html += "<h5>Remaining DTCs:</h5>"
        after_html += render_dtc_list(after_dtcs_json, remaining_dtcs)

        body = f"""
        <div class="card">
            <h2>เปรียบเทียบผล VDCI Report: {vin}</h2>
            <p><a class="btn" href="/vdci/images/{pair_id}">🖼️ ดู/จัดการรูปภาพประกอบ</a></p>
            <div class="compare-grid">
                <div>{before_html}</div>
                <div>{after_html}</div>
            </div>
             <p><a href="/vdci/manage?token={vin}" class="btn">กลับไปหน้ารายการ</a></p>
        </div>
        """
        return base_layout(f"Compare VDCI: {vin}", body, active_tab="vdci")

    @app.get("/vdci/images/{pair_id}", response_class=HTMLResponse)
    def vdci_manage_images(pair_id: int):
        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            pair = conn.execute("SELECT * FROM vdci_report_pairs WHERE id = ?", (pair_id,)).fetchone()
            if not pair:
                raise HTTPException(status_code=404, detail="Report pair not found")
            images = conn.execute("SELECT * FROM vdci_report_images WHERE pair_id = ? ORDER BY id", (pair_id,)).fetchall()
        finally:
            conn.close()
        
        vin = pair['vin_no']

        gallery_html = ""
        if images:
            for img in images:
                image_url = f"/uploads_damage/{img['file_path']}"
                gallery_html += f"""
                <div class="image-container">
                    <a href="{image_url}" target="_blank">
                        <img src="{image_url}" alt="VDCI Image">
                    </a>
                    <form action="/api/vdci/delete_image" method="post" style="display:inline;">
                        <input type="hidden" name="image_id" value="{img['id']}">
                        <input type="hidden" name="pair_id" value="{pair_id}">
                        <button type="submit" class="delete-btn" title="Delete Image">×</button>
                    </form>
                </div>
                """
        else:
            gallery_html = "<p class='small'>ยังไม่มีรูปภาพ</p>"

        body = f"""
        <div class="card">
            <h2>จัดการรูปภาพสำหรับ VDCI Report: {vin}</h2>
            <p class="small">สำหรับชุดรีพอร์ต: {pair['created_at']}</p>
        </div>
        <div class="card">
            <h3>อัปโหลดรูปภาพ (สูงสุด 20 รูป)</h3>
            <form id="upload-form" action="/api/vdci/upload_images/{pair_id}" method="post" enctype="multipart/form-data">
                <div id="drop-zone" class="drop-zone">
                    ลากไฟล์มาวางที่นี่ หรือคลิกเพื่อเลือกไฟล์
                    <input type="file" id="file-input" name="files" multiple accept="image/*" style="display: none;">
                </div>
                <div id="file-list"></div>
                <button type="submit" class="btn ok">อัปโหลด</button>
            </form>
        </div>
        <div class="card">
            <h3>แกลเลอรีรูปภาพ</h3>
            <div class="image-gallery">{gallery_html}</div>
        </div>
        <p><a href="/vdci/manage?token={vin}" class="btn">กลับไปหน้ารายการรีพอร์ต</a></p>
        
        <script>
            const dropZone = document.getElementById('drop-zone');
            const fileInput = document.getElementById('file-input');
            const fileList = document.getElementById('file-list');
            const uploadForm = document.getElementById('upload-form');

            dropZone.addEventListener('click', () => fileInput.click());
            dropZone.addEventListener('dragover', (e) => {{
                e.preventDefault();
                dropZone.classList.add('dragover');
            }});
            dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
            dropZone.addEventListener('drop', (e) => {{
                e.preventDefault();
                dropZone.classList.remove('dragover');
                const files = e.dataTransfer.files;
                if (files.length > 0) {{
                    fileInput.files = files;
                    updateFileList();
                }}
            }});
            fileInput.addEventListener('change', updateFileList);

            function updateFileList() {{
                fileList.innerHTML = '';
                if (fileInput.files.length > 20) {{
                    alert('สามารถอัปโหลดได้สูงสุด 20 รูปภาพ');
                    fileInput.value = ''; 
                    return;
                }}
                for (const file of fileInput.files) {{
                    const listItem = document.createElement('p');
                    listItem.textContent = file.name;
                    fileList.appendChild(listItem);
                }}
            }}
        </script>
        """
        return base_layout(f"Manage Images: {vin}", body, active_tab="vdci")
        
    @app.post("/api/vdci/upload")
    async def vdci_upload_file(
        vin_no: str = Form(...), 
        report_file1: UploadFile = File(...),
        report_file2: UploadFile = File(...)
    ):
        if not report_file1.filename.lower().endswith('.html') or not report_file2.filename.lower().endswith('.html'):
            raise HTTPException(status_code=400, detail="Invalid file type. Only .html files are allowed.")
        
        content1_bytes = await report_file1.read()
        content2_bytes = await report_file2.read()
        
        info1 = _parse_html_report(content1_bytes.decode('utf-8', errors='ignore'))
        info2 = _parse_html_report(content2_bytes.decode('utf-8', errors='ignore'))

        if not info1['report_datetime'] or not info2['report_datetime']:
            raise HTTPException(status_code=400, detail="Could not determine report time from one or both files.")

        if info1['report_datetime'] < info2['report_datetime']:
            before_info, after_info = info1, info2
            before_bytes, after_bytes = content1_bytes, content2_bytes
        else:
            before_info, after_info = info2, info1
            before_bytes, after_bytes = content2_bytes, content1_bytes

        conn = sqlite3.connect(DB_PATH())
        try:
            _save_and_log_vdci_pair(conn, vin_no, before_info, before_bytes, after_info, after_bytes)
            _mark_pdi_complete_for_vin(conn, vin_no)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        finally:
            conn.close()

        return RedirectResponse(url=f"/vdci/manage?token={vin_no}", status_code=303)

    @app.get("/vdci/all", response_class=HTMLResponse)
    def vdci_batch_upload_page():
        body = f"""
        <div class="card">
            <h2>Batch Upload VDCI Reports</h2>
            <p><b>คลิกเพื่อเลือกโฟลเดอร์</b> ที่มีไฟล์ Report (.html) อยู่ข้างใน</p>
            <p class="small">ระบบจะค้นหาไฟล์ .html ทั้งหมดที่อยู่ในโฟลเดอร์ที่คุณเลือก (รวมถึงโฟลเดอร์ย่อย) และประมวลผลอัตโนมัติ</p>
            <form action="/vdci/all/upload" method="post" enctype="multipart/form-data">
                <div id="drop-zone" class="drop-zone">
                    คลิกเพื่อเลือกโฟลเดอร์ (หรือลากไฟล์ .html มาวาง)
                    <input type="file" id="file-input" name="files" multiple webkitdirectory directory style="display: none;">
                </div>
                <div id="file-list" style="margin-top:10px;"></div>
                <button type="submit" class="btn ok">อัปโหลดและประมวลผล</button>
            </form>
        </div>
        <script>
            const dropZone = document.getElementById('drop-zone');
            const fileInput = document.getElementById('file-input');
            const fileList = document.getElementById('file-list');
            
            function handleFiles(files) {{
                fileList.innerHTML = '';
                if (files.length > 0) {{
                    const list = document.createElement('ul');
                    let htmlCount = 0;
                    for (const file of files) {{
                        if (file.name.toLowerCase().endsWith('.html')) {{
                            const item = document.createElement('li');
                            item.textContent = file.name;
                            list.appendChild(item);
                            htmlCount++;
                        }}
                    }}
                    
                    if (htmlCount === 0) {{
                        fileList.innerHTML = '<p style="color:red;">ไม่พบไฟล์ .html ในไฟล์/โฟลเดอร์ที่เลือก</p>';
                    }} else {{
                        fileList.innerHTML = '<p><b>พบ ' + htmlCount + ' ไฟล์ .html (จากไฟล์/โฟลเดอร์ที่เลือก):</b></p>';
                        fileList.appendChild(list);
                    }}
                }}
                fileInput.files = files;
            }}

            dropZone.addEventListener('click', () => fileInput.click());
            dropZone.addEventListener('dragover', (e) => {{
                e.preventDefault();
                dropZone.classList.add('dragover');
            }});
            dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
            dropZone.addEventListener('drop', (e) => {{
                e.preventDefault();
                dropZone.classList.remove('dragover');
                handleFiles(e.dataTransfer.files); 
            }});
            fileInput.addEventListener('change', () => handleFiles(fileInput.files)); 
        </script>
        """
        return base_layout("Batch Upload VDCI", body, active_tab="all_vdci")

    @app.post("/vdci/all/upload", response_class=HTMLResponse)
    async def vdci_batch_upload_handler(files: List[UploadFile] = File(...)):
        vin_groups = {}
        errors = []
        processed_pairs = []

        for file in files:
            if not file.filename.lower().endswith('.html'):
                errors.append(f"ข้ามไฟล์: {file.filename} (ไม่ใช่ .html)")
                continue

            content_bytes = await file.read()
            try:
                content_str = content_bytes.decode('utf-8', errors='ignore')
                info = _parse_html_report(content_str)
            except Exception as e:
                errors.append(f"ข้ามไฟล์: {file.filename} (อ่านไฟล์ล้มเหลว: {e})")
                continue

            vin = info.get("vin_no")
            if not vin:
                errors.append(f"ข้ามไฟล์: {file.filename} (ไม่พบ VIN ในไฟล์)")
                continue
                
            if not info.get("report_datetime"):
                errors.append(f"ข้ามไฟล์: {file.filename} (ไม่พบเวลา 'Report recording time' ในไฟล์)")
                continue

            if vin not in vin_groups:
                vin_groups[vin] = []
            
            vin_groups[vin].append({
                "info": info,
                "bytes": content_bytes,
                "filename": file.filename
            })

        conn = sqlite3.connect(DB_PATH())
        try:
            for vin, file_list in vin_groups.items():
                
                if len(file_list) == 0:
                    continue 

                file_list.sort(key=lambda x: x['info']['report_datetime'])
                
                before_file = None
                after_file = None

                if len(file_list) == 1:
                    after_file = file_list[0]
                
                elif len(file_list) >= 2:
                    before_file = file_list[0]
                    after_file = file_list[-1]

                if not after_file:
                    errors.append(f"ข้าม VIN: {vin} (Logic error, no 'after' file found)")
                    continue

                try:
                    vin_dir = pathlib.Path(UPLOAD_ROOT()) / vin
                    vin_dir.mkdir(parents=True, exist_ok=True)
                    
                    before_fpath_str = None
                    before_time_str = None
                    before_dtc_str = '[]'
                    
                    if before_file:
                        ts_before = before_file['info']['report_datetime'].strftime('%Y%m%d_%H%M%S')
                        before_fname = f"vdci_report_{ts_before}_before.html"
                        before_fpath = vin_dir / before_fname
                        with open(before_fpath, "wb") as buffer:
                            buffer.write(before_file['bytes'])
                        
                        before_fpath_str = f"{vin}/{before_fname}"
                        before_time_str = before_file['info']['report_time_str']
                        before_dtc_str = json.dumps(before_file['info']['dtc_summary'])

                    ts_after = after_file['info']['report_datetime'].strftime('%Y%m%d_%H%M%S')
                    after_fname = f"vdci_report_{ts_after}_after.html"
                    after_fpath = vin_dir / after_fname
                    with open(after_fpath, "wb") as buffer:
                        buffer.write(after_file['bytes'])
                    
                    after_fpath_str = f"{vin}/{after_fname}"
                    after_time_str = after_file['info']['report_time_str']
                    after_dtc_str = json.dumps(after_file['info']['dtc_summary'])
                    
                    conn.execute(
                        """
                        INSERT INTO vdci_report_pairs (
                            vin_no, 
                            before_file_path, before_report_time, before_dtc_summary,
                            after_file_path, after_report_time, after_dtc_summary,
                            created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            vin,
                            before_fpath_str, before_time_str, before_dtc_str,
                            after_fpath_str, after_time_str, after_dtc_str,
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        )
                    )
                    
                    _mark_pdi_complete_for_vin(conn, vin)
                    conn.commit() 
                    
                    if before_file:
                        processed_pairs.append(f"<b>{vin}</b> (ใช้ไฟล์: {before_file['filename']} และ {after_file['filename']}) - <b style='color:green;'>PDI 100%</b>")
                    else:
                        processed_pairs.append(f"<b>{vin}</b> (ใช้ไฟล์เดียว: {after_file['filename']}) - <b style='color:green;'>PDI 100%</b>")

                except Exception as e:
                    conn.rollback() 
                    errors.append(f"ล้มเหลว VIN: {vin} (Database error: {e})")
        
        finally:
            conn.close()

        summary_html = "<h2>ผลการประมวลผล Batch Upload</h2>"
        if processed_pairs:
            summary_html += "<div class='card'><h3>✅ ประมวลผลสำเร็จ</h3><ul>"
            summary_html += "".join(f"<li>{p}</li>" for p in processed_pairs)
            summary_html += "</ul></div>"
        
        if errors:
            summary_html += "<div class='card'><h3>❌ รายการที่ข้าม/ผิดพลาด</h3><ul>"
            summary_html += "".join(f"<li>{e}</li>" for e in errors)
            summary_html += "</ul></div>"
            
        summary_html += '<p><a class="btn" href="/vdci/all">กลับไปอัปโหลดอีกครั้ง</a></p>'
        
        return base_layout("Batch Upload Result", summary_html, active_tab="all_vdci")


    @app.post("/api/vdci/upload_images/{pair_id}")
    async def vdci_upload_images(pair_id: int, files: List[UploadFile] = File(...)):
        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            pair = conn.execute("SELECT vin_no FROM vdci_report_pairs WHERE id = ?", (pair_id,)).fetchone()
            if not pair:
                raise HTTPException(status_code=404, detail="Report pair not found")
            
            vin_no = pair['vin_no']
            vin_dir = pathlib.Path(UPLOAD_ROOT()) / vin_no
            vin_dir.mkdir(exist_ok=True)

            for file in files:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                random_part = hashlib.sha1(os.urandom(16)).hexdigest()[:6]
                fname = f"vdci_img_{timestamp}_{random_part}_{file.filename}"
                dest = vin_dir / fname
                
                with open(dest, "wb") as buffer:
                    buffer.write(await file.read())
                
                conn.execute(
                    "INSERT INTO vdci_report_images (pair_id, file_path, uploaded_at) VALUES (?, ?, ?)",
                    (pair_id, f"{vin_no}/{fname}", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                )
            conn.commit()
        finally:
            conn.close()
        
        return RedirectResponse(url=f"/vdci/images/{pair_id}", status_code=303)

    @app.post("/api/vdci/delete_image")
    def vdci_delete_image(image_id: int = Form(...), pair_id: int = Form(...)):
        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            image = conn.execute("SELECT * FROM vdci_report_images WHERE id = ?", (image_id,)).fetchone()
            if image:
                file_to_delete = pathlib.Path(UPLOAD_ROOT()) / image['file_path']
                if file_to_delete.is_file():
                    file_to_delete.unlink()
                
                conn.execute("DELETE FROM vdci_report_images WHERE id = ?", (image_id,))
                conn.commit()
        finally:
            conn.close()
        
        return RedirectResponse(url=f"/vdci/images/{pair_id}", status_code=303)


    @app.post("/damage/create")
    def damage_create(token: str = Form(...)):
        vin, idv = resolve_vin_idvan(token)
        if not vin:
            return base_layout("Error", "<div class='card'>ไม่พบรถในระบบ</div>", active_tab="damage")
        
        conn = sqlite3.connect(DB_PATH())
        try:
            ensure_damage_tables(conn)
            existing = conn.execute("SELECT id FROM damage_reports WHERE vin_no = ? AND status = 'pending'", (vin,)).fetchone()
            if existing:
                return RedirectResponse(url=f"/damage/job/{existing[0]}", status_code=303)

            now = datetime.now().isoformat()
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO damage_reports (vin_no, id_van, created_at) VALUES (?, ?, ?)",
                (vin, idv, now)
            )
            report_id = cursor.lastrowid
            log_damage_action(conn, report_id, "created", f"สร้างงานสำหรับ VIN: {vin}")
            conn.commit()
        finally:
            conn.close()
        return RedirectResponse(url=f"/damage/job/{report_id}", status_code=303)

    @app.get("/damage/job/{report_id}", response_class=HTMLResponse)
    def damage_job_details(report_id: int):
        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            report = conn.execute("SELECT * FROM damage_reports WHERE id = ?", (report_id,)).fetchone()
            if not report:
                raise HTTPException(status_code=404, detail="Report not found")
        finally:
            conn.close()

        body = f"""
        <div class="card">
            <h2>รายละเอียดงานแจ้งซ่อม #{report['id']}</h2>
            <p><b>VIN:</b> {report['vin_no']} | <b>ID VAN:</b> {report['id_van'] or '-'}</p>
            <p><b>Model:</b> {get_model(report['vin_no']) or '-'}</p>
            <p><b>สถานะ:</b> <span class="badge">{report['status']}</span> {'🔒 Locked' if report['is_locked'] else '🔓 Unlocked'}</p>
        </div>
        <div class="card">
            <form action="/api/damage/{report_id}/update" method="post" enctype="multipart/form-data">
                <h3>รายละเอียดความเสียหาย</h3>
                <textarea name="description" placeholder="อธิบายความเสียหาย...">{report['description'] or ''}</textarea>
                
                <h3>ไฟล์แนบ (2 ไฟล์)</h3>
                <p>ไฟล์ 1: <input type="file" name="file1"> {f'<a href="/uploads_damage/{report["file_path1"]}" target="_blank">ดูไฟล์ปัจจุบัน</a>' if report["file_path1"] else ""}</p>
                <p>ไฟล์ 2: <input type="file" name="file2"> {f'<a href="/uploads_damage/{report["file_path2"]}" target="_blank">ดูไฟล์ปัจจุบัน</a>' if report["file_path2"] else ""}</p>

                <hr style="margin:20px 0;">
                <button type="submit" name="action" value="save" class="btn">Save (บันทึก)</button>
                <button type="submit" name="action" value="ng" class="btn ng">NG (บันทึก & ล็อกไว้)</button>
                <button type="submit" name="action" value="ok" class="btn ok">OK (แก้ไขเสร็จ & ปลดล็อก)</button>
                <a href="/damage" class="btn">กลับ</a>
            </form>
        </div>
        """
        return base_layout(f"Damage Report #{report_id}", body, active_tab="damage")

    @app.post("/api/damage/{report_id}/update")
    async def damage_update(report_id: int, 
                            description: str = Form(""),
                            action: str = Form("save"),
                            file1: UploadFile = File(None),
                            file2: UploadFile = File(None)):
        
        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            report = conn.execute("SELECT * FROM damage_reports WHERE id = ?", (report_id,)).fetchone()
            if not report:
                raise HTTPException(status_code=404, detail="Report not found")

            updates = {"description": description}
            log_details = []

            vin_dir = pathlib.Path(UPLOAD_ROOT()) / report['vin_no']
            vin_dir.mkdir(exist_ok=True)
            
            for i, f in enumerate([file1, file2]):
                if f and f.filename:
                    fname = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{f.filename}"
                    dest = vin_dir / fname
                    with open(dest, "wb") as buffer:
                        buffer.write(await f.read())
                    
                    key = f"file_path{i+1}"
                    updates[key] = f"{report['vin_no']}/{fname}"
                    log_details.append(f"อัปเดตไฟล์ {i+1}: {fname}")

            if description != report['description']:
                log_details.append("อัปเดตรายละเอียด")

            if action == 'ok':
                updates['status'] = 'completed'
                updates['is_locked'] = 0
                updates['completed_at'] = datetime.now().isoformat()
                log_details.append("เปลี่ยนสถานะเป็น Completed และปลดล็อก")
            elif action == 'ng':
                updates['is_locked'] = 1
                log_details.append("ยืนยันสถานะ NG และล็อกไว้")
            
            if updates:
                set_clauses = ", ".join([f"{k} = ?" for k in updates.keys()])
                params = list(updates.values()) + [report_id]
                conn.execute(f"UPDATE damage_reports SET {set_clauses} WHERE id = ?", tuple(params))

            if log_details:
                log_damage_action(conn, report_id, "updated", ", ".join(log_details))
            
            conn.commit()
        finally:
            conn.close()

        return RedirectResponse(url=f"/damage/job/{report_id}", status_code=303)

    @app.get("/battery/manage", response_class=HTMLResponse)
    def manage_battery_form(token: str = Query(...)):
        vin, idv = resolve_vin_idvan(token)
        if not vin:
            return base_layout("ไม่พบรถ", "<div class='card'>ไม่พบรถในระบบ</div>", active_tab="battery")

        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            last_check = conn.execute("SELECT * FROM battery_checks WHERE vin_no=? ORDER BY id DESC LIMIT 1", (vin,)).fetchone()
        finally:
            conn.close()

        body = f"""
        <div class="card">
            <h2>จัดการแบตเตอรี่: {vin}</h2>
            <p>ID VAN: {idv or '-'}</p>
        </div>
        <div class="card">
            <form action="/api/battery/save" method="post" enctype="multipart/form-data">
                <input type="hidden" name="vin_no" value="{vin}">
                <h3>แบตเตอรี่ 12V</h3>
                <p>
                    สถานะ: 
                    <input type="radio" name="volt12_status" value="OK" {'checked' if last_check and last_check['volt12_status'] == 'OK' else ''}> OK
                    <input type="radio" name="volt12_status" value="NG" {'checked' if last_check and last_check['volt12_status'] == 'NG' else ''}> NG
                </p>
                <p>Note: <textarea name="volt12_note">{last_check['volt12_note'] if last_check else ''}</textarea></p>
                <p>ไฟล์ 1: <input type="file" name="volt12_file1"> {f'<a href="/uploads_damage/{last_check["volt12_file1"]}" target="_blank">ดูไฟล์ปัจจุบัน</a>' if last_check and last_check["volt12_file1"] else ""}</p>
                <p>ไฟล์ 2: <input type="file" name="volt12_file2"> {f'<a href="/uploads_damage/{last_check["volt12_file2"]}" target="_blank">ดูไฟล์ปัจจุบัน</a>' if last_check and last_check["volt12_file2"] else ""}</p>
                
                <hr style="margin:20px 0;">

                <h3>แบตเตอรี่ High Voltage</h3>
                <p>
                    สถานะ:
                    <input type="radio" name="hivol_status" value="OK" {'checked' if last_check and last_check['hivol_status'] == 'OK' else ''}> OK
                    <input type="radio" name="hivol_status" value="NG" {'checked' if last_check and last_check['hivol_status'] == 'NG' else ''}> NG
                </p>
                <p>% แบตเตอรี่: <input type="number" name="hivol_percent" min="0" max="100" value="{last_check['hivol_percent'] if last_check and last_check['hivol_percent'] is not None else ''}"></p>
                <p>Note: <textarea name="hivol_note">{last_check['hivol_note'] if last_check else ''}</textarea></p>
                <p>ไฟล์ 1: <input type="file" name="hivol_file1"> {f'<a href="/uploads_damage/{last_check["hivol_file1"]}" target="_blank">ดูไฟล์ปัจจุบัน</a>' if last_check and last_check["hivol_file1"] else ""}</p>
                <p>ไฟล์ 2: <input type="file" name="hivol_file2"> {f'<a href="/uploads_damage/{last_check["hivol_file2"]}" target="_blank">ดูไฟล์ปัจจุบัน</a>' if last_check and last_check["hivol_file2"] else ""}</p>
                
                <hr style="margin:20px 0;">
                <button type="submit" class="btn ok">บันทึกข้อมูลแบตเตอรี่</button>
                <a href="/battery" class="btn">กลับ</a>
            </form>
        </div>
        """
        return base_layout(f"จัดการแบตเตอรี่ {vin}", body, active_tab="battery")

    @app.post("/api/battery/save")
    async def save_battery_data(
        vin_no: str = Form(...),
        volt12_status: Optional[str] = Form(None),
        volt12_note: Optional[str] = Form(""),
        volt12_file1: Optional[UploadFile] = File(None),
        volt12_file2: Optional[UploadFile] = File(None),
        hivol_status: Optional[str] = Form(None),
        hivol_percent: str = Form(""),
        hivol_note: Optional[str] = Form(""),
        hivol_file1: Optional[UploadFile] = File(None),
        hivol_file2: Optional[UploadFile] = File(None),
    ):
        conn = sqlite3.connect(DB_PATH())
        try:
            hivol_percent_val = None
            if hivol_percent and hivol_percent.strip().isdigit():
                hivol_percent_val = int(hivol_percent)

            data = {
                "vin_no": vin_no,
                "check_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "volt12_status": volt12_status,
                "volt12_note": volt12_note,
                "hivol_status": hivol_status,
                "hivol_percent": hivol_percent_val,
                "hivol_note": hivol_note,
            }

            vin_dir = pathlib.Path(UPLOAD_ROOT()) / vin_no
            vin_dir.mkdir(exist_ok=True)

            async def save_file(file: UploadFile, prefix: str):
                if file and file.filename:
                    fname = f"battery_{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
                    dest = vin_dir / fname
                    with open(dest, "wb") as buffer:
                        buffer.write(await file.read())
                    return f"{vin_no}/{fname}"
                return None

            data["volt12_file1"] = await save_file(volt12_file1, "12v_1")
            data["volt12_file2"] = await save_file(volt12_file2, "12v_2")
            data["hivol_file1"] = await save_file(hivol_file1, "hv_1")
            data["hivol_file2"] = await save_file(hivol_file2, "hv_2")
            
            final_data = {k: v for k, v in data.items() if v is not None}

            keys = ", ".join(final_data.keys())
            placeholders = ", ".join(["?"] * len(final_data))
            conn.execute(f"INSERT INTO battery_checks ({keys}) VALUES ({placeholders})", tuple(final_data.values()))
            conn.commit()
        finally:
            conn.close()

        return RedirectResponse(url=f"/battery/manage?token={vin_no}", status_code=303)

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    load_config()
    web = CONFIG.get("pdi_web", {})
    uvicorn.run(app, host=web.get("host", "0.0.0.0"), port=int(web.get("port", 9000)), log_level="info")