#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import sqlite3
import hashlib
import os
from datetime import datetime, date
from typing import List, Tuple, Optional
from math import ceil

from fastapi import FastAPI, Form, Query, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware

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

def STOCKOUT_DB_PATH() -> str:
    return CONFIG.get("stockout_db_path", "stockout.db")

def ensure_inventory(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS inventory(
        vin_no TEXT PRIMARY KEY,
        id_van TEXT,
        in_stock INTEGER NOT NULL DEFAULT 1,
        updated_at TEXT
    )""")
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

def ensure_movements(conn: sqlite3.Connection) -> None:
    conn.execute("""CREATE TABLE IF NOT EXISTS movements(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vin_no TEXT NOT NULL,
        at TEXT NOT NULL,
        action TEXT,
        from_slot TEXT,
        to_slot TEXT,
        note TEXT,
        source TEXT
    )""")
    conn.commit()

# --- ⭐️ START: ระบบตรวจนับ (Count System) - ตารางใหม่ ---
def ensure_count_tables(conn: sqlite3.Connection) -> None:
    """สร้างตารางสำหรับเก็บ Job และ Item การตรวจนับ"""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS inventory_count_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        completed_at TEXT,
        created_by TEXT
    )""")
    
    conn.execute("""
    CREATE TABLE IF NOT EXISTS inventory_count_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        vin_no TEXT NOT NULL,
        id_van TEXT,
        model TEXT,
        new_slot TEXT,
        counted_at TEXT NOT NULL,
        FOREIGN KEY (job_id) REFERENCES inventory_count_jobs(id),
        UNIQUE(job_id, vin_no)
    )""")
    conn.commit()
# --- ⭐️ END: ระบบตรวจนับ (Count System) - ตารางใหม่ ---

# --- ⭐️ START: ตารางใหม่สำหรับข้อมูลเพิ่มเติม ---
def ensure_vehicle_registration_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS vehicle_registration (
        vin_no TEXT PRIMARY KEY,
        plate_number TEXT,
        tax_due_date TEXT,
        updated_at TEXT,
        updated_by TEXT
    )""")
    conn.commit()

def ensure_vehicle_type_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS vehicle_type (
        vin_no TEXT PRIMARY KEY,
        type_name TEXT,
        updated_at TEXT,
        updated_by TEXT
    )""")
    conn.commit()

def ensure_delivery_prep_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS delivery_prep (
        vin_no TEXT PRIMARY KEY,
        paint_side_status TEXT DEFAULT 'N/A',
        paint_plate_status TEXT DEFAULT 'N/A',
        sticker_status TEXT DEFAULT 'N/A',
        sticker_details TEXT,
        taxi_equip_status TEXT DEFAULT 'N/A',
        updated_at TEXT,
        updated_by TEXT
    )""")
    conn.commit()
# --- ⭐️ END: ตารางใหม่สำหรับข้อมูลเพิ่มเติม ---


# --- ⭐️ START: ฟังก์ชัน Helper ใหม่สำหรับเขียน Log 'in_yard' ---
def _log_stock_in(conn_stockout: sqlite3.Connection, vin_no: str, id_van: str, source: str, new_slot: str):
    """Logs an 'in_yard' action to the stockout.db. Assumes conn_stockout is a connection to stockout.db."""
    try:
        _at = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        conn_stockout.execute(
            # Assuming stockout_logs table exists and has these columns
            "INSERT INTO stockout_logs(vin_no, id_van, action, at, source, location) VALUES (?, ?, 'in_yard', ?, ?, ?)",
            (vin_no, id_van, _at, source, new_slot) # Use new_slot as location for clarity
        )
    except Exception as e:
        print(f"Error logging stock_in to stockout.db for {vin_no}: {e}")
        # Re-raise the exception to trigger a rollback of both databases
        raise e
# --- ⭐️ END: ฟังก์ชัน Helper ใหม่ ---


def log_movement(conn: sqlite3.Connection, vin_no: str, action: str,
                 from_slot: str = None, to_slot: str = None,
                 note: str = "", source: str = "inventory_web") -> None:
    ensure_movements(conn)
    at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO movements(vin_no,at,action,from_slot,to_slot,note,source) "
        "VALUES(?,?,?,?,?,?,?)",
        (vin_no, at, action, from_slot, to_slot, note, source)
    )
    # ไม่ commit ที่นี่ ปล่อยให้ function ที่เรียกใช้เป็นคน commit
    # conn.commit() 

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

def get_model(vin_no: str) -> Optional[str]:
    conn = sqlite3.connect(DB_PATH())
    try:
        for tbl in ("vehicles", "stocks", "stock", "car_stock"):
            try:
                r = conn.execute(f"SELECT model FROM {tbl} WHERE vin_no=?", (vin_no,)).fetchone()
                if r and r[0]:
                    return str(r[0])
            except Exception:
                continue
        return None
    finally:
        conn.close()

def get_color(vin_no: str) -> Optional[str]:
    conn = sqlite3.connect(DB_PATH())
    try:
        for tbl in ("vehicles", "stocks", "stock", "car_stock"):
            for col in ("exterior_color", "color", "ext_color"):
                try:
                    r = conn.execute(f"SELECT {col} FROM {tbl} WHERE vin_no=?", (vin_no,)).fetchone()
                    if r and r[0]:
                        return str(r[0])
                except Exception:
                    continue
        return None
    finally:
        conn.close()

def get_vehicle_slot_status(vin_no: str) -> Tuple[str, str]:
    conn = sqlite3.connect(DB_PATH())
    try:
        ensure_vehicle_columns(conn)
        r = conn.execute(
            "SELECT COALESCE(slot,''), COALESCE(status,'') FROM vehicles WHERE vin_no=?",
            (vin_no,)
        ).fetchone()
        return (r[0] if r else "", r[1] if r else "")
    finally:
        conn.close()

def get_inventory_confirmed(vin_no: str) -> int:
    conn = sqlite3.connect(DB_PATH())
    try:
        ensure_inventory(conn)
        r = conn.execute(
            "SELECT COALESCE(in_stock,1) FROM inventory WHERE vin_no=?",
            (vin_no,)
        ).fetchone()
        return int(r[0]) if r else 0
    finally:
        conn.close()

def get_pdi_state(vin_no: str) -> Tuple[str, int]:
    conn = sqlite3.connect(DB_PATH())
    try:
        try:
            r = conn.execute(
                "SELECT id,status,percent_ok FROM pdi_jobs WHERE vin_no=? "
                "ORDER BY id DESC LIMIT 1", (vin_no,)
            ).fetchone()
            if not r:
                return ("-", 0)
            return (r[1] or "-", int(r[2] or 0))
        except Exception:
            return ("-", 0)
    finally:
        conn.close()

def get_export_state(vin_no: str) -> str:
    conn = sqlite3.connect(DB_PATH())
    try:
        try:
            r = conn.execute(
                "SELECT status FROM export_jobs WHERE vin_no=? ORDER BY id DESC LIMIT 1",
                (vin_no,)
            ).fetchone()
            return r[0] if r else "-"
        except Exception:
            return "-"
    finally:
        conn.close()

def get_damage_lock_status(vin_no: str) -> bool:
    conn = sqlite3.connect(DB_PATH())
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='damage_reports'")
        if cursor.fetchone() is None:
            return False
        r = conn.execute(
            "SELECT is_locked FROM damage_reports WHERE vin_no = ? AND status = 'pending'",
            (vin_no,)
        ).fetchone()
        return bool(r and r[0] == 1)
    except Exception:
        return False
    finally:
        conn.close()

def get_latest_battery_check(vin_no: str) -> Optional[sqlite3.Row]:
    conn = sqlite3.connect(DB_PATH())
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='battery_checks'")
        if cursor.fetchone() is None:
            return None
            
        r = conn.execute(
            "SELECT * FROM battery_checks WHERE vin_no=? ORDER BY id DESC LIMIT 1",
            (vin_no,)
        ).fetchone()
        return r
    except Exception:
        return None
    finally:
        conn.close()

def get_latest_vdci_report_pair(vin_no: str) -> Optional[sqlite3.Row]:
    """
    Fetches the latest VDCI report pair for a given VIN.
    """
    conn = sqlite3.connect(DB_PATH())
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='vdci_report_pairs'")
        if cursor.fetchone() is None:
            return None
        
        r = conn.execute(
            "SELECT * FROM vdci_report_pairs WHERE vin_no=? ORDER BY id DESC LIMIT 1",
            (vin_no,)
        ).fetchone()
        return r
    except sqlite3.OperationalError:
        return None
    except Exception as e:
        print(f"Error fetching VDCI report pair: {e}")
        return None
    finally:
        conn.close()

# --- ⭐️ START: ฟังก์ชัน Get Info ใหม่ ---
def get_registration_info(vin_no: str) -> Optional[sqlite3.Row]:
    conn = sqlite3.connect(DB_PATH())
    try:
        conn.row_factory = sqlite3.Row
        ensure_vehicle_registration_table(conn) # Ensure table exists
        r = conn.execute("SELECT * FROM vehicle_registration WHERE vin_no=?", (vin_no,)).fetchone()
        return r
    except Exception:
        return None
    finally:
        conn.close()

def get_vehicle_type_info(vin_no: str) -> Optional[sqlite3.Row]:
    conn = sqlite3.connect(DB_PATH())
    try:
        conn.row_factory = sqlite3.Row
        ensure_vehicle_type_table(conn) # Ensure table exists
        r = conn.execute("SELECT * FROM vehicle_type WHERE vin_no=?", (vin_no,)).fetchone()
        return r
    except Exception:
        return None
    finally:
        conn.close()

def get_delivery_prep_info(vin_no: str) -> Optional[sqlite3.Row]:
    conn = sqlite3.connect(DB_PATH())
    try:
        conn.row_factory = sqlite3.Row
        ensure_delivery_prep_table(conn) # Ensure table exists
        r = conn.execute("SELECT * FROM delivery_prep WHERE vin_no=?", (vin_no,)).fetchone()
        return r
    except Exception:
        return None
    finally:
        conn.close()
# --- ⭐️ END: ฟังก์ชัน Get Info ใหม่ ---


# --- ⭐️ START: [NEW] ฟังก์ชันดึงสถานะ Longterm ---
def get_longterm_status(vin_no: str) -> Tuple[str, str, bool]:
    """
    ดึงสถานะ Longterm Maintenance ล่าสุด
    :return: (Last_Done_Cycle, Active_Cycle, Is_Locked)
    """
    conn = sqlite3.connect(DB_PATH())
    try:
        conn.row_factory = sqlite3.Row
        # ตรวจสอบว่าตาราง longterm_jobs มีอยู่หรือไม่
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='longterm_jobs'")
        if cursor.fetchone() is None:
            return ("-", "-", False) # ถ้าไม่มีตาราง ก็คืนค่าว่าง
        
        # 1. หางานล่าสุดที่ทำเสร็จ (complete)
        last_done_row = conn.execute(
            "SELECT cycle_day FROM longterm_jobs WHERE vin_no=? AND status='complete' ORDER BY datetime(done_at) DESC, id DESC LIMIT 1",
            (vin_no,)
        ).fetchone()
        last_done_str = f"{last_done_row['cycle_day']}d" if last_done_row else "-"

        # 2. หางานที่กำลังทำ (active)
        active_job_row = conn.execute(
            "SELECT id, cycle_day, locked FROM longterm_jobs WHERE vin_no=? AND status='active' ORDER BY id DESC LIMIT 1",
            (vin_no,)
        ).fetchone()
        
        active_str = f"{active_job_row['cycle_day']}d" if active_job_row else "-"
        is_locked = bool(active_job_row and active_job_row['locked'])
        
        return (last_done_str, active_str, is_locked)
    except Exception as e:
        print(f"Error get_longterm_status: {e}")
        return ("Error", "Error", False) # คืนค่า Error ถ้า Query ไม่ได้
    finally:
        conn.close()
# --- ⭐️ END: [NEW] ฟังก์ชันดึงสถานะ Longterm ---


def get_damage_report_id(vin_no: str) -> Optional[int]:
    conn = sqlite3.connect(DB_PATH())
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='damage_reports'")
        if cursor.fetchone() is None:
            return None
        r = conn.execute(
            "SELECT id FROM damage_reports WHERE vin_no = ? AND status = 'pending' ORDER BY id DESC LIMIT 1",
            (vin_no,)
        ).fetchone()
        return r[0] if r else None
    except Exception:
        return None
    finally:
        conn.close()

def has_out_yard(vin_no: str, id_van: str = "") -> bool:
    vin_no = (vin_no or "").strip()
    id_van = (id_van or "").strip()
    if not vin_no and not id_van:
        return False
    conn = sqlite3.connect(STOCKOUT_DB_PATH())
    try:
        conditions, params = [], []
        if vin_no:
            conditions.append("vin_no=?"); params.append(vin_no)
        if id_van:
            conditions.append("id_van=?"); params.append(id_van)
        if not conditions:
            return False
        where_clause = " OR ".join(conditions)
        row = conn.execute(
            f"SELECT action, source FROM stockout_logs WHERE ({where_clause}) ORDER BY id DESC LIMIT 1",
            tuple(params)
        ).fetchone()
        if not row:
            return False
        last_action = (row[0] or "").lower()
        last_source = (row[1] or "").lower()
        return last_source == 'cmd' and last_action == 'out_yard'
    except Exception:
        return False
    finally:
        conn.close()

def set_vehicle_slot(vin_no: str, id_van: str, slot: str) -> None:
    conn = sqlite3.connect(DB_PATH())
    try:
        ensure_vehicle_columns(conn)
        old = conn.execute("SELECT COALESCE(slot,'') FROM vehicles WHERE vin_no=?", (vin_no,)).fetchone()
        old_slot = old[0] if old else ""
        conn.execute("UPDATE vehicles SET slot=? WHERE vin_no=?", (slot, vin_no))
        log_movement(conn, vin_no, action="slot_update", from_slot=old_slot, to_slot=slot,
                     note="", source="inventory_web")
        conn.commit()
    finally:
        conn.close()

def confirm_in_stock(vin_no: str, id_van: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_PATH())
    try:
        ensure_inventory(conn)
        conn.execute(
            """
            INSERT INTO inventory(vin_no,id_van,in_stock,updated_at)
            VALUES(?,?,1,?)
            ON CONFLICT(vin_no) DO UPDATE SET
              id_van=excluded.id_van,
              in_stock=1,
              updated_at=excluded.updated_at
            """,
            (vin_no, id_van, now)
        )
        conn.commit()
    finally:
        conn.close()

def unconfirm_in_stock(vin_no: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_PATH())
    try:
        ensure_inventory(conn)
        conn.execute("""
            INSERT INTO inventory(vin_no,id_van,in_stock,updated_at)
            VALUES(?, (SELECT id_van FROM vehicles WHERE vin_no=? LIMIT 1), 0, ?)
            ON CONFLICT(vin_no) DO UPDATE SET
              in_stock=0,
              updated_at=excluded.updated_at
        """, (vin_no, vin_no, now))
        conn.commit()
    finally:
        conn.close()

def base_layout(title: str, body: str, active_tab: str = "inventory") -> HTMLResponse:
    # --- ⭐️ START: เพิ่ม Tab ใหม่ ---
    tabs_html = f"""
    <div style="margin: 0 0 16px 0;">
      <a class="btn {'warn' if active_tab == 'inventory' else ''}" href="/">INVENTORY</a>
      <a class="btn {'warn' if active_tab == 'count' else ''}" href="/count">ตรวจนับสต็อก</a>
    </div>
    """
    # --- ⭐️ END: เพิ่ม Tab ใหม่ ---

    return HTMLResponse(f"""<!doctype html>
<html lang="th">
<head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{title}</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:24px;background:#fafafa}}
.card{{border:1px solid #eee;border-radius:10px;padding:16px;margin:12px 0;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.03)}}
.small{{color:#666;font-size:12px}}
.btn{{display:inline-block;padding:6px 10px;border-radius:6px;border:1px solid #ddd;background:#f7f7f7;text-decoration:none;color:#111;cursor:pointer; margin: 2px;}}
.btn.ng{{background:#fdecea;border-color:#f5c2bf}}
.btn.ok{{background:#e8f6ec;border-color:#bfe6cd}}
.btn.warn{{background:#fff2e6;border-color:#ffd6a6}}
input[type=text],input[type=date],input[type=password]{{padding:8px 10px;border:1px solid #ddd;border-radius:6px;min-width:280px}}
.badge{{display:inline-block;padding:2px 8px;border-radius:999px;background:#111;color:#fff;font-size:12px}}
ul.dd{{list-style:none;margin:6px 0 0;padding:0;border:1px solid #eee;border-radius:8px;background:#fff;max-width:520px;box-shadow:0 2px 12px rgba(0,0,0,.06)}}
ul.dd li{{padding:8px 12px;border-bottom:1px solid #f1f1f1;cursor:pointer}}
ul.dd li:last-child{{border-bottom:none}}
ul.dd li:hover{{background:#f7f7f7}}
.grid2{{display:grid;grid-template-columns:140px 1fr;gap:8px;align-items:center}}
.ok{{color:#0b7a29;font-weight:700}} .ng{{color:#b3251d;font-weight:700}}
table{{width:100%;border-collapse:collapse}} th,td{{padding:6px;font-size:13px;border:1px solid #eee}}
.pagination a {{ margin: 0 4px; text-decoration: none; }}
.pagination strong {{ margin: 0 4px; color: #000; font-weight: bold; }}
</style>
</head>
<body>
{tabs_html}
{body}
</body>
</html>""")

def create_app() -> FastAPI:
    load_config()
    app = FastAPI(title="Inventory Web")

    secret_key = os.environ.get("INV_SESSION_SECRET", "inv_default_dev_secret_change_me")
    try:
        app.add_middleware(SessionMiddleware, secret_key=secret_key, same_site="lax", https_only=False)
    except Exception:
        pass

    ALLOWED_HASHES = load_inv_hashes()
    PUBLIC_PATHS = {"/login", "/logout", "/openapi.json", "/docs", "/redoc", "/healthz"}

    LOGIN_HTML = """<!doctype html>
    <html>
      <head><meta charset="utf-8"><title>Inventory Login</title></head>
      <body style="font-family: Arial, Helvetica, sans-serif; padding:40px; max-width:600px;">
        <h2>Inventory - Login</h2>
        <form method="post" action="/login">
          <label>Password: <input type="password" name="password" autofocus></label>
          <button type="submit">Login</button>
        </form>
      </body>
    </html>"""

    @app.get("/healthz")
    async def healthz():
        return JSONResponse({"ok": True, "app": "inventory_web"})

    @app.get("/login")
    async def login_get():
        return HTMLResponse(LOGIN_HTML.replace("", ""))

    @app.post("/login")
    async def login_post(password: str = Form(...)):
        h = hashlib.sha256(password.encode("utf-8")).hexdigest()
        if h in ALLOWED_HASHES:
            resp = RedirectResponse(url="/", status_code=302)
            resp.set_cookie("inv_auth_session", h, httponly=True, samesite="lax")
            return resp
        return HTMLResponse(
            LOGIN_HTML.replace("", "<p style='color:red'>รหัสผ่านไม่ถูกต้อง</p>"),
            status_code=401
        )

    @app.get("/logout")
    async def logout():
        res = RedirectResponse(url="/login")
        try:
            res.delete_cookie("inv_auth_session")
            res.delete_cookie("session")
        except Exception:
            pass
        return res

    @app.middleware("http")
    async def _inv_auth_guard(request: Request, call_next):
        path = request.url.path or "/"
        if path in PUBLIC_PATHS or any(path.startswith(p + "/") for p in PUBLIC_PATHS):
            return await call_next(request)
        try:
            if request.session.get("inv_auth_ok"):
                return await call_next(request)
        except Exception:
            pass
        try:
            c = request.cookies.get("inv_auth_session")
            if c and c in ALLOWED_HASHES:
                try:
                    request.session["inv_auth_ok"] = True
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
                authed = bool(request.session.get("inv_auth_ok"))
            except Exception:
                authed = False
            try:
                ck = request.cookies.get("inv_auth_session")
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

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # --- ⭐️ START: เรียกใช้ ensure_..._tables ---
    conn = sqlite3.connect(DB_PATH())
    try:
        ensure_count_tables(conn)
        ensure_vehicle_registration_table(conn)
        ensure_vehicle_type_table(conn)
        ensure_delivery_prep_table(conn)
    finally:
        conn.close()
    # --- ⭐️ END: เรียกใช้ ensure_..._tables ---


    @app.get("/", response_class=HTMLResponse)
    def home():
        script = """
        <script>
        (function(){
          var q = document.getElementById('q');
          var dd = document.getElementById('dd');
          var timer = null;
          q.addEventListener('input', function(){
            var v = q.value.trim();
            if(timer) clearTimeout(timer);
            if(v.length < 2){ dd.style.display='none'; dd.innerHTML=''; return; }
            timer = setTimeout(function(){
              fetch('/api/search?q='+encodeURIComponent(v))
                .then(r=>r.json()).then(items=>{
                  if(!items || !items.length){ dd.style.display='none'; dd.innerHTML=''; return; }
                  dd.innerHTML = items.map(function(it){
                    var vin = it.vin_no || ''; var idv = it.id_van || ''; var mdl = it.model || '';
                    return '<li data-vin="'+vin+'"><b>'+vin+'</b> &nbsp; <span class="small">'+(idv||'-')+'</span> &nbsp; <span class="badge">'+(mdl||'-')+'</span></li>';
                  }).join('');
                  dd.style.display='block';
                  dd.querySelectorAll('li').forEach(function(li){
                    li.onclick = function(){ location.href='/manage?token='+encodeURIComponent(this.getAttribute('data-vin')); };
                  });
                });
            }, 200);
          });
        })();
        </script>
        """
        body = f"""
        <div class="card">
          <h2>🔎 ค้นหา VIN / ID VAN (Inventory)</h2>
          <input id="q" type="text" placeholder="พิมพ์เพื่อค้นหา..."/>
          <ul id="dd" class="dd" style="display:none"></ul>
          <p class="small">พิมพ์อย่างน้อย 2 ตัวอักษร จากนั้นเลือกเพื่อไปหน้าจัดการ</p>
          <hr style="margin-top:20px; border:none; border-top:1px solid #eee;">
          <a class="btn" href="/report/movement" style="margin-top:12px;">รายงานการเคลื่อนไหว</a>
        </div>
        """ + script
        return base_layout("Inventory | Search", body, active_tab="inventory")

    @app.get("/manage", response_class=HTMLResponse)
    def manage(request: Request, token: str = Query(..., description="VIN หรือ ID VAN")):
        vin, idv = resolve_vin_idvan(token)
        if not vin:
            return base_layout("Inventory | ไม่พบ", "<div class='card'>❌ ไม่พบข้อมูล VIN/ID VAN นี้</div>")
        mdl = get_model(vin) or "-"
        col = get_color(vin) or "-"
        slot, vstatus = get_vehicle_slot_status(vin)
        confirmed = get_inventory_confirmed(vin)
        pdi_status, pdi_pct = get_pdi_state(vin)
        # export_status = get_export_state(vin) # ⭐️ REMOVED
        outed = has_out_yard(vin, idv or "")
        
        # ⭐️ ADDED: Get new info
        reg_info = get_registration_info(vin)
        type_info = get_vehicle_type_info(vin)
        prep_info = get_delivery_prep_info(vin)
        
        # --- ⭐️ START: [NEW] ดึงข้อมูล Longterm ---
        last_lt, active_lt, locked_lt = get_longterm_status(vin)
        longterm_display = ""
        if active_lt != "-":
            longterm_display = f"<span class='warn'>Active: {active_lt}</span>"
            if locked_lt:
                # ถ้า Active และ Locked (ติด NOK)
                longterm_display += " <span class='ng' style='border:1px solid red; padding: 1px 4px; border-radius:4px;'>🔒LOCKED</span>"
        elif last_lt != "-":
            longterm_display = f"<span class='ok'>Done: {last_lt}</span>"
        else:
            longterm_display = "<span>-</span>"
        # --- ⭐️ END: [NEW] ดึงข้อมูล Longterm ---
        
        is_damage_locked = get_damage_lock_status(vin)
        damage_lock_html = ""
        if is_damage_locked:
            report_id = get_damage_report_id(vin)
            detail_link = ""
            if report_id:
                pdi_web_port = CONFIG.get("pdi_web", {}).get("port", 9000)
                detail_link = f'<a class="btn ng" href="http://{request.url.hostname}:{pdi_web_port}/damage/job/{report_id}?back_token={token}">ดูรายละเอียด</a>'
            
            damage_lock_html = f"""
            <div style="color:red; font-weight:bold; margin: 10px 0; border: 1px solid red; padding: 8px; border-radius: 6px; background-color: #fdecea;">
            🔒 ถูกล็อกโดยระบบแจ้งซ่อม (ไม่สามารถนำออกได้) {detail_link}
            </div>
            """
            
        battery_check = get_latest_battery_check(vin)
        volt12_status = "-"
        hivol_percent = "-"
        if battery_check:
            volt12_status = battery_check['volt12_status'] or "-"
            if battery_check['hivol_percent'] is not None:
                hivol_percent = f"{battery_check['hivol_percent']}%"

        vdci_report_pair = get_latest_vdci_report_pair(vin)
        vdci_html = ""
        if vdci_report_pair:
            pdi_web_port = CONFIG.get("pdi_web", {}).get("port", 9000)
            compare_url = f"http://{request.url.hostname}:{pdi_web_port}/vdci/compare/{vdci_report_pair['id']}"
            vdci_html = f"""
            <div>VDCI Report</div>
            <div>
                <span class="ok">มี</span>
                <a class="btn warn" href="{compare_url}" target="_blank">ดูผลเปรียบเทียบ VDCI ล่าสุด</a>
            </div>
            """
        else:
            vdci_html = """
            <div>VDCI Report</div>
            <div><span class="ng">ไม่มี</span></div>
            """

        status_html = "<span class='badge'>ยืนยัน</span>" if confirmed else "<span class='badge'>UNCONFIRMED</span>"

        # ⭐️ ADDED: Helper logic to format new display strings
        reg_display = f"<b>{reg_info['plate_number'] or 'N/A'}</b> (ภาษี: {reg_info['tax_due_date'] or 'N/A'})" if reg_info else "<span class='small'><i>ไม่มีข้อมูล</i></span>"
        type_display = f"<b>{type_info['type_name'] or 'N/A'}</b>" if type_info else "<span class='small'><i>ไม่มีข้อมูล</i></span>"
        
        prep_summary = "<span class='small'><i>ไม่มีข้อมูล</i></span>"
        if prep_info:
            def p_status(s):
                if s == 'OK': return "<span class='ok'>OK</span>"
                if s == 'NOK': return "<span class='ng'>NOK</span>"
                return "<span class='small'>N/A</span>"
            
            prep_summary = (
                f"พ่นข้าง: {p_status(prep_info['paint_side_status'])} | "
                f"พ่นทะเบียน: {p_status(prep_info['paint_plate_status'])} | "
                f"สติกเกอร์: {p_status(prep_info['sticker_status'])} ({prep_info['sticker_details'] or '-'}) | "
                f"Taxi: {p_status(prep_info['taxi_equip_status'])}"
            )

        # ⭐️ MODIFIED: info_html
        info_html = f"""
        <div class="card">
          <h3>Inventory Info</h3>
          {damage_lock_html}
          <div class="grid2">
            <div>VIN</div><div><b>{vin}</b></div>
            <div>ID VAN</div><div>{idv or '-'}</div>
            <div>MODEL</div><div>{mdl}</div>
            <div>EXTERIOR</div><div>{col}</div>
            <div>VEH Slot</div><div><b>{slot or '-'}</b></div>
            <div>VEH Status</div><div>{vstatus or '-'}</div>
            <div>ยืนยันในระบบ</div><div>{status_html}</div>
            <div>PDI</div>
            <div>
              {pdi_status} {('(' + str(pdi_pct)+'%)' if pdi_pct else '')}
              &nbsp; <a class="btn" href="/status/pdi?vin={vin}">ดูสถานะ</a>
            </div>
            
            <div>ทะเบียน/ภาษี</div>
            <div style="font-size:12px;">
              {reg_display}
              &nbsp; <a class="btn" href="/manage/registration?token={token}">แก้ไข</a>
            </div>
            <div>ประเภทรถ</div>
            <div style="font-size:12px;">
              {type_display}
              &nbsp; <a class="btn" href="/manage/vehicle_type?token={token}">แก้ไข</a>
            </div>
            <div>งานเตรียมส่งมอบ</div>
            <div style="font-size:12px;">
              {prep_summary}
              &nbsp; <a class="btn" href="/manage/delivery_prep?token={token}">แก้ไข</a>
            </div>
            <div>ตรวจแบตเตอรี่</div>
            <div>
              12V: {volt12_status} | HV: {hivol_percent}
              &nbsp; <a class="btn" href="/status/battery?vin={vin}">ดูประวัติ</a>
            </div>
            {vdci_html}
            
            <div>Longterm</div>
            <div>
              {longterm_display}
              &nbsp; <a class="btn" href="/status/longterm?vin={vin}">ดูประวัติ</a>
            </div>
            <div>นำออกจาก Yard</div><div>{'✅' if outed else '—'}&nbsp; <a class='btn' href="/status/out?vin={vin}">ดูสถานะ</a></div>
            <div>Movement</div><div><a class="btn" href="/movement?vin={vin}">เปิดดูรายการ</a></div>
          </div>
        </div>
        """

        script_manage = """
        <script>
        (function(){
          function postForm(f, cb){
            fetch(f.action, {method:'POST', body:new FormData(f)}).then(r=>r.json()).then(cb).catch(console.error);
          }
          document.getElementById('slot-form').onsubmit = function(ev){
            ev.preventDefault();
            postForm(this, function(res){
              alert(res.ok ? 'บันทึก Slot แล้ว' : ('ผิดพลาด: '+(res.error||'')));
              if(res.ok) location.reload();
            });
          };
          document.getElementById('confirm-form').onsubmit = function(ev){
            ev.preventDefault();
            postForm(this, function(res){
              if(res.ok){
                alert('ยืนยันสต็อกแล้ว');
                location.reload();
              }else{
                alert('ผิดพลาด: '+(res.error||''));
              }
            });
          };
          var uf = document.getElementById('unconfirm-form');
          if(uf){
            uf.onsubmit = function(ev){
              ev.preventDefault();
              postForm(this, function(res){
                if(res.ok){ alert('ยกเลิกยืนยันแล้ว'); location.reload(); }
                else{ alert('ผิดพลาด: '+(res.error||'')); }
              });
            };
          }
        })();
        </script>
        """

        body = f"""
        <div class="card">
          <h2>📦 จัดการ Inventory</h2>
          <div class="small">VIN: <b>{vin}</b> &nbsp; | ID VAN: <b>{idv or '-'}</b> &nbsp; | MODEL: {mdl} | EXTERIOR: {col}</div>
        </div>
        {info_html}
        <div class="card">
          <div class="grid2">
            <div>ปรับ Slot</div>
            <div>
              <form id="slot-form" method="post" action="/api/update_slot">
                <input type="hidden" name="vin" value="{vin}"/>
                <input type="hidden" name="id_van" value="{idv}"/>
                <input type="text" name="slot" placeholder="เช่น A-01 / EXPORT-2" value="{slot or ''}" />
                <button class="btn" type="submit">บันทึก Slot</button>
              </form>
            </div>

            <div>ยืนยัน</div>
            <div>
              <form id="confirm-form" method="post" action="/api/confirm_stock" style="display:inline">
                <input type="hidden" name="vin" value="{vin}"/>
                <input type="hidden" name="id_van" value="{idv}"/>
                <button class="btn" type="submit">ยืนยันว่ามี Stock</button>
              </form>
              <form id="unconfirm-form" method="post" action="/api/unconfirm_stock" style="display:inline;margin-left:8px">
                <input type="hidden" name="vin" value="{vin}"/>
                <button class="btn" type="submit">ยกเลิกยืนยัน Stock</button>
              </form>
              <a class="btn" href="/">กลับหน้าค้นหา</a>
              <a class="btn" href="/" id="finish-btn">จบการจัดการ</a>
            </div>
          </div>
        </div>
        """ + script_manage
        return base_layout("Inventory | Manage", body, active_tab="inventory")

    # 📌 START: BUG FIX SECTION
    @app.get("/report/movement", response_class=HTMLResponse)
    async def movement_report(report_date: Optional[str] = Query(None)):
        selected_date_str = report_date or date.today().strftime("%Y-%m-%d")
        all_movements = []

        # 1. Get movements from stock.db
        conn_stock = sqlite3.connect(DB_PATH())
        conn_stock.row_factory = sqlite3.Row
        try:
            # 🐞 BUG FIX: Changed 'date(at) = ?' to 'at LIKE ?'
            stock_movements = conn_stock.execute(
                "SELECT at, vin_no, action, from_slot, to_slot, note, source FROM movements WHERE at LIKE ? ORDER BY at DESC",
                (f"{selected_date_str}%",) # ใช้ YYYY-MM-DD%
            ).fetchall()
            for r in stock_movements:
                all_movements.append(dict(r))
        except Exception as e:
            print(f"Error querying movements: {e}")
        finally:
            conn_stock.close()

        # 2. Get out-yard movements from stockout.db
        conn_stockout = sqlite3.connect(STOCKOUT_DB_PATH())
        conn_stockout.row_factory = sqlite3.Row
        try:
            # Convert YYYY-MM-DD to DD-MM-YYYY for LIKE query
            selected_date_ddmmyyyy = datetime.strptime(selected_date_str, "%Y-%m-%d").strftime("%d-%m-%Y")
            
            out_movements = conn_stockout.execute(
                "SELECT at, vin_no, id_van, action, source, location FROM stockout_logs WHERE at LIKE ?",
                (f"{selected_date_ddmmyyyy}%",) # ใช้ DD-MM-YYYY%
            ).fetchall()
            for r in out_movements:
                all_movements.append({
                    "at": r["at"], "vin_no": r["vin_no"], "action": r["action"],
                    "from_slot": "", "to_slot": "",
                    "note": f"Location: {r['location'] or 'N/A'}", "source": r["source"]
                })
        except Exception as e:
            print(f"Error querying stockout_logs: {e}")
        finally:
            conn_stockout.close()
        
        # 3. Sort all movements by timestamp
        def sort_key(item):
            try:
                # Try parsing DD-MM-YYYY HH:MM:SS
                return datetime.strptime(item['at'], "%d-%m-%Y %H:%M:%S")
            except ValueError:
                try:
                    # Try parsing YYYY-MM-DD HH:MM:SS
                    return datetime.strptime(item['at'], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    # Fallback for unparseable dates
                    return datetime.min

        all_movements.sort(key=sort_key, reverse=True)

        # 4. Render HTML
        trs = []
        for r in all_movements:
            trs.append(f"""
                <tr>
                    <td>{r.get('at', '')}</td>
                    <td>{r.get('vin_no', '')}</td>
                    <td>{r.get('action', '')}</td>
                    <td>{r.get('from_slot', '') or '-'}</td>
                    <td>{r.get('to_slot', '') or '-'}</td>
                    <td class='small'>{r.get('note', '')}</td>
                    <td class='small'>{r.get('source', '')}</td>
                </tr>
            """)

        body = f"""
        <div class="card">
            <h2>รายงานการเคลื่อนไหว</h2>
            <form method="get" action="/report/movement">
                <label for="report_date">เลือกวันที่:</label>
                <input type="date" id="report_date" name="report_date" value="{selected_date_str}">
                <button type="submit" class="btn">ดูรายงาน</button>
            </form>
        </div>
        <div class="card">
            <h3>ข้อมูลวันที่ {selected_date_str} (ทั้งหมด {len(all_movements)} รายการ)</h3>
            <table>
                <thead>
                    <tr>
                        <th>เวลา</th>
                        <th>VIN</th>
                        <th>Action</th>
                        <th>จาก</th>
                        <th>ไปที่</th>
                        <th>Note / Location</th>
                        <th>Source</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(trs) if trs else "<tr><td colspan='7' class='small'>ไม่พบข้อมูลในวันที่เลือก</td></tr>"}
                </tbody>
            </table>
        </div>
        """
        return base_layout(f"รายงานการเคลื่อนไหว {selected_date_str}", body, active_tab="inventory")
    # 📌 END: BUG FIX SECTION

    @app.get("/status/battery", response_class=HTMLResponse)
    def status_battery(request: Request, vin: str = Query(...)):
        vin_res, idv = resolve_vin_idvan(vin)
        vin_key = vin_res or vin

        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            checks = conn.execute(
                "SELECT * FROM battery_checks WHERE vin_no=? ORDER BY id DESC",
                (vin_key,)
            ).fetchall()
        except Exception:
            checks = []
        finally:
            conn.close()

        if not checks:
            body = f"""
            <div class="card">
              <h2>ประวัติการตรวจแบตเตอรี่</h2>
              <p><a class="btn" href="/manage?token={vin_key}">← กลับหน้าจัดการ</a></p>
              <p class="small">ไม่มีประวัติการตรวจแบตเตอรี่สำหรับรถคันนี้</p>
            </div>
            """
            return base_layout("Inventory | Battery History", body, active_tab="inventory")
        
        pdi_web_port = CONFIG.get("pdi_web", {}).get("port", 9000)
        upload_url_base = f"http://{request.url.hostname}:{pdi_web_port}/uploads_damage"

        trs = []
        for r in checks:
            v12_files = ' '.join(f'<a href="{upload_url_base}/{f}" target="_blank">ดู</a>' for f in [r['volt12_file1'], r['volt12_file2']] if f)
            hv_files = ' '.join(f'<a href="{upload_url_base}/{f}" target="_blank">ดู</a>' for f in [r['hivol_file1'], r['hivol_file2']] if f)
            trs.append(f"""
            <tr>
                <td class='small'>{r['check_at']}</td>
                <td>{r['volt12_status'] or '-'}</td>
                <td class='small'>{r['volt12_note'] or '-'}</td>
                <td>{v12_files}</td>
                <td>{r['hivol_status'] or '-'}</td>
                <td>{str(r['hivol_percent']) + '%' if r['hivol_percent'] is not None else '-'}</td>
                <td class='small'>{r['hivol_note'] or '-'}</td>
                <td>{hv_files}</td>
            </tr>
            """)
        
        body = f"""
        <div class="card">
            <h2>ประวัติการตรวจแบตเตอรี่: {vin_key}</h2>
            <p><a class="btn" href="/manage?token={vin_key}">← กลับหน้าจัดการ</a></p>
            <table>
                <thead>
                    <tr>
                        <th>เวลาตรวจ</th>
                        <th>12V Status</th>
                        <th>12V Note</th>
                        <th>12V Files</th>
                        <th>HV Status</th>
                        <th>HV %</th>
                        <th>HV Note</th>
                        <th>HV Files</th>
                    </tr>
                </thead>
                <tbody>{''.join(trs)}</tbody>
            </table>
        </div>
        """
        return base_layout(f"Inventory | Battery History", body, active_tab="inventory")

    @app.get("/status/out", response_class=HTMLResponse)
    def status_out(vin: str = Query(...)):
        vin_res, idv = resolve_vin_idvan(vin)
        vin_key = vin_res or vin
        idv_key = idv or ""
        conn = sqlite3.connect(STOCKOUT_DB_PATH())
        conn.row_factory = sqlite3.Row
        outs = []
        try:
            try:
                outs1 = conn.execute(
                    "SELECT stock_out_at AS at, vin_no, id_van, source, 'out_yard' AS action "
                    "FROM stock_outs WHERE vin_no=? OR id_van=? "
                    "ORDER BY datetime(substr(stock_out_at,7,4)||'-'||substr(stock_out_at,4,2)||'-'||substr(stock_out_at,1,2) || ' ' || substr(stock_out_at,12)) DESC",
                    (vin_key, idv_key)
                ).fetchall()
            except Exception:
                outs1 = []
            try:
                outs2 = conn.execute(
                    "SELECT at, vin_no, id_van, source, action FROM stockout_logs "
                    "WHERE (vin_no=? OR id_van=?) AND LOWER(COALESCE(action,''))='out_yard' "
                    "ORDER BY datetime(substr(at,7,4)||'-'||substr(at,4,2)||'-'||substr(at,1,2) || ' ' || substr(at,12)) DESC",
                    (vin_key, idv_key)
                ).fetchall()
            except Exception:
                outs2 = []
            outs = list(outs1) + list(outs2)
        finally:
            conn.close()
        if not outs:
            body = f"""
            <div class="card">
              <h2>สถานะการนำออกจาก Yard</h2>
              <p><a class="btn" href="/manage?token={vin_key}">← กลับหน้าจัดการ</a></p>
              <p class="small">ไม่มีข้อมูลการนำออกจาก Yard</p>
            </div>
            """
            return base_layout("Inventory | Out Status", body, active_tab="inventory")
        trs = []
        for r in outs:
            trs.append(f"<tr><td>{r['at']}</td><td>{r['vin_no'] or ''}</td><td>{r['id_van'] or ''}</td><td>{r['source'] or ''}</td><td>{r['action'] or ''}</td></tr>")
        body = f"""
        <div class="card">
          <h2>สถานะการนำออกจาก Yard</h2>
          <p><a class="btn" href="/manage?token={vin_key}">← กลับหน้าจัดการ</a></p>
          <table style="width:100%;border-collapse:collapse">
            <thead><tr><th>เวลา</th><th>VIN</th><th>ID VAN</th><th>Source</th><th>Action</th></tr></thead>
            <tbody>{''.join(trs)}</tbody>
          </table>
        </div>
        """
        return base_layout("Inventory | Out Status", body, active_tab="inventory")

    @app.get("/status/pdi", response_class=HTMLResponse)
    def status_pdi(vin: str = Query(...)):
        vin_res, idv = resolve_vin_idvan(vin)
        vin_key = vin_res or vin

        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            job = conn.execute(
                "SELECT id, status, percent_ok, created_at, updated_at "
                "FROM pdi_jobs WHERE vin_no=? ORDER BY id DESC LIMIT 1",
                (vin_key,)
            ).fetchone()
            if not job:
                body = f"""
                <div class="card">
                  <h2>PDI Status</h2>
                  <p><a class="btn" href="/manage?token={vin_key}">← กลับหน้าจัดการ</a></p>
                  <p class="small">ยังไม่มีงาน PDI สำหรับคันนี้</p>
                </div>
                """
                return base_layout("Inventory | PDI Status", body, active_tab="inventory")

            steps = conn.execute(
                "SELECT s.step_code, s.step_name, s.seq, "
                "COALESCE(r.status,'-') AS status, COALESCE(r.note,'') AS note, COALESCE(r.at,'') AS at "
                "FROM pdi_steps s "
                "LEFT JOIN pdi_results r ON r.step_code=s.step_code AND r.job_id=? "
                "ORDER BY s.seq ASC",
                (job["id"],)
            ).fetchall()
        finally:
            conn.close()

        trs = []
        for r in steps:
            trs.append(
                f"<tr><td>{r['seq']}</td><td>{r['step_code']}</td><td>{r['step_name']}</td>"
                f"<td>{r['status']}</td><td class='small'>{r['note']}</td><td class='small'>{r['at']}</td></tr>"
            )
        body = f"""
        <div class="card">
          <h2>PDI Status</h2>
          <p><a class="btn" href="/manage?token={vin_key}">← กลับหน้าจัดการ</a></p>
          <p class="small">สรุปงานล่าสุด: {job['status']} ({int(job['percent_ok'] or 0)}%)</p>
          <table style="width:100%;border-collapse:collapse">
            <thead><tr><th>#</th><th>Code</th><th>Step</th><th>Status</th><th>Note</th><th>เวลา</th></tr></thead>
            <tbody>{''.join(trs) if trs else "<tr><td colspan='6' class='small'>ไม่มีข้อมูลขั้นตอน</td></tr>"}</tbody>
          </table>
        </div>
        """
        return base_layout("Inventory | PDI Status", body, active_tab="inventory")

    # --- ⭐️ START: [NEW] Endpoint ประวัติ Longterm ---
    @app.get("/status/longterm", response_class=HTMLResponse)
    def status_longterm(vin: str = Query(...)):
        vin_res, idv = resolve_vin_idvan(vin)
        vin_key = vin_res or vin
        error_msg = ""
        jobs = []

        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            # ตรวจสอบว่าตาราง longterm_jobs มีอยู่หรือไม่
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='longterm_jobs'")
            if cursor.fetchone() is None:
                raise Exception("ตาราง longterm_jobs ไม่พบในฐานข้อมูล")

            jobs = conn.execute(
                "SELECT id, status, cycle_day, created_at, done_at, locked, user_open "
                "FROM longterm_jobs WHERE vin_no=? ORDER BY id DESC",
                (vin_key,)
            ).fetchall()
        except Exception as e:
            jobs = []
            error_msg = str(e)
        finally:
            conn.close()

        if not jobs:
            body = f"""
            <div class="card">
              <h2>ประวัติ Longterm Maintenance</h2>
              <p><a class="btn" href="/manage?token={vin_key}">← กลับหน้าจัดการ</a></p>
              <p class="small">{ "ไม่มีประวัติ Longterm สำหรับรถคันนี้" if not error_msg else f"Error: {error_msg}" }</p>
            </div>
            """
            return base_layout("Inventory | Longterm History", body, active_tab="inventory")
        
        trs = []
        for r in jobs:
            status_style = ""
            if r['status'] == 'complete':
                status_style = "style='color:green;'"
            elif r['status'] == 'active':
                status_style = "style='color:orange;'"

            lock_str = "<span class='ng'>🔒</span>" if r['locked'] else ""
            
            trs.append(f"""
            <tr>
                <td>{r['id']}</td>
                <td><b {status_style}>{r['status']}</b> {lock_str}</td>
                <td>{r['cycle_day']}d</td>
                <td class='small'>{r['created_at']}</td>
                <td class='small'>{r['done_at'] or '-'}</td>
                <td class='small'>{r['user_open'] or ''}</td>
            </tr>
            """)
        
        body = f"""
        <div class="card">
            <h2>ประวัติ Longterm Maintenance: {vin_key}</h2>
            <p><a class="btn" href="/manage?token={vin_key}">← กลับหน้าจัดการ</a></p>
            <table>
                <thead>
                    <tr>
                        <th>Job ID</th>
                        <th>Status</th>
                        <th>Cycle</th>
                        <th>Created At</th>
                        <th>Done At</th>
                        <th>User Open</th>
                    </tr>
                </thead>
                <tbody>{''.join(trs)}</tbody>
            </table>
        </div>
        """
        return base_layout(f"Inventory | Longterm History", body, active_tab="inventory")
    # --- ⭐️ END: [NEW] Endpoint ประวัติ Longterm ---

    @app.get("/status/export", response_class=HTMLResponse)
    def status_export(vin: str = Query(...)):
        vin_res, idv = resolve_vin_idvan(vin)
        vin_key = vin_res or vin

        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT id, status, created_at, updated_at, COALESCE(id_van,'') AS id_van "
                "FROM export_jobs WHERE vin_no=? ORDER BY id DESC",
                (vin_key,)
            ).fetchall()
        finally:
            conn.close()

        if not rows:
            body = f"""
            <div class="card">
              <h2>Export Status</h2>
              <p><a class="btn" href="/manage?token={vin_key}">← กลับหน้าจัดการ</a></p>
              <p class="small">ยังไม่มีรายการ Export สำหรับคันนี้</p>
            </div>
            """
            return base_layout("Inventory | Export Status", body, active_tab="inventory")

        trs = []
        for r in rows:
            trs.append(
                f"<tr><td>{r['id']}</td><td>{r['status']}</td><td>{r['created_at'] or ''}</td><td>{r['updated_at'] or ''}</td><td>{r['id_van']}</td></tr>"
            )
        body = f"""
        <div class="card">
          <h2>Export Status</h2>
          <p><a class="btn" href="/manage?token={vin_key}">← กลับหน้าจัดการ</a></p>
          <table style="width:100%;border-collapse:collapse">
            <thead><tr><th>ID</th><th>Status</th><th>Created</th><th>Updated</th><th>ID VAN</th></tr></thead>
            <tbody>{''.join(trs)}</tbody>
          </table>
        </div>
        """
        return base_layout("Inventory | Export Status", body, active_tab="inventory")

    @app.get("/movement", response_class=HTMLResponse)
    def movement(vin: str = Query(...)):
        def _norm_ts(s: str) -> str:
            s = (s or "").strip()
            try:
                return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
            try:
                dt = datetime.strptime(s, "%d-%m-%Y %H:%M:%S")
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
            return s

        def _to_display(s_iso: str) -> str:
            try:
                return datetime.strptime(s_iso, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %H:%M:%S")
            except Exception:
                return s_iso

        all_rows = []
        conn = sqlite3.connect(DB_PATH())
        try:
            conn.row_factory = sqlite3.Row
            try:
                base_rows = conn.execute(
                    "SELECT at,action,from_slot,to_slot,note,source FROM movements WHERE vin_no=?",
                    (vin,)
                ).fetchall()
            except Exception:
                base_rows = []
        finally:
            conn.close()

        for r in base_rows:
            all_rows.append({
                "at_iso": _norm_ts(r["at"]),
                "action": r["action"],
                "from_slot": r["from_slot"] or "",
                "to_slot": r["to_slot"] or "",
                "note": r["note"] or "",
                "source": r["source"] or ""
            })

        try:
            idv = resolve_vin_idvan(vin)[1]
        except Exception:
            idv = ""
        conn_so = sqlite3.connect(STOCKOUT_DB_PATH())
        try:
            conn_so.row_factory = sqlite3.Row
            rows_so = conn_so.execute(
                "SELECT stock_out_at AS at, 'out_yard' AS action, '' AS from_slot, '' AS to_slot, '' AS note, source "
                "FROM stock_outs WHERE vin_no=? OR id_van=?",
                (vin, idv)
            ).fetchall()
            for r in rows_so:
                all_rows.append({
                    "at_iso": _norm_ts(r["at"]),
                    "action": r["action"],
                    "from_slot": "",
                    "to_slot": "",
                    "note": "",
                    "source": r["source"] or ""
                })
            rows_logs = conn_so.execute(
                "SELECT at, 'out_yard' AS action, '' AS from_slot, '' AS to_slot, '' AS note, source "
                "FROM stockout_logs WHERE (vin_no=? OR id_van=?) AND LOWER(COALESCE(action,''))='out_yard'",
                (vin, idv)
            ).fetchall()
            for r in rows_logs:
                all_rows.append({
                    "at_iso": _norm_ts(r["at"]),
                    "action": r["action"],
                    "from_slot": "",
                    "to_slot": "",
                    "note": "",
                    "source": r["source"] or ""
                })
        except Exception:
            pass
        finally:
            conn_so.close()

        all_rows.sort(key=lambda x: x["at_iso"])

        trs = []
        for r in all_rows:
            trs.append(
                f"<tr><td>{_to_display(r['at_iso'])}</td><td>{r['action']}</td>"
                f"<td>{r['from_slot']}</td><td>{r['to_slot']}</td>"
                f"<td class='small'>{r['note']}</td><td class='small'>{r['source']}</td></tr>"
            )
        if not trs:
            trs.append("<tr><td colspan='6' class='small'>ยังไม่มีการเคลื่อนไหว</td></tr>")

        body = f"""
        <div class="card">
          <h2>Movement: {vin}</h2>
          <p><a class="btn" href="/manage?token={vin}">← กลับหน้าจัดการ</a></p>
          <table style="width:100%;border-collapse:collapse">
            <thead><tr><th>เวลา</th><th>Action</th><th>จาก</th><th>ไป</th><th>Note</th><th>Source</th></tr></thead>
            <tbody>{''.join(trs)}</tbody>
          </table>
        </div>
        """
        return base_layout("Inventory | Movement", body, active_tab="inventory")

    # --- ⭐️ START: Endpoints ใหม่สำหรับจัดการข้อมูลเพิ่มเติม ---
    
    def get_current_user(request: Request) -> str:
        """Helper to get username from session, used for logging."""
        try:
            if "session" in request.scope:
                if request.session.get("inv_auth_ok"):
                    # The auth middleware doesn't store username, use a generic one
                    return "inventory_user"
        except Exception:
            pass
        return "unknown" # Fallback

    @app.get("/manage/registration", response_class=HTMLResponse)
    def manage_registration_form(request: Request, token: str = Query(...)):
        vin, idv = resolve_vin_idvan(token)
        if not vin:
            return base_layout("ไม่พบรถ", "<div class='card'>❌ ไม่พบข้อมูล VIN/ID VAN นี้</div>")
        
        info = get_registration_info(vin)
        plate_number = info['plate_number'] if info else ""
        tax_due_date = info['tax_due_date'] if info else ""

        body = f"""
        <div class="card">
            <h2>แก้ไขข้อมูลทะเบียน/ภาษี</h2>
            <p><b>VIN:</b> {vin} | <b>ID VAN:</b> {idv or '-'}</p>
            <form method="post" action="/manage/registration">
                <input type="hidden" name="token" value="{token}">
                <p>
                    <label for="plate_number">เลขทะเบียน:</label><br>
                    <input type="text" id="plate_number" name="plate_number" value="{plate_number}">
                </p>
                <p>
                    <label for="tax_due_date">วันหมดอายุภาษี (DD-MM-YYYY):</label><br>
                    <input type="text" id="tax_due_date" name="tax_due_date" value="{tax_due_date}" placeholder="DD-MM-YYYY">
                </p>
                <button type="submit" class="btn ok">บันทึก</button>
                <a href="/manage?token={token}" class="btn">กลับ</a>
            </form>
        </div>
        """
        return base_layout("แก้ไขทะเบียน", body, active_tab="inventory")

    @app.post("/manage/registration")
    def manage_registration_save(request: Request, token: str = Form(...), plate_number: str = Form(""), tax_due_date: str = Form("")):
        vin, idv = resolve_vin_idvan(token)
        if not vin:
            raise HTTPException(status_code=400, detail="VIN not found")
        
        user = get_current_user(request) # Get user for logging
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        conn = sqlite3.connect(DB_PATH())
        try:
            info_old_row = get_registration_info(vin) # Get old data for logging
            info_old = dict(info_old_row) if info_old_row else {}
            
            conn.execute("""
                INSERT INTO vehicle_registration (vin_no, plate_number, tax_due_date, updated_at, updated_by)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(vin_no) DO UPDATE SET
                    plate_number = excluded.plate_number,
                    tax_due_date = excluded.tax_due_date,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
            """, (vin, plate_number.strip(), tax_due_date.strip(), now, user))
            
            # Log movement
            log_note = f"Plate: {plate_number} | Tax: {tax_due_date}"
            if info_old:
                log_note = f"Plate: {info_old.get('plate_number','')} -> {plate_number} | Tax: {info_old.get('tax_due_date','')} -> {tax_due_date}"
                
            log_movement(conn, vin, action="reg_update", note=log_note, source="inventory_web")
            conn.commit()
        except Exception as e:
            conn.rollback()
            return base_layout("Error", f"<div class='card'>Error: {e}</div>")
        finally:
            conn.close()
            
        return RedirectResponse(url=f"/manage?token={token}", status_code=303)

    @app.get("/manage/vehicle_type", response_class=HTMLResponse)
    def manage_vehicle_type_form(request: Request, token: str = Query(...)):
        vin, idv = resolve_vin_idvan(token)
        if not vin:
            return base_layout("ไม่พบรถ", "<div class='card'>❌ ไม่พบข้อมูล VIN/ID VAN นี้</div>")
        
        info = get_vehicle_type_info(vin)
        type_name = info['type_name'] if info else ""
        
        # Example types, you can expand this
        example_types = ["รถส่วนบุคคล", "รถ Taxi", "รถ Demo", "รถ Fleet"]
        options_html = "".join(f"<option value='{t}' {'selected' if t == type_name else ''}>{t}</option>" for t in example_types)
        
        body = f"""
        <div class="card">
            <h2>แก้ไขประเภทรถ</h2>
            <p><b>VIN:</b> {vin} | <b>ID VAN:</b> {idv or '-'}</p>
            <form method="post" action="/manage/vehicle_type">
                <input type="hidden" name="token" value="{token}">
                <p>
                    <label for="type_name">ประเภทรถ:</label><br>
                    <input list="vehicle_types" id="type_name" name="type_name" value="{type_name}" placeholder="เลือกหรือพิมพ์...">
                    <datalist id="vehicle_types">
                        {options_html}
                    </datalist>
                </p>
                <button type="submit" class="btn ok">บันทึก</button>
                <a href="/manage?token={token}" class="btn">กลับ</a>
            </form>
        </div>
        """
        return base_layout("แก้ไขประเภทรถ", body, active_tab="inventory")

    @app.post("/manage/vehicle_type")
    def manage_vehicle_type_save(request: Request, token: str = Form(...), type_name: str = Form("")):
        vin, idv = resolve_vin_idvan(token)
        if not vin:
            raise HTTPException(status_code=400, detail="VIN not found")
        
        user = get_current_user(request)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        conn = sqlite3.connect(DB_PATH())
        try:
            info_old_row = get_vehicle_type_info(vin)
            info_old = dict(info_old_row) if info_old_row else {}
            
            conn.execute("""
                INSERT INTO vehicle_type (vin_no, type_name, updated_at, updated_by)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(vin_no) DO UPDATE SET
                    type_name = excluded.type_name,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
            """, (vin, type_name.strip(), now, user))
            
            log_note = f"Type: {type_name}"
            if info_old:
                log_note = f"Type: {info_old.get('type_name','')} -> {type_name}"
            
            log_movement(conn, vin, action="type_update", note=log_note, source="inventory_web")
            conn.commit()
        except Exception as e:
            conn.rollback()
            return base_layout("Error", f"<div class='card'>Error: {e}</div>")
        finally:
            conn.close()
            
        return RedirectResponse(url=f"/manage?token={token}", status_code=303)


    @app.get("/manage/delivery_prep", response_class=HTMLResponse)
    def manage_delivery_prep_form(request: Request, token: str = Query(...)):
        vin, idv = resolve_vin_idvan(token)
        if not vin:
            return base_layout("ไม่พบรถ", "<div class='card'>❌ ไม่พบข้อมูล VIN/ID VAN นี้</div>")
        
        info = get_delivery_prep_info(vin)
        # Set defaults if no info
        prep = {
            'paint_side_status': 'N/A', 'paint_plate_status': 'N/A',
            'sticker_status': 'N/A', 'sticker_details': '',
            'taxi_equip_status': 'N/A'
        }
        if info:
            prep.update(dict(info))

        def make_radio(name, value):
            ok_chk = "checked" if value == "OK" else ""
            nok_chk = "checked" if value == "NOK" else ""
            na_chk = "checked" if value == "N/A" or not value else "" # Default to N/A
            return f"""
            <label style="margin-right:8px;"><input type="radio" name="{name}" value="OK" {ok_chk}> OK</label>
            <label style="margin-right:8px;"><input type="radio" name="{name}" value="NOK" {nok_chk}> NOK</label>
            <label><input type="radio" name="{name}" value="N/A" {na_chk}> N/A</label>
            """

        body = f"""
        <div class="card">
            <h2>แก้ไขงานเตรียมการเพื่อส่งมอบ</h2>
            <p><b>VIN:</b> {vin} | <b>ID VAN:</b> {idv or '-'}</p>
            <form method="post" action="/manage/delivery_prep">
                <input type="hidden" name="token" value="{token}">
                
                <p><b>1. พ่นข้าง:</b><br>{make_radio('paint_side_status', prep['paint_side_status'])}</p>
                <p><b>2. พ่นทะเบียน:</b><br>{make_radio('paint_plate_status', prep['paint_plate_status'])}</p>
                <p><b>3. สติกเกอร์:</b><br>{make_radio('sticker_status', prep['sticker_status'])}</p>
                <p>
                    <label for="sticker_details">รายละเอียดสติกเกอร์:</label><br>
                    <input type="text" id="sticker_details" name="sticker_details" value="{prep['sticker_details'] or ''}">
                </p>
                <p><b>4. อุปกรณ์ Taxi:</b><br>{make_radio('taxi_equip_status', prep['taxi_equip_status'])}</p>
                
                <hr style="margin-top:20px;">
                <button type="submit" class="btn ok">บันทึก</button>
                <a href="/manage?token={token}" class="btn">กลับ</a>
            </form>
        </div>
        """
        return base_layout("แก้ไขงานเตรียมส่งมอบ", body, active_tab="inventory")

    @app.post("/manage/delivery_prep")
    def manage_delivery_prep_save(
        request: Request, token: str = Form(...),
        paint_side_status: str = Form("N/A"),
        paint_plate_status: str = Form("N/A"),
        sticker_status: str = Form("N/A"),
        sticker_details: str = Form(""),
        taxi_equip_status: str = Form("N/A")
    ):
        vin, idv = resolve_vin_idvan(token)
        if not vin:
            raise HTTPException(status_code=400, detail="VIN not found")
        
        user = get_current_user(request)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        conn = sqlite3.connect(DB_PATH())
        try:
            conn.execute("""
                INSERT INTO delivery_prep (
                    vin_no, paint_side_status, paint_plate_status, sticker_status, 
                    sticker_details, taxi_equip_status, updated_at, updated_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(vin_no) DO UPDATE SET
                    paint_side_status = excluded.paint_side_status,
                    paint_plate_status = excluded.paint_plate_status,
                    sticker_status = excluded.sticker_status,
                    sticker_details = excluded.sticker_details,
                    taxi_equip_status = excluded.taxi_equip_status,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
            """, (
                vin, paint_side_status, paint_plate_status, sticker_status, 
                sticker_details.strip(), taxi_equip_status, now, user
            ))
            
            log_note = (
                f"Side:{paint_side_status}, Plate:{paint_plate_status}, "
                f"Sticker:{sticker_status} ({sticker_details}), Taxi:{taxi_equip_status}"
            )
            
            log_movement(conn, vin, action="prep_update", note=log_note, source="inventory_web")
            conn.commit()
        except Exception as e:
            conn.rollback()
            return base_layout("Error", f"<div class='card'>Error: {e}</div>")
        finally:
            conn.close()
            
        return RedirectResponse(url=f"/manage?token={token}", status_code=303)
    # --- ⭐️ END: Endpoints ใหม่ ---

    @app.get("/api/search")
    def api_search(q: str):
        items = [{"vin_no": a, "id_van": b, "model": c}
                 for (a, b, c) in candidate_search(q, limit=20)]
        return JSONResponse(items)

    @app.post("/api/update_slot")
    def api_update_slot(vin: str = Form(...), id_van: str = Form(""), slot: str = Form("")):
        if not vin:
            return JSONResponse({"ok": False, "error": "missing vin"}, status_code=400)
        set_vehicle_slot(vin, id_van, slot.strip())
        return JSONResponse({"ok": True})

    @app.post("/api/confirm_stock")
    def api_confirm_stock(vin: str = Form(...), id_van: str = Form("")):
        if not vin:
            return JSONResponse({"ok": False, "error": "missing vin"}, status_code=400)
        confirm_in_stock(vin, id_van)
        return JSONResponse({"ok": True})

    @app.post("/api/unconfirm_stock")
    def api_unconfirm_stock(vin: str = Form(...)):
        if not vin:
            return JSONResponse({"ok": False, "error": "missing vin"}, status_code=400)
        unconfirm_in_stock(vin)
        return JSONResponse({"ok": True})


    # --- ⭐️ START: Endpoints ระบบตรวจนับ (Count System) ---

    @app.get("/count", response_class=HTMLResponse)
    def count_home():
        """หน้าหลักของระบบตรวจนับ"""
        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            active_job = conn.execute("SELECT * FROM inventory_count_jobs WHERE status = 'active' ORDER BY id DESC LIMIT 1").fetchone()
        finally:
            conn.close()

        if active_job:
            body = f"""
            <div class="card">
                <h2>ตรวจนับสต็อก</h2>
                <p style="color:red; font-weight:bold;">มีงานตรวจนับที่กำลัง Active อยู่ (Job ID: {active_job['id']})</p>
                <p>สร้างเมื่อ: {active_job['created_at']}</p>
                <a href="/count/job/{active_job['id']}" class="btn warn">ไปที่งาน (นับต่อ)</a>
                <a href="/count/list/{active_job['id']}" class="btn">ดูรายการที่นับแล้ว</a>
                <a href="/count/log" class="btn">ดูประวัติงานทั้งหมด</a>
            </div>
            """
        else:
            body = f"""
            <div class="card">
                <h2>ตรวจนับสต็อก</h2>
                <p>ไม่มีงานตรวจนับที่กำลัง Active</p>
                <form action="/count/create" method="post" style="display:inline;">
                    <button type="submit" class="btn ok">เปิดระบบตรวจนับ (สร้างงานใหม่)</button>
                </form>
                <a href="/count/log" class="btn">ดูประวัติงานทั้งหมด</a>
            </div>
            """
        return base_layout("ระบบตรวจนับสต็อก", body, active_tab="count")

    @app.post("/count/create")
    def count_create_job():
        """สร้าง Job ตรวจนับใหม่"""
        conn = sqlite3.connect(DB_PATH())
        try:
            # ตรวจสอบอีกครั้งว่ามี active job หรือไม่
            active_job = conn.execute("SELECT id FROM inventory_count_jobs WHERE status = 'active' LIMIT 1").fetchone()
            if active_job:
                return RedirectResponse(url=f"/count/job/{active_job[0]}", status_code=303)
            
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor = conn.execute("INSERT INTO inventory_count_jobs (status, created_at) VALUES ('active', ?)", (now,))
            new_job_id = cursor.lastrowid
            conn.commit()
            return RedirectResponse(url=f"/count/job/{new_job_id}", status_code=303)
        finally:
            conn.close()

    @app.get("/count/job/{job_id}", response_class=HTMLResponse)
    def count_job_page(job_id: int):
        """หน้านี้คือหน้าทำงาน (ค้นหา)"""
        conn = sqlite3.connect(DB_PATH())
        try:
            job = conn.execute("SELECT * FROM inventory_count_jobs WHERE id = ? AND status = 'active'", (job_id,)).fetchone()
            if not job:
                return RedirectResponse(url="/count", status_code=303)
            
            count = conn.execute("SELECT COUNT(*) FROM inventory_count_items WHERE job_id = ?", (job_id,)).fetchone()[0]
        finally:
            conn.close()
            
        # Script ค้นหา (เหมือนหน้าแรก แต่เปลี่ยน URL ปลายทาง)
        script = f"""
        <script>
        (function(){{
          var q = document.getElementById('q');
          var dd = document.getElementById('dd');
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
                    li.onclick = function(){{ location.href='/count/manage/{job_id}?token='+encodeURIComponent(this.getAttribute('data-vin')); }};
                  }});
                }});
            }}, 200);
          }});
        }})();
        </script>
        """
        
        body = f"""
        <div class="card">
          <h2>🔎 ค้นหาเพื่อตรวจนับ (Job ID: {job_id})</h2>
          <p>นับแล้ว: <b>{count}</b> รายการ | <a href="/count/list/{job_id}" class="btn">ดูรายการ/ปิดจ๊อบ</a></p>
          <input id="q" type="text" placeholder="พิมพ์ VIN / ID VAN เพื่อตรวจนับ..." autofocus/>
          <ul id="dd" class="dd" style="display:none"></ul>
          <p class="small">เลือกรายการเพื่อยืนยันการนับและอัปเดต Slot</p>
        </div>
        """ + script
        return base_layout(f"ตรวจนับ Job {job_id}", body, active_tab="count")

    @app.get("/count/manage/{job_id}", response_class=HTMLResponse)
    def count_manage_item(job_id: int, token: str = Query(...)):
        """หน้ายืนยันการนับและใส่ Slot"""
        vin, idv = resolve_vin_idvan(token)
        if not vin:
            return base_layout("ไม่พบรถ", "<div class='card'>❌ ไม่พบข้อมูล VIN/ID VAN นี้</div>", active_tab="count")

        mdl = get_model(vin) or "-"
        col = get_color(vin) or "-"
        current_slot, _ = get_vehicle_slot_status(vin)
        
        conn = sqlite3.connect(DB_PATH())
        try:
            # ดึงข้อมูลที่เคยนับไว้ (ถ้านับซ้ำ)
            counted = conn.execute("SELECT new_slot FROM inventory_count_items WHERE job_id = ? AND vin_no = ?", (job_id, vin)).fetchone()
            counted_slot = counted[0] if counted else None
        finally:
            conn.close()

        body = f"""
        <div class="card">
            <h2>ยืนยันการนับ (Job ID: {job_id})</h2>
            <div class="grid2">
                <div>VIN</div><div><b>{vin}</b></div>
                <div>ID VAN</div><div>{idv or '-'}</div>
                <div>MODEL</div><div>{mdl}</div>
                <div>EXTERIOR</div><div>{col}</div>
                <div>Slot ปัจจุบัน</div><div><b>{current_slot or '-'}</b></div>
                {f"<div>Slot ที่นับ (งานนี้)</div><div><b>{counted_slot}</b></div>" if counted_slot else ""}
            </div>
        </div>
        <div class="card">
            <form method="post" action="/count/save/{job_id}">
                <input type="hidden" name="vin" value="{vin}">
                <input type="hidden" name="id_van" value="{idv}">
                <input type="hidden" name="model" value="{mdl}">
                
                <label for="slot" style="font-weight:bold; font-size: 1.1em;">ใส่ Slot ใหม่ (หรือยืนยัน Slot เดิม):</label><br>
                <input type="text" id="slot" name="slot" value="{counted_slot or current_slot or ''}" style="margin-top:8px;" autofocus>
                <br><br>
                <button type="submit" class="btn ok">ยืนยันการนับ</button>
                <a href="/count/job/{job_id}" class="btn">กลับไปค้นหา</a>
            </form>
        </div>
        """
        return base_layout("ยืนยันการนับ", body, active_tab="count")

    @app.post("/count/save/{job_id}")
    def count_save_item(job_id: int, 
                        vin: str = Form(...), 
                        id_van: str = Form(""), 
                        model: str = Form(""), 
                        slot: str = Form("")):
        """บันทึกรายการที่นับ (ชั่วคราว)"""
        conn = sqlite3.connect(DB_PATH())
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            conn.execute("""
                INSERT INTO inventory_count_items (job_id, vin_no, id_van, model, new_slot, counted_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id, vin_no) DO UPDATE SET
                    new_slot = excluded.new_slot,
                    counted_at = excluded.counted_at
            """, (job_id, vin, id_van, model, slot.strip(), now))
            conn.commit()
        finally:
            conn.close()
        
        # กลับไปหน้าค้นหา
        return RedirectResponse(url=f"/count/job/{job_id}", status_code=303)

    @app.get("/count/list/{job_id}", response_class=HTMLResponse)
    def count_list_items(job_id: int, page: int = Query(1, ge=1)):
        """แสดงรายการที่นับแล้วใน Job นี้"""
        MAX_PAGE = 10
        ITEMS_PER_PAGE = 100
        
        if page > MAX_PAGE:
            page = MAX_PAGE
        
        offset = (page - 1) * ITEMS_PER_PAGE
        
        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            job = conn.execute("SELECT * FROM inventory_count_jobs WHERE id = ?", (job_id,)).fetchone()
            if not job:
                return RedirectResponse(url="/count", status_code=303)
            
            items = conn.execute(
                "SELECT * FROM inventory_count_items WHERE job_id = ? ORDER BY counted_at DESC LIMIT ? OFFSET ?",
                (job_id, ITEMS_PER_PAGE, offset)
            ).fetchall()
            
            total_count = conn.execute("SELECT COUNT(*) FROM inventory_count_items WHERE job_id = ?", (job_id,)).fetchone()[0]
            total_pages = min(MAX_PAGE, ceil(total_count / ITEMS_PER_PAGE)) # จำกัดที่ 10 หน้า
            
        finally:
            conn.close()

        # Render Table
        trs = []
        if not items:
            trs.append("<tr><td colspan='6' class='small'>ยังไม่มีรายการที่นับ</td></tr>")
        else:
            for item in items:
                trs.append(f"""
                <tr>
                    <td>{item['counted_at']}</td>
                    <td>{item['vin_no']}</td>
                    <td>{item['id_van']}</td>
                    <td>{item['model']}</td>
                    <td>{item['new_slot']}</td>
                </tr>
                """)

        # Render Pagination
        pagination_html = "<div class='pagination'>"
        if page > 1:
            pagination_html += f"<a href='/count/list/{job_id}?page={page - 1}'>&laquo; ก่อนหน้า</a>"
        
        pagination_html += f"<span> หน้า <strong>{page}</strong> / {total_pages} (สูงสุด 10 หน้า) </span>"
        
        if page < total_pages:
            pagination_html += f"<a href='/count/list/{job_id}?page={page + 1}'>ต่อไป &raquo;</a>"
        pagination_html += "</div>"
        
        # Render Buttons (เฉพาะถ้า Job ยัง active)
        buttons_html = ""
        if job['status'] == 'active':
            buttons_html = f"""
            <hr>
            <form method="post" action="/count/finalize/{job_id}" 
                  onsubmit="return confirm('คุณกำลังจะปิดจ๊อบและอัปเดตสต็อกจริง!\\n1. รถทั้งหมดจะถูกตั้งค่าเป็น \\'ไม่ยืนยัน\\'.\\n2. รถในรายการนี้ ({total_count} คัน) จะถูกตั้งค่าเป็น \\'ยืนยัน\\' และอัปเดต Slot ใหม่\\nดำเนินการต่อหรือไม่?')"
                  style="display:inline;">
                <button type="submit" class="btn ok">ยืนยันทำรายการ (ปิดจ๊อบ)</button>
            </form>
            <form method="post" action="/count/cancel/{job_id}" 
                  onsubmit="return confirm('คุณต้องการยกเลิก Job นี้หรือไม่? ข้อมูลการนับ {total_count} รายการจะถูกลบทั้งหมด')"
                  style="display:inline;">
                <button type="submit" class="btn ng">ยกเลิกรายการ (ลบจ๊อบ)</button>
            </form>
            <a href="/count/job/{job_id}" class="btn">กลับไปนับต่อ</a>
            """
        else:
            buttons_html = f"<p>Job นี้ {job['status']} แล้วเมื่อ {job['completed_at']}</p><a href='/count' class='btn'>กลับหน้าหลัก</a>"


        body = f"""
        <div class="card">
            <h2>รายการที่นับแล้ว (Job ID: {job_id})</h2>
            <p>สถานะ: <b>{job['status']}</b> | นับแล้วทั้งหมด: <b>{total_count}</b> รายการ</p>
            {buttons_html}
        </div>
        <div class="card">
            {pagination_html}
            <table>
                <thead>
                    <tr>
                        <th>เวลานับ</th>
                        <th>VIN</th>
                        <th>ID VAN</th>
                        <th>Model</th>
                        <th>Slot (ที่นับได้)</th>
                    </tr>
                </thead>
                <tbody>{''.join(trs)}</tbody>
            </table>
            {pagination_html}
        </div>
        """
        return base_layout(f"รายการ Job {job_id}", body, active_tab="count")

    @app.post("/count/cancel/{job_id}")
    def count_cancel_job(job_id: int):
        """ยกเลิก Job และลบรายการชั่วคราว"""
        conn = sqlite3.connect(DB_PATH())
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # 1. ลบรายการ
            conn.execute("DELETE FROM inventory_count_items WHERE job_id = ?", (job_id,))
            # 2. อัปเดตสถานะ Job
            conn.execute("UPDATE inventory_count_jobs SET status = 'cancelled', completed_at = ? WHERE id = ?", (now, job_id))
            conn.commit()
        finally:
            conn.close()
        return RedirectResponse(url="/count", status_code=303)
        
    # --- ⭐️ START: แก้ไขฟังก์ชัน Finalize ---
    @app.post("/count/finalize/{job_id}")
    def count_finalize_job(job_id: int):
        """
        กระบวนการหลัก:
        1. ตั้งค่า inventory ทั้งหมดเป็น in_stock = 0
        2. วนลูปรายการใน inventory_count_items
        3. อัปเดต inventory ที่นับเจอเป็น in_stock = 1
        4. อัปเดต vehicles.slot เป็น new_slot
        5. บันทึก log_movement (ใน stock.db)
        6. [NEW] บันทึก 'in_yard' log ใน stockout.db เพื่อยกเลิกการ Out (ถ้ามี)
        7. ปิด Job
        """
        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        
        # --- NEW: Open connection to stockout.db ---
        conn_stockout = sqlite3.connect(STOCKOUT_DB_PATH())
        
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # --- START TRANSACTION (Main DB) ---
            conn.execute("BEGIN TRANSACTION")
            # --- START TRANSACTION (Stockout DB) ---
            conn_stockout.execute("BEGIN TRANSACTION")
            
            # Step 1: Mark all as unconfirmed
            conn.execute("UPDATE inventory SET in_stock = 0, updated_at = ?", (now,))
            
            # Step 2: Get all items from this job
            items_to_confirm = conn.execute("SELECT vin_no, id_van, new_slot FROM inventory_count_items WHERE job_id = ?", (job_id,)).fetchall()
            
            log_source = f"inventory_web_count(Job{job_id})"
            
            # Step 3, 4, 5, 6: Loop and update
            for item in items_to_confirm:
                vin = item['vin_no']
                idv = item['id_van']
                new_slot = item['new_slot']
                
                # Get old slot for logging
                old_slot_row = conn.execute("SELECT slot FROM vehicles WHERE vin_no = ?", (vin,)).fetchone()
                old_slot = old_slot_row['slot'] if old_slot_row else ""
                
                # Step 3: Update inventory table
                conn.execute("""
                    INSERT INTO inventory(vin_no, id_van, in_stock, updated_at)
                    VALUES(?,?,1,?)
                    ON CONFLICT(vin_no) DO UPDATE SET
                      id_van=excluded.id_van,
                      in_stock=1,
                      updated_at=excluded.updated_at
                """, (vin, idv, now))
                
                # Step 4: Update vehicles table (for slot)
                conn.execute("UPDATE vehicles SET slot = ? WHERE vin_no = ?", (new_slot, vin))
                
                # Step 5: Log movement (in main db)
                log_movement(conn, vin, 
                             action="physical_count", 
                             from_slot=old_slot, 
                             to_slot=new_slot, 
                             note=f"Count Job {job_id}", 
                             source=log_source)
                             
                # --- NEW Step 6: Log 'in_yard' to stockout.db to reverse any 'out_yard' ---
                # This ensures that even if the car was marked 'out', finding it in a physical count
                # brings it back 'in_yard' in the movement log.
                _log_stock_in(conn_stockout, vin, idv, log_source, new_slot)
            
            # Step 7: Mark job as complete
            conn.execute("UPDATE inventory_count_jobs SET status = 'completed', completed_at = ? WHERE id = ?", (now, job_id))
            
            # --- Commit Both Transactions ---
            conn.commit()
            conn_stockout.commit()
            
        except Exception as e:
            # --- Rollback Both Transactions ---
            conn.rollback()
            conn_stockout.rollback()
            print(f"Error finalizing job {job_id}: {e}")
            return HTMLResponse(f"Error: {e}", status_code=500)
        finally:
            conn.close()
            conn_stockout.close()
            
        return RedirectResponse(url=f"/count/list/{job_id}", status_code=303)
    # --- ⭐️ END: แก้ไขฟังก์ชัน Finalize ---

    @app.get("/count/log", response_class=HTMLResponse)
    def count_log_page():
        """ดูประวัติการสร้าง Job ทั้งหมด"""
        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            jobs = conn.execute("SELECT * FROM inventory_count_jobs ORDER BY id DESC LIMIT 100").fetchall()
        finally:
            conn.close()
            
        trs = []
        if not jobs:
            trs.append("<tr><td colspan='5' class='small'>ไม่มีประวัติงาน</td></tr>")
        else:
            for job in jobs:
                trs.append(f"""
                <tr>
                    <td>{job['id']}</td>
                    <td><span class="badge" style="background-color: {'green' if job['status'] == 'completed' else 'red' if job['status'] == 'cancelled' else 'orange'}">{job['status']}</span></td>
                    <td>{job['created_at']}</td>
                    <td>{job['completed_at'] or '-'}</td>
                    <td>
                        <a href="/count/list/{job['id']}" class="btn">ดูรายการ</a>
                    </td>
                </tr>
                """)
                
        body = f"""
        <div class="card">
            <h2>ประวัติงานตรวจนับ (ล่าสุด 100 งาน)</h2>
            <p><a href="/count" class="btn">กลับหน้าหลักตรวจนับ</a></p>
            <table>
                <thead>
                    <tr>
                        <th>Job ID</th>
                        <th>Status</th>
                        <th>Created At</th>
                        <th>Completed/Cancelled At</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody>{''.join(trs)}</tbody>
            </table>
        </div>
        """
        return base_layout("ประวัติงานตรวจนับ", body, active_tab="count")

    # --- ⭐️ END: Endpoints ระบบตรวจนับ (Count System) ---

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    load_config()
    conf = CONFIG.get("inventory_web", {})
    uvicorn.run(app, host=conf.get("host", "0.0.0.0"),
                port=int(conf.get("port", 9111)), log_level="info")