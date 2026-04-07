#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import csv
from datetime import datetime, date
from typing import Tuple, Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware

import os
import json
import sqlite3
import hashlib

CONFIG: dict = {}

OUT_LOCATIONS = [
    "Aion Yard", "Bravo", "Central Rama2", "Emsphere", "Ramintra",
    "Kanchanapisek", "Mahachai", "Minburi", "Pibulsongkram", "Salaya",
    "Sampeng", "Silom", "The Mall Bangkae", "The Mall Bangkapi", "Tip 5",
    "Ubon", "Vibpavadi", "SaTon", "Evme", "บ้านลูกค้า", "EV7",
    "อู่ Taxi เจ้ประคอง", "Fleet ตำรวจ", "อยุธยา", "Com7", "Taxi lineman"
]

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

def load_inv_hashes(path: str = None):
    try:
        if not path:
            path = os.path.join(os.path.dirname(__file__), "INVP.json")
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            return set(data.get("hashes", []))
    except Exception:
        return set()

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def is_logged_in(req: Request) -> bool:
    try:
        return bool(req.session.get("inv_auth"))
    except Exception:
        return False

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

# --- ⭐️ START: ตารางใหม่สำหรับข้อมูลเพิ่มเติม (จาก inventory_web) ---
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

def get_pdi_state(vin_no: str) -> Tuple[str, int]:
    conn = sqlite3.connect(DB_PATH())
    try:
        try:
            r = conn.execute(
                "SELECT status, percent_ok FROM pdi_jobs WHERE vin_no=? ORDER BY id DESC LIMIT 1",
                (vin_no,)
            ).fetchone()
            if not r:
                return ("-", 0)
            status = r[0] or "-"
            try:
                pct = int(r[1] or 0)
            except Exception:
                pct = 0
            return (status, pct)
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
            return (r[0] or "-") if r else "-"
        except Exception:
            return "-"
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

def has_out_yard(vin_no: str, id_van: str = "") -> bool:
    vin_no = (vin_no or "").strip(); id_van = (id_van or "").strip()
    conn = sqlite3.connect(STOCKOUT_DB_PATH())
    try:
        q = "SELECT 1 FROM stockout_logs WHERE {cond} ORDER BY id DESC LIMIT 1"
        try:
            if vin_no:
                r = conn.execute(q.format(cond="vin_no=? AND LOWER(COALESCE(action,'')) LIKE 'out_yard%'"), (vin_no,)).fetchone()
                if r: return True
            if id_van:
                r = conn.execute(q.format(cond="id_van=? AND LOWER(COALESCE(action,'')) LIKE 'out_yard%'"), (id_van,)).fetchone()
                if r: return True
        except Exception:
            pass
        return False
    finally:
        conn.close()

def get_last_out_info(vin_no: str) -> Tuple[str, str]:
    conn = sqlite3.connect(STOCKOUT_DB_PATH())
    try:
        try:
            conn.execute("ALTER TABLE stockout_logs ADD COLUMN location TEXT")
        except Exception:
            pass
        
        r = conn.execute(
            "SELECT location, at FROM stockout_logs "
            "WHERE vin_no=? AND LOWER(COALESCE(action,'')) LIKE 'out_yard%' "
            "ORDER BY id DESC LIMIT 1",
            (vin_no,)
        ).fetchone()
        if r:
            return (r[0] or "N/A", r[1] or "")
        return ("ยังไม่ออก", "")
    except Exception:
        return ("N/A", "")
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
    """Fetches the latest VDCI report pair for a given VIN."""
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
    except Exception:
        return None
    finally:
        conn.close()

# --- ⭐️ START: ฟังก์ชัน Get Info ใหม่ (จาก inventory_web) ---
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


# ---------- NEW: helpers for VDCI status (Yes/No) & Longterm latest cycle ----------
def vdci_exists(vin_no: str) -> bool:
    """Return True if there is at least one vdci_report_pairs record for this VIN."""
    pair = get_latest_vdci_report_pair(vin_no)
    return bool(pair)

def longterm_latest_cycle_label(vin_no: str) -> str:
    """Return 'รอบ X' from MAX(cycle_day) in longterm_jobs where status='complete', else 'ไม่มีข้อมูล'."""
    conn = sqlite3.connect(DB_PATH())
    try:
        row = conn.execute(
            "SELECT MAX(cycle_day) FROM longterm_jobs WHERE vin_no=? AND status='complete'",
            (vin_no,)
        ).fetchone()
        if not row or row[0] is None:
            return "ไม่มีข้อมูล"
        try:
            cyc = int(row[0])
            return f"รอบ {cyc}"
        except Exception:
            return "ไม่มีข้อมูล"
    finally:
        conn.close()

def _parse_ddmmyyyy(s: str):
    s = (s or "").strip()
    try:
        d, m, y = map(int, s.split("-"))
        return date(y, m, d)
    except Exception:
        return None

def _calc_day_yard(stock_in_str: str, is_out: bool) -> str:
    if is_out:
        return ""
    dt = _parse_ddmmyyyy(stock_in_str)
    if not dt:
        return ""
    today = date.today()
    return str((today - dt).days if today >= dt else 0)

def _rein_label(slot: str) -> str:
    return "rein" if (slot or "").strip().lower() == "rein" else "ไม่rein"

def _labels_for_filters(vin_no: str) -> tuple:
    conn = sqlite3.connect(DB_PATH())
    try:
        conn.row_factory = sqlite3.Row
        ensure_vehicle_columns(conn)
        r = conn.execute(
            "SELECT COALESCE(slot,''), COALESCE(id_van,'') FROM vehicles WHERE vin_no=?",
            (vin_no,)
        ).fetchone()
        slot = (r[0] if r else "") or ""
        id_van = (r[1] if r else "") or ""
    finally:
        conn.close()
    
    f1_txt = "มีSlot" if slot else "ไม่มีSlot"

    st_pdi, pct = get_pdi_state(vin_no)
    pct_i = int(pct or 0)
    f2_txt = "PDI100%" if (str(st_pdi).lower() == "complete" or pct_i >= 100) else "ยังไม่PDI"

    st_exp = get_export_state(vin_no)
    f3_txt = "Export100%" if str(st_exp).lower() == "complete" else "ยังไม่Export"

    in_stock = get_inventory_confirmed(vin_no)
    f4_txt = "InStock" if int(in_stock or 0) == 1 else "NotInStock"

    out = has_out_yard(vin_no, id_van)
    f5_txt = "Out_yard" if out else "ยังไม่Out_yard"

    f6_txt = _rein_label(slot)

    location_txt, out_at_txt = get_last_out_info(vin_no) if out else ("ยังไม่ออก", "")
    f7_txt = location_txt
    
    return (f1_txt, f2_txt, f3_txt, f4_txt, f5_txt, f6_txt, f7_txt, out_at_txt)

def _accept_filters(f1_txt, f2_txt, f3_txt, f4_txt, f5_txt, f6_txt, f7_txt,
                    f1_sel, f2_sel, f3_sel, f4_sel, f5_sel, f6_sel, f7_sel) -> bool:
    def ok(label, sel):
        return (sel == "N/A") or (label == sel)
    return (ok(f1_txt, f1_sel) and ok(f2_txt, f2_sel) and ok(f3_txt, f3_sel)
            and ok(f4_txt, f4_sel) and ok(f5_txt, f5_sel) and ok(f6_txt, f6_sel)
            and ok(f7_txt, f7_sel))

def _accept_dayyard(day_yard_str: str, dy_sel: str) -> bool:
    if dy_sel in (None, "", "ทั้งหมด"):
        return True
    try:
        n = int(day_yard_str)
    except Exception:
        return False
    if dy_sel == ">30วัน":
        return n > 30
    if dy_sel == ">60วัน":
        return n > 60
    if dy_sel == ">90วัน":
        return n > 90
    return True

# ⭐️ MODIFIED: เพิ่ม reg_info, type_info, prep_info
def _row_for_export(row_tuple, f1, f2, f3, f4, f5, f6, f7, out_at, battery_info, vdci_info,
                    vdci_has: bool, longterm_text: str, 
                    reg_info, type_info, prep_info):
    vin_no, motor_no, model, ex_col, in_col, stock_in, slot = row_tuple
    day_yard = _calc_day_yard(stock_in, f5 == "Out_yard")
    rein_count = "1" if f6 == "rein" else "0"
    
    volt12_status = battery_info['volt12_status'] if battery_info else ""
    hivol_percent = battery_info['hivol_percent'] if battery_info and battery_info['hivol_percent'] is not None else ""
    check_at = battery_info['check_at'] if battery_info else ""
    
    dtcs_before_str = ""
    dtcs_after_str = ""
    if vdci_info:
        try:
            before_dtcs = json.loads(vdci_info['before_dtc_summary'] or '[]')
            dtcs_before_str = ", ".join([d.get('dtc', '') for d in before_dtcs])
        except Exception:
            dtcs_before_str = "Error parsing"
        
        try:
            after_dtcs = json.loads(vdci_info['after_dtc_summary'] or '[]')
            dtcs_after_str = ", ".join([d.get('dtc', '') for d in after_dtcs])
        except Exception:
            dtcs_after_str = "Error parsing"

    vdci_text = "มี" if vdci_has else "ไม่มี"
    longterm_txt = longterm_text or "ไม่มีข้อมูล"

    # ⭐️ NEW: Parse new info
    plate_number = reg_info['plate_number'] if reg_info else ""
    tax_due_date = reg_info['tax_due_date'] if reg_info else ""
    vehicle_type = type_info['type_name'] if type_info else ""
    
    paint_side = prep_info['paint_side_status'] if prep_info else "N/A"
    paint_plate = prep_info['paint_plate_status'] if prep_info else "N/A"
    sticker_status = prep_info['sticker_status'] if prep_info else "N/A"
    sticker_details = prep_info['sticker_details'] if prep_info else ""
    taxi_equip = prep_info['taxi_equip_status'] if prep_info else "N/A"

    # ⭐️ MODIFIED: Return list
    return [
        vin_no or "",
        motor_no or "",
        model or "",
        ex_col or "",
        in_col or "",
        stock_in or "",
        day_yard,
        rein_count,
        slot or "",
        out_at or "",
        f7,
        volt12_status,
        hivol_percent,
        check_at,
        dtcs_before_str,
        dtcs_after_str,
        f1, f2, f3, f4, f5, f6,
        # ⭐️ NEW COLUMNS ADDED HERE
        plate_number,
        tax_due_date,
        vehicle_type,
        paint_side,
        paint_plate,
        sticker_status,
        sticker_details,
        taxi_equip,
        # ⭐️ END NEW COLUMNS
        vdci_text,
        longterm_txt
    ]

def _query_all_vehicles():
    conn = sqlite3.connect(DB_PATH())
    try:
        conn.row_factory = sqlite3.Row
        ensure_vehicle_columns(conn)
        return conn.execute("""
            SELECT
              COALESCE(vin_no,'') AS vin_no,
              COALESCE(motor_no,'') AS motor_no,
              COALESCE(model,'') AS model,
              COALESCE(exterior_color,'') AS exterior_color,
              COALESCE(interior_color,'') AS interior_color,
              COALESCE(stock_in,'') AS stock_in,
              COALESCE(slot,'') AS slot
            FROM vehicles
            ORDER BY vin_no
        """).fetchall()
    finally:
        conn.close()

def calculate_duration(start_str: str, end_str: Optional[str]) -> str:
    if not end_str: return ""
    try:
        start_dt = datetime.fromisoformat(start_str)
        end_dt = datetime.fromisoformat(end_str)
        delta = end_dt - start_dt
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        return f"{days}d {hours}h {minutes}m"
    except Exception: return ""

def base_layout(title: str, body_html: str) -> HTMLResponse:
    css = """
    <style>
    :root{--bg:#0b0f14;--muted:#94a3b8;--panel:#0f172a;--border:#1f2937;--txt:#d9e1ec;}
    *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--txt);font-family:ui-sans-serif,system-ui}
    header{padding:12px 16px;background:#111827;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}
    h1{margin:0;font-size:16px}
    a.btn,button,select,input{background:#1f2937;color:#e5e7eb;border:1px solid #374151;border-radius:8px;padding:8px 12px;text-decoration:none;cursor:pointer}
    a.btn:hover,button:hover{background:#253041}
    main{padding:16px;max-width:1600px;margin:0 auto}
    .card{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:16px;margin:12px 0}
    .grid{display:grid;grid-template-columns: repeat(4,minmax(0,1fr)); gap:12px}
    .muted{color:var(--muted)}
    table{width:100%;border-collapse:collapse}
    th,td{border:1px solid var(--border);padding:6px;font-size:13px}
    th{position:sticky;top:0;background:#0f172a}
    .row{display:flex;gap:8px;align-items:center}
    .pill{display:inline-block;padding:3px 8px;border:1px solid var(--border);border-radius:999px}
    </style>
    """
    head = f"<head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>{title}</title>{css}</head>"
    header = """
    <header>
      <h1>Report Service</h1>
      <div class="row">
        <a class="btn" href="/">Home</a>
        <a class="btn" href="/export/inventory">Export Inventory</a>
        <a class="btn" href="/export/damage">Export Damage</a>
        <a class="btn" href="/logout">Logout</a>
      </div>
    </header>
    """
    html = f"<!doctype html><html>{head}<body>{header}<main>{body_html}</main></body></html>"
    return HTMLResponse(html)

def create_app() -> FastAPI:
    load_config()
    app = FastAPI(title="Report Service", version="1.3.1")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"]
    )
    secret = CONFIG.get("session_secret", "report-secret-please-change")
    app.add_middleware(SessionMiddleware, secret_key=secret)

    @app.get("/login", response_class=HTMLResponse)
    def login_form():
        body = """
        <div class="card">
          <h3>Login</h3>
          <form method="post" action="/login">
            <input type="password" name="password" placeholder="Password"/>
            <button type="submit">Sign in</button>
          </form>
          <p class="muted">ใช้รหัสผ่านที่ hash ไว้ใน INVP.json</p>
        </div>
        """
        return base_layout("Login", body)

    @app.post("/login")
    def login(request: Request, password: str = Form("")):
        hashes = load_inv_hashes()
        if not password:
            return RedirectResponse("/login", status_code=302)
        if sha256_hex(password) in hashes:
            request.session["inv_auth"] = True
            return RedirectResponse("/", status_code=302)
        return RedirectResponse("/login", status_code=302)

    @app.get("/logout")
    def logout(request: Request):
        try:
            request.session.clear()
        except Exception:
            pass
        return RedirectResponse("/login", status_code=302)

    def _guard(request: Request):
        if not is_logged_in(request):
            return RedirectResponse("/login", status_code=302)
        return None

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request):
        resp = _guard(request)
        if resp:
            return resp
        body = """
        <div class="card">
          <h2>หน้ารายงาน</h2>
          <p>โมดูล Report (เชื่อม DB เดิม)</p>
          <a class="btn" href="/export/inventory">📤 Export Inventory</a>
          <a class="btn" href="/export/damage"> DAMAGE REPORT</a>
        </div>
        """
        return base_layout("Home", body)

    location_options = "".join(f"<option>{loc}</option>" for loc in OUT_LOCATIONS)
    # ⭐️ MODIFIED: FORM (p.muted)
    FORM = f"""
    <div class="card">
      <h3>Export Inventory</h3>
      <form method="post" action="/export/inventory">
        <div class="grid">
          <div>
            <label>Filter1: Slot</label>
            <select name="f1">
              <option>N/A</option>
              <option>มีSlot</option>
              <option>ไม่มีSlot</option>
            </select>
          </div>
          <div>
            <label>Filter2: PDI</label>
            <select name="f2">
              <option>N/A</option>
              <option>PDI100%</option>
              <option>ยังไม่PDI</option>
            </select>
          </div>
          <div>
            <label>Filter3: Export</label>
            <select name="f3">
              <option>N/A</option>
              <option>Export100%</option>
              <option>ยังไม่Export</option>
            </select>
          </div>
          <div>
            <label>Filter4: Inventory</label>
            <select name="f4">
              <option>N/A</option>
              <option>InStock</option>
              <option>NotInStock</option>
            </select>
          </div>
          <div>
            <label>Filter5: Out_yard</label>
            <select name="f5">
              <option>N/A</option>
              <option>Out_yard</option>
              <option>ยังไม่Out_yard</option>
            </select>
          </div>
          <div>
            <label>Filter6: Rein</label>
            <select name="f6">
              <option>N/A</option>
              <option>rein</option>
              <option>ไม่rein</option>
            </select>
          </div>
          <div>
            <label>Filter7: Location</label>
            <select name="f7">
              <option>N/A</option>
              <option>ยังไม่ออก</option>
              {location_options}
            </select>
          </div>
          <div>
            <label>Day yard</label>
            <select name="dy">
              <option>ทั้งหมด</option>
              <option>&gt;30วัน</option>
              <option>&gt;60วัน</option>
              <option>&gt;90วัน</option>
            </select>
          </div>
        </div>
        <div style="margin-top:12px;display:flex;gap:8px">
          <button type="submit">Export</button>
          <button type="submit" formaction="/export/inventory/preview">Preview</button>
          <a class="btn" href="/">ย้อนกลับ</a>
        </div>
        <p class="muted">คอลัมน์: Vin No., Motor No., Model, Color, Stock In, Day yard, Rein, slot, Out At, Location, แบต 12V, แบต Hivol, ตรวจสอบแบตล่าสุด, DTCs Before, DTCs After, Fiter1..Fiter6, เลขทะเบียน, วันหมดภาษี, ประเภทรถ, พ่นข้าง, พ่นทะเบียน, สติกเกอร์, รายละเอียดสติกเกอร์, อุปกรณ์ Taxi, VDCI, Longterm รอบล่าสุด</p>
      </form>
    </div>
    """

    @app.get("/export/inventory", response_class=HTMLResponse)
    def export_form(request: Request):
        resp = _guard(request)
        if resp:
            return resp
        return base_layout("Export Inventory", FORM)

    # ⭐️ MODIFIED: export_preview
    @app.post("/export/inventory/preview", response_class=HTMLResponse)
    def export_preview(request: Request,
                       f1: str = Form("N/A"),
                       f2: str = Form("N/A"),
                       f3: str = Form("N/A"),
                       f4: str = Form("N/A"),
                       f5: str = Form("N/A"),
                       f6: str = Form("N/A"),
                       f7: str = Form("N/A"),
                       dy: str = Form("ทั้งหมด")):
        resp = _guard(request)
        if resp:
            return resp
        rows = _query_all_vehicles()
        out_rows = []
        rein_total = 0
        for r in rows:
            vin = r["vin_no"]
            slot = r["slot"] or ""
            f1_txt, f2_txt, f3_txt, f4_txt, f5_txt, f6_txt, f7_txt, out_at_txt = _labels_for_filters(vin)

            day_yard_tmp = _calc_day_yard(r["stock_in"], f5_txt == "Out_yard")
            if not _accept_filters(f1_txt, f2_txt, f3_txt, f4_txt, f5_txt, f6_txt, f7_txt, f1, f2, f3, f4, f5, f6, f7):
                continue
            if not _accept_dayyard(day_yard_tmp, dy):
                continue

            battery_check = get_latest_battery_check(vin)
            battery_info = {
                'volt12_status': battery_check['volt12_status'],
                'hivol_percent': battery_check['hivol_percent'],
                'check_at': battery_check['check_at']
            } if battery_check else None
            
            vdci_pair = get_latest_vdci_report_pair(vin)
            vdci_has = vdci_exists(vin)
            longterm_txt = longterm_latest_cycle_label(vin)

            # ⭐️ NEW: Get new info
            reg_info = get_registration_info(vin)
            type_info = get_vehicle_type_info(vin)
            prep_info = get_delivery_prep_info(vin)

            row_data = _row_for_export(
                (r["vin_no"], r["motor_no"], r["model"], r["exterior_color"], r["interior_color"], r["stock_in"], slot),
                f1_txt, f2_txt, f3_txt, f4_txt, f5_txt, f6_txt, f7_txt, out_at_txt,
                battery_info, vdci_pair, vdci_has, longterm_txt,
                # ⭐️ NEW PARAMS
                reg_info, type_info, prep_info
            )
            out_rows.append(row_data)
            if row_data[7] == "1":
                rein_total += 1
        
        # ⭐️ MODIFIED: head
        head = ["Vin No.","Motor No.","Model","Ext Color","Int Color","Stock In","Day yard","Rein","slot","Out At","Location", "แบต 12V", "แบต Hivol", "ตรวจสอบแบตล่าสุด", "DTCs Before", "DTCs After", "F1","F2","F3","F4","F5","F6", "เลขทะเบียน", "วันหมดภาษี", "ประเภทรถ", "พ่นข้าง", "พ่นทะเบียน", "สติกเกอร์", "รายละเอียดฯ", "อุปกรณ์ Taxi", "VDCI", "Longterm รอบล่าสุด"]
        thead = "".join(f"<th>{h}</th>" for h in head)
        body_rows = []
        for rr in out_rows:
            tds = "".join(f"<td>{(c if c is not None else '')}</td>" for c in rr)
            body_rows.append(f"<tr>{tds}</tr>")
        table_html = "<table><thead><tr>" + thead + "</tr></thead><tbody>" + "".join(body_rows) + "</tbody></table>"

        top = (
            "<div class='row'>"
            f"<span class='muted'>ผลลัพธ์ทั้งหมด: <b>{len(out_rows):,}</b> รายการ</span>"
            f"<span class='pill'>Rein ทั้งหมด: <b>{rein_total:,}</b></span>"
            "</div>"
        )
        controls = (f"<p><a class='btn' href='/export/inventory'>ปรับฟิลเตอร์</a> "
                    f"<a class='btn' href='/'>หน้าแรก</a></p>")
        html = top + controls + table_html + controls
        return base_layout("Export Inventory | Preview", html)

    # ⭐️ MODIFIED: export_csv
    @app.post("/export/inventory")
    def export_csv(request: Request,
                   f1: str = Form("N/A"),
                   f2: str = Form("N/A"),
                   f3: str = Form("N/A"),
                   f4: str = Form("N/A"),
                   f5: str = Form("N/A"),
                   f6: str = Form("N/A"),
                   f7: str = Form("N/A"),
                   dy: str = Form("ทั้งหมด")):
        resp = _guard(request)
        if resp:
            return resp
        rows = _query_all_vehicles()
        buf = io.StringIO()
        w = csv.writer(buf)
        # ⭐️ MODIFIED: header
        header = ["Vin No.","Motor No.","Model","Exterior Color","Interior Color","Stock In","Day yard","Rein","slot","Out At","Location","แบต 12V","แบต Hivol","ตรวจสอบแบตล่าสุด", "DTCs Before", "DTCs After", "Fiter1","Fiter2","Fiter3","Fiter4","Fiter5","Fiter6", "เลขทะเบียน", "วันหมดภาษี", "ประเภทรถ", "พ่นข้าง", "พ่นทะเบียน", "สติกเกอร์", "รายละเอียดสติกเกอร์", "อุปกรณ์ Taxi", "VDCI", "Longterm รอบล่าสุด"]
        w.writerow(header)

        for r in rows:
            vin = r["vin_no"]
            slot = r["slot"] or ""
            f1_txt, f2_txt, f3_txt, f4_txt, f5_txt, f6_txt, f7_txt, out_at_txt = _labels_for_filters(vin)

            day_yard_tmp = _calc_day_yard(r["stock_in"], f5_txt == "Out_yard")
            if not _accept_filters(f1_txt, f2_txt, f3_txt, f4_txt, f5_txt, f6_txt, f7_txt, f1, f2, f3, f4, f5, f6, f7):
                continue
            if not _accept_dayyard(day_yard_tmp, dy):
                continue
            
            battery_check = get_latest_battery_check(vin)
            battery_info = {
                'volt12_status': battery_check['volt12_status'],
                'hivol_percent': battery_check['hivol_percent'],
                'check_at': battery_check['check_at']
            } if battery_check else None

            vdci_pair = get_latest_vdci_report_pair(vin)
            vdci_has = vdci_exists(vin)
            longterm_txt = longterm_latest_cycle_label(vin)

            # ⭐️ NEW: Get new info
            reg_info = get_registration_info(vin)
            type_info = get_vehicle_type_info(vin)
            prep_info = get_delivery_prep_info(vin)

            w.writerow(_row_for_export(
                (r["vin_no"], r["motor_no"], r["model"], r["exterior_color"], r["interior_color"], r["stock_in"], slot),
                f1_txt, f2_txt, f3_txt, f4_txt, f5_txt, f6_txt, f7_txt, out_at_txt,
                battery_info, vdci_pair, vdci_has, longterm_txt,
                # ⭐️ NEW PARAMS
                reg_info, type_info, prep_info
            ))

        data = buf.getvalue().encode("utf-8-sig")
        buf.close()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"export_inventory_{ts}.csv"
        return StreamingResponse(
            io.BytesIO(data),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'}
        )

    @app.get("/export/damage", response_class=HTMLResponse)
    def damage_report_form(request: Request):
        resp = _guard(request)
        if resp: return resp
        
        today = date.today().strftime("%Y-%m-%d")
        body = f"""
        <div class="card">
            <h3>Export Damage Reports</h3>
            <form method="post" action="/export/damage">
                <label>Start Date: <input type="date" name="start_date" value="{today}"></label>
                <label>End Date: <input type="date" name="end_date" value="{today}"></label>
                <button type="submit">Export CSV</button>
            </form>
        </div>
        """
        return base_layout("Damage Report", body)

    @app.post("/export/damage")
    def export_damage_csv(request: Request, start_date: str = Form(...), end_date: str = Form(...)):
        resp = _guard(request)
        if resp: return resp

        start_dt = f"{start_date} 00:00:00"
        end_dt = f"{end_date} 23:59:59"

        conn = sqlite3.connect(DB_PATH())
        conn.row_factory = sqlite3.Row
        try:
            reports = conn.execute(
                "SELECT * FROM damage_reports WHERE created_at BETWEEN ? AND ? ORDER BY created_at",
                (start_dt, end_dt)
            ).fetchall()
        finally:
            conn.close()

        buf = io.StringIO()
        w = csv.writer(buf)
        header = ["Report ID", "VIN", "ID VAN", "Status", "Description", "Created At", "Completed At", "Duration", "File 1", "File 2"]
        w.writerow(header)

        for r in reports:
            w.writerow([
                r["id"],
                r["vin_no"],
                r["id_van"],
                r["status"],
                r["description"],
                r["created_at"],
                r["completed_at"],
                calculate_duration(r["created_at"], r["completed_at"]),
                r["file_path1"],
                r["file_path2"],
            ])

        data = buf.getvalue().encode("utf-8-sig")
        buf.close()
        filename = f"damage_report_{start_date}_to_{end_date}.csv"
        return StreamingResponse(
            io.BytesIO(data),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'}
        )
        
    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    load_config()
    conf = CONFIG.get("report_web", {})
    host = conf.get("host", "0.0.0.0")
    port = int(conf.get("port", 9114))
    uvicorn.run(app, host=host, port=port, reload=bool(conf.get("reload", False)))