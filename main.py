#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import re
import sqlite3
import hashlib
from datetime import datetime, date, timedelta
from typing import Dict, Tuple, List

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler,
)

import schedule
import time
import threading
import logging

_lock_file_handle = None
def _acquire_singleton_lock(lock_path: str = "bot_instance.lock") -> bool:
    global _lock_file_handle
    try:
        if os.name == "nt":
            import msvcrt
            _lock_file_handle = open(lock_path, "a+")
            msvcrt.locking(_lock_file_handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            _lock_file_handle = open(lock_path, "a+")
            fcntl.flock(_lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except Exception:
        return False

def _release_singleton_lock():
    global _lock_file_handle
    try:
        if _lock_file_handle:
            try:
                if os.name == "nt":
                    import msvcrt
                    _lock_file_handle.seek(0)
                    msvcrt.locking(_lock_file_handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(_lock_file_handle.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            _lock_file_handle.close()
    except Exception:
        pass

async def _on_error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        err = context.error
    except Exception:
        err = "unknown"
    logging.exception("Telegram handler error: %s", err)

REQUIRED_HEADERS = [
    "ID VAN.", "Vin No.", "Motor No.", "Model",
    "Exterior Color", "Interior Color", "Stock In", "Ref On"
]

IDVAN_PREFIX = "VAN"
IDVAN_PAD = 6

EXPORT_DIR = "exports"
UPLOADS_DIR = "uploads"
os.makedirs(EXPORT_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)

OUT_LOCATIONS = [
    "Aion Yard", "Bravo", "Central Rama2", "Emsphere", "Ramintra",
    "Kanchanapisek", "Mahachai", "Minburi", "Pibulsongkram", "Salaya",
    "Sampeng", "Silom", "The Mall Bangkae", "The Mall Bangkapi", "Tip 5",
    "Ubon", "Vibpavadi", "SaTon", "Evme", "บ้านลูกค้า", "EV7",
    "อู่ Taxi เจ้ประคอง", "Fleet ตำรวจ", "อยุธยา", "Com7", "Taxi lineman"
]


def _norm(h: str) -> str:
    return re.sub(r'[.\s_]+', '', str(h)).lower()

HEADER_ALIASES = {
    "idvan": "ID VAN.", "idvan.": "ID VAN.",
    "vinno": "Vin No.",
    "motorno": "Motor No.",
    "model": "Model",
    "exteriorcolor": "Exterior Color",
    "interiorcolor": "Interior Color",
    "stockin": "Stock In",
    "refon": "Ref On",
}

CONFIG = {}

def _excel_serial_to_date_str(n: float) -> str:
    try:
        base = datetime(1899, 12, 30)
        dt = base + timedelta(days=float(n))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

def _parse_thai_month_date(s: str) -> str:
    months_th = {
        "ม.ค.":1, "มกราคม":1, "มค":1, "ม.ค":1,
        "ก.พ.":2, "กุมภาพันธ์":2, "กพ":2, "ก.พ":2,
        "มี.ค.":3, "มีนาคม":3, "มีค":3, "มี.ค":3,
        "เม.ย.":4, "เมษายน":4, "เมย":4, "เม.ย":4,
        "พ.ค.":5, "พฤษภาคม":5, "พค":5, "พ.ค":5,
        "มิ.ย.":6, "มิถุนายน":6, "มิย":6, "มิ.ย":6,
        "ก.ค.":7, "กรกฎาคม":7, "กค":7, "ก.ค":7,
        "ส.ค.":8, "สิงหาคม":8, "สค":8, "ส.ค":8,
        "ก.ย.":9, "กันยายน":9, "กย":9, "ก.ย":9,
        "ต.ค.":10, "ตุลาคม":10, "ตค":10, "ต.ค":10,
        "พ.ย.":11, "พฤศจิกายน":11, "พย":11, "พ.ย":11,
        "ธ.ค.":12, "ธันวาคม":12, "ธค":12, "ธ.ค":12,
    }
    s2 = re.sub(r"\s+", " ", str(s).replace("\u00a0"," ").strip())
    m = re.fullmatch(r"(\d{1,2})\s*([^\s]+)\s*(\d{4})", s2)
    if not m:
        return ""
    d = int(m.group(1)); mon_token = m.group(2); y = int(m.group(3))
    mon_clean = mon_token.strip().strip(".")
    month = months_th.get(mon_token) or months_th.get(mon_clean) or months_th.get(mon_clean + ".")
    if not month: return ""
    if y > 2400: y -= 543
    try: return datetime(y, month, d).strftime("%Y-%m-%d")
    except ValueError: return ""

def _parse_english_month_date(s: str) -> str:
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try: return datetime.strptime(s.strip(), fmt).strftime("%Y-%m-%d")
        except Exception: pass
    return ""

def thai_or_std_to_iso(value) -> str:
    if value is None: return ""
    if isinstance(value, (datetime, date)):
        return datetime(value.year, value.month, value.day).strftime("%Y-%m-%d")
    if isinstance(value, (int, float)):
        return _excel_serial_to_date_str(value)
    s = str(value).replace("\u00a0"," ").strip()
    if not s: return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s): return s
    m_dash = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4})", s)
    if m_dash:
        d, mo, y = map(int, m_dash.groups())
        try: return datetime(y, mo, d).strftime("%Y-%m-%d")
        except ValueError: return ""
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        d, mo, y = map(int, m.groups())
        if y > 2400: y -= 543
        try: return datetime(y, mo, d).strftime("%Y-%m-%d")
        except ValueError: return ""
    th = _parse_thai_month_date(s)
    if th: return th
    en = _parse_english_month_date(s)
    if en: return en
    try: return datetime.fromisoformat(s).strftime("%Y-%m-%d")
    except Exception: return ""

def iso_to_ddmmyyyy(iso_str: str) -> str:
    if not iso_str: return ""
    try:
        y, m, d = map(int, iso_str.split("-"))
        return f"{d:02d}-{m:02d}-{y:04d}"
    except Exception: return ""

def gs_retry(fn, tries: int = 3, delay_sec: float = 1.5):
    last_err = None
    for _ in range(tries):
        try:
            return fn()
        except Exception as e:
            last_err = e
            time.sleep(delay_sec)
    if last_err:
        raise last_err

def load_config():
    global CONFIG
    with open("config.json", "r", encoding="utf-8") as f:
        CONFIG = json.load(f)

def get_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(CONFIG["credentials_path"], scopes=scopes)

    def _open():
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(CONFIG["google_sheet_url"])
        wsname = CONFIG.get("worksheet_name")
        if wsname:
            try:
                return sh.worksheet(wsname)
            except Exception:
                return sh.sheet1
        return sh.sheet1

    return gs_retry(_open)

def ensure_db(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vehicles (
            id_van TEXT,
            vin_no TEXT,
            motor_no TEXT,
            model TEXT,
            exterior_color TEXT,
            interior_color TEXT,
            stock_in TEXT,
            ref_on TEXT,
            vin_id INTEGER,
            CONSTRAINT uq_vin UNIQUE (vin_no)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS import_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin_no TEXT,
            id_van TEXT,
            action TEXT,
            at TIMESTAMP,
            model TEXT,
            exterior_color TEXT,
            interior_color TEXT,
            stock_in TEXT
        )
    """)
    try: conn.execute("ALTER TABLE vehicles ADD COLUMN vin_id INTEGER")
    except sqlite3.OperationalError: pass
    try: conn.execute("ALTER TABLE vehicles ADD COLUMN id_van TEXT")
    except sqlite3.OperationalError: pass
    try: conn.execute("ALTER TABLE vehicles ADD COLUMN slot TEXT")
    except sqlite3.OperationalError: pass
    try: conn.execute("ALTER TABLE vehicles ADD COLUMN status TEXT")
    except sqlite3.OperationalError: pass
    conn.commit()

def ensure_movements(conn: sqlite3.Connection):
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

def log_movement(conn: sqlite3.Connection, vin_no: str, action: str,
                 from_slot: str = "", to_slot: str = "", note: str = "",
                 source: str = "inventory_cmd"):
    ensure_movements(conn)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO movements(vin_no,at,action,from_slot,to_slot,note,source) VALUES(?,?,?,?,?,?,?)",
        (vin_no, now, action, from_slot, to_slot, note, source)
    )
    conn.commit()

def ensure_inventory(conn: sqlite3.Connection):
    conn.execute("""CREATE TABLE IF NOT EXISTS inventory(
        vin_no TEXT PRIMARY KEY,
        id_van TEXT,
        in_stock INTEGER NOT NULL DEFAULT 1,
        updated_at TEXT
    )""")
    conn.commit()

def confirm_in_stock(conn: sqlite3.Connection, vin_no: str, id_van: str):
    ensure_inventory(conn)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT INTO inventory(vin_no,id_van,in_stock,updated_at)
        VALUES(?,?,1,?)
        ON CONFLICT(vin_no) DO UPDATE SET
          id_van=excluded.id_van,
          in_stock=1,
          updated_at=excluded.updated_at
    """, (vin_no, id_van, now))
    conn.commit()

def _rename_and_align_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for col in df.columns:
        key = _norm(col)
        if key in HEADER_ALIASES:
            rename_map[col] = HEADER_ALIASES[key]
        elif col in REQUIRED_HEADERS:
            rename_map[col] = col
    df2 = df.rename(columns=rename_map)
    for req in REQUIRED_HEADERS:
        if req not in df2.columns:
            df2[req] = ""
    return df2[REQUIRED_HEADERS]

def _next_vin_id(conn: sqlite3.Connection) -> int:
    cur = conn.execute("SELECT MAX(COALESCE(vin_id, 0)) FROM vehicles")
    row = cur.fetchone()
    return (row[0] or 0) + 1

def _idvan_from_vinid(vin_id: int) -> str:
    return f"{IDVAN_PREFIX}{vin_id:0{IDVAN_PAD}d}"

def upsert_sqlite_and_return_info(df: pd.DataFrame) -> Tuple[Dict[str, str], List[dict]]:
    conn = sqlite3.connect(CONFIG["db_path"])
    vin_to_idvan = {}
    actions: List[dict] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        ensure_db(conn)
        for _, row in df.iterrows():
            vin = str(row["Vin No."] or "").strip()
            if not vin:
                continue
            cur = conn.execute("SELECT vin_id, id_van FROM vehicles WHERE vin_no = ?", (vin,))
            existing = cur.fetchone()
            action = "update" if existing else "insert"
            if existing is None:
                vin_id = _next_vin_id(conn)
                id_van = str(row["ID VAN."] or "").strip() or _idvan_from_vinid(vin_id)
            else:
                vin_id, id_van_db = existing
                id_van = (str(row["ID VAN."] or "").strip()) or (id_van_db or _idvan_from_vinid(vin_id))
            stock_iso = thai_or_std_to_iso(row["Stock In"])
            stock_ddmmyyyy = iso_to_ddmmyyyy(stock_iso)
            conn.execute("""
                INSERT INTO vehicles (id_van, vin_no, motor_no, model, exterior_color, interior_color, stock_in, ref_on, vin_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(vin_no) DO UPDATE SET
                    id_van = excluded.id_van,
                    motor_no = excluded.motor_no,
                    model = excluded.model,
                    exterior_color = excluded.exterior_color,
                    interior_color = excluded.interrior_color,
                    stock_in = excluded.stock_in,
                    ref_on = excluded.ref_on
            """.replace("interrior","interior"), (
                id_van,
                vin,
                str(row["Motor No."] or "").strip(),
                str(row["Model"] or "").strip(),
                str(row["Exterior Color"] or "").strip(),
                str(row["Interior Color"] or "").strip(),
                stock_ddmmyyyy,
                str(row["Ref On"] or "").strip(),
                vin_id
            ))
            conn.execute("""
                INSERT INTO import_logs (vin_no, id_van, action, at, model, exterior_color, interior_color, stock_in)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (vin, id_van, action, now,
                  str(row["Model"] or "").strip(),
                  str(row["Exterior Color"] or "").strip(),
                  str(row["Interior Color"] or "").strip(),
                  stock_ddmmyyyy))
            vin_to_idvan[vin] = id_van
            actions.append({
                "VIN": vin, "ID VAN.": id_van, "Action": action, "When": now,
                "Model": str(row["Model"] or "").strip(),
                "Exterior Color": str(row["Exterior Color"] or "").strip(),
                "Interior Color": str(row["Interior Color"] or "").strip(),
                "Stock In": stock_ddmmyyyy
            })
        conn.commit()
    finally:
        conn.close()
    return vin_to_idvan, actions

def read_sheet_as_df(ws) -> pd.DataFrame:
    values = gs_retry(lambda: ws.get_all_values())
    if not values:
        return pd.DataFrame(columns=REQUIRED_HEADERS)
    headers = values[0]
    data_rows = values[1:]
    df = pd.DataFrame(data_rows, columns=headers)
    return _rename_and_align_columns(df)

def upsert_sheet(ws, df_new: pd.DataFrame) -> dict:
    df_new = _rename_and_align_columns(df_new)
    gs_retry(lambda: ws.update(values=[REQUIRED_HEADERS], range_name='A1'))
    df_sheet = read_sheet_as_df(ws)
    vin_to_row = {}
    for idx, row in df_sheet.iterrows():
        vin = str(row.get("Vin No.", "")).strip()
        if vin:
            vin_to_row[vin] = idx + 2
    inserted = 0; updated = 0
    updates = []; inserts = []
    for _, r in df_new.iterrows():
        vin = str(r["Vin No."] or "").strip()
        if not vin: continue
        val = str(r["Stock In"] or "").strip()
        if re.fullmatch(r"\d{1,2}-\d{1,2}-\d{4}", val):
            stock_ddmmyyyy = val
        else:
            stock_ddmmyyyy = iso_to_ddmmyyyy(thai_or_std_to_iso(val))
        row_values = [
            str(r["ID VAN."] or "").strip(), vin,
            str(r["Motor No."] or "").strip(),
            str(r["Model"] or "").strip(),
            str(r["Exterior Color"] or "").strip(),
            str(r["Interior Color"] or "").strip(),
            stock_ddmmyyyy, str(r["Ref On"] or "").strip(),
        ]
        if vin in vin_to_row:
            sr = vin_to_row[vin]
            updates.append((f"A{sr}:H{sr}", [row_values])); updated += 1
        else:
            inserts.append(row_values); inserted += 1
    if updates:
        gs_retry(lambda: ws.batch_update([{"range": r, "values": v} for r, v in updates]))
    if inserts:
        gs_retry(lambda: ws.append_rows(inserts))
    try:
        gs_retry(lambda: ws.format('G2:G', {'numberFormat': {'type': 'DATE', 'pattern': 'dd-mm-yyyy'}}))
    except Exception:
        pass
    return {"inserted": inserted, "updated": updated}

def df_from_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, engine="openpyxl")
    df = _rename_and_align_columns(df)
    df["Stock In"] = df["Stock In"].apply(lambda v: iso_to_ddmmyyyy(thai_or_std_to_iso(v)))
    return df

def calc_hash_of_df(df: pd.DataFrame) -> str:
    s = df[REQUIRED_HEADERS].to_csv(index=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def load_snapshot() -> str:
    if os.path.exists("sheet_snapshot.json"):
        try:
            with open("sheet_snapshot.json", "r", encoding="utf-8") as f:
                return json.load(f).get("hash", "")
        except Exception:
            return ""
    return ""

def save_snapshot(h: str):
    with open("sheet_snapshot.json", "w", encoding="utf-8") as f:
        json.dump({"hash": h, "saved_at": datetime.now().isoformat()}, f, ensure_ascii=False)

def ensure_stockout_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_outs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin_no TEXT,
            id_van TEXT,
            stock_out_at TEXT,
            source TEXT,
            raw TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stockout_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin_no TEXT,
            id_van TEXT,
            action TEXT,
            at TEXT,
            source TEXT
        )
    """)
    try:
        conn.execute("ALTER TABLE stockout_logs ADD COLUMN location TEXT")
    except sqlite3.OperationalError:
        pass
    conn.commit()


def _resolve_vin_and_idvan(val: str) -> tuple:
    vv = (val or "").strip()
    if not vv: return ("","")
    is_idvan = bool(re.fullmatch(r"[A-Za-z]+[0-9]+", vv))
    vconn = sqlite3.connect(CONFIG["db_path"])
    try:
        if is_idvan:
            cur = vconn.execute("SELECT vin_no, id_van FROM vehicles WHERE id_van = ?", (vv,))
            r = cur.fetchone()
            if r: return (r[0], r[1])
            return ("", vv)
        else:
            cur = vconn.execute("SELECT vin_no, id_van FROM vehicles WHERE vin_no = ?", (vv,))
            r = cur.fetchone()
            if r: return (r[0], r[1])
            return (vv, "")
    finally:
        vconn.close()

def _insert_stockout_record(vin_or_idvan: str, when_dt: datetime, source: str, location: str = None) -> dict:
    stockout_db = CONFIG.get("stockout_db_path", "stockout.db")
    sconn = sqlite3.connect(stockout_db)
    try:
        ensure_stockout_db(sconn)
        vin_no, id_van = _resolve_vin_and_idvan(vin_or_idvan)
        stamp = when_dt.strftime("%d-%m-%Y %H:%M:%S")
        payload = {
            "input": vin_or_idvan,
            "resolved_vin": vin_no,
            "resolved_idvan": id_van,
            "at": stamp,
            "source": source,
            "location": location,
        }
        sconn.execute(
            "INSERT INTO stock_outs (vin_no, id_van, stock_out_at, source, raw) VALUES (?, ?, ?, ?, ?)",
            (vin_no, id_van, stamp, source, json.dumps(payload, ensure_ascii=False))
        )
        sconn.execute(
            "INSERT INTO stockout_logs (vin_no, id_van, action, at, source, location) VALUES (?, ?, 'out_yard', ?, ?, ?)",
            (vin_no, id_van, stamp, source, location)
        )
        sconn.commit()
        return {"vin_no": vin_no, "id_van": id_van, "at": stamp, "source": source, "location": location}
    finally:
        sconn.close()


def _process_stockout_excel(path: str) -> tuple:
    df = pd.read_excel(path, engine="openpyxl")
    cols = [str(c).strip() for c in df.columns.tolist()]
    col_vin = None
    for cand in ["Vin No.","VIN","vin","ID VAN.","ID VAN"]:
        if cand in cols:
            col_vin = cand; break
    if not col_vin:
        raise ValueError("ไม่พบคอลัมน์ Vin No. หรือ ID VAN. ในไฟล์")

    col_loc = None
    for cand_loc in ["Location", "location", "สถานที่"]:
        if cand_loc in cols:
            col_loc = cand_loc; break

    while len(df.columns) < 3:
        df[f"col_{len(df.columns)+1}"] = ""

    upload_now = datetime.now()

    actions = []
    for idx, row in df.iterrows():
        date_only_val = df.iloc[idx, 2]
        date_obj = _parse_date_only_to_date(date_only_val)
        when_dt = datetime(date_obj.year, date_obj.month, date_obj.day,
                           upload_now.hour, upload_now.minute, upload_now.second)
        stamp_str = when_dt.strftime("%d-%m-%Y %H:%M:%S")
        df.iloc[idx, 2] = stamp_str

        val = "" if pd.isna(row[col_vin]) else str(row[col_vin]).strip()
        if not val:
            continue

        location = None
        if col_loc and not pd.isna(row[col_loc]):
            location = str(row[col_loc]).strip()

        rec = _insert_stockout_record(val, when_dt, source="excel", location=location)
        actions.append(rec)

    stamped_path = os.path.join(UPLOADS_DIR, f"stockout_stamped_{upload_now.strftime('%Y%m%d_%H%M%S')}.xlsx")
    with pd.ExcelWriter(stamped_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="StockOut")
    return actions, stamped_path

def _parse_date_only_to_date(val):
    from datetime import date as _date, datetime as _dt, date as _d
    if val is None:
        return _date.today()
    if isinstance(val, (_dt, _d)):
        return _date(val.year, val.month, val.day)
    if isinstance(val, (int, float)):
        try:
            base = _dt(1899, 12, 30)
            dt = base + timedelta(days=float(val))
            return _date(dt.year, dt.month, dt.day)
        except Exception:
            return _date.today()
    s = str(val).replace("\u00a0"," ").strip()
    if not s:
        return _date.today()
    m = re.fullmatch(r"(\d{1,2})-(\d{1,2})-(\d{4})", s)
    if m:
        d, mo, y = map(int, m.groups())
        try:
            return _date(y, mo, d)
        except Exception:
            return _date.today()
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        d, mo, y = map(int, m.groups())
        if y > 2400: y -= 543
        try:
            return _date(y, mo, d)
        except Exception:
            return _date.today()
    iso = thai_or_std_to_iso(s)
    if iso:
        try:
            y, mo, d = map(int, iso.split("-"))
            return _date(y, mo, d)
        except Exception:
            pass
    try:
        dt = _dt.fromisoformat(s)
        return _date(dt.year, dt.month, dt.day)
    except Exception:
        return _date.today()

def ensure_pdi_core(conn: sqlite3.Connection):
    conn.execute("""CREATE TABLE IF NOT EXISTS pdi_steps(
        step_code TEXT PRIMARY KEY, step_name TEXT NOT NULL, seq INTEGER NOT NULL
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS pdi_jobs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vin_no TEXT NOT NULL, id_van TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        percent_ok INTEGER NOT NULL DEFAULT 0,
        created_at TEXT, updated_at TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS pdi_results(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL, step_code TEXT NOT NULL,
        status TEXT NOT NULL, note TEXT, at TEXT,
        UNIQUE(job_id, step_code)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS pdi_locks(
        vin_no TEXT PRIMARY KEY,
        job_id INTEGER,
        is_locked INTEGER NOT NULL DEFAULT 1,
        from_slot TEXT,
        to_slot TEXT,
        locked_at TEXT,
        unlocked_at TEXT
    )""")
    c = conn.execute("SELECT COUNT(*) FROM pdi_steps").fetchone()[0] or 0
    if c == 0:
        conn.executemany("INSERT INTO pdi_steps(step_code,step_name,seq) VALUES(?,?,?)", [
            ("FILL","เติมน้ำ/เติมลม/เช็คแบ็ต",1),
            ("VDCI","VDCI",2),
            ("BODY","ลอกลายตัวถัง",3),
        ])
    conn.commit()

def ensure_export_core(conn: sqlite3.Connection):
    conn.execute("""CREATE TABLE IF NOT EXISTS export_jobs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vin_no TEXT NOT NULL,
        id_van TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT,
        updated_at TEXT
    )""")
    conn.commit()

def _get_or_create_pdi_job(conn: sqlite3.Connection, vin_no: str, id_van: str) -> int:
    ensure_pdi_core(conn)
    ensure_export_core(conn)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    r = conn.execute("SELECT id FROM pdi_jobs WHERE vin_no=? ORDER BY id DESC LIMIT 1", (vin_no,)).fetchone()
    if r:
        job_id = r[0]
        conn.execute("UPDATE pdi_jobs SET updated_at=? WHERE id=?", (now, job_id))
    else:
        conn.execute("""INSERT INTO pdi_jobs(vin_no,id_van,status,percent_ok,created_at,updated_at)
                        VALUES(?,?,?,?,?,?)""", (vin_no, id_van, 'pending', 0, now, now))
        job_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    return job_id

def is_pdi_locked(vin_no: str) -> bool:
    conn = sqlite3.connect(CONFIG["db_path"])
    try:
        ensure_pdi_core(conn)
        ensure_export_core(conn)
        r = conn.execute("SELECT is_locked FROM pdi_locks WHERE vin_no=?", (vin_no,)).fetchone()
        return bool(r and r[0] == 1)
    finally:
        conn.close()

def is_damage_locked(vin_no: str) -> bool:
    conn = sqlite3.connect(CONFIG["db_path"])
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
    finally:
        conn.close()

# --- START: NEW FUNCTION TO CHECK LONGTERM LOCK ---
def is_longterm_locked(vin_no: str) -> bool:
    conn = sqlite3.connect(CONFIG["db_path"])
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='longterm_jobs'")
        if cursor.fetchone() is None:
            return False 

        r = conn.execute(
            "SELECT 1 FROM longterm_jobs WHERE vin_no = ? AND status = 'active' AND locked = 1",
            (vin_no,)
        ).fetchone()
        return bool(r)
    except Exception as e:
        print(f"Error checking longterm lock: {e}")
        return False
    finally:
        conn.close()
# --- END: NEW FUNCTION ---

async def pdmo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("ใช้: /PDMO <VIN หรือ IDVAN>")
        return
    token = context.args[0].strip()
    conn = sqlite3.connect(CONFIG["db_path"])
    try:
        vin, idvan = _resolve_vin_and_idvan(token)
        if not vin:
            await update.message.reply_text("ไม่พบรถในระบบ")
            return
        ensure_pdi_core(conn)
        ensure_export_core(conn)
        r = conn.execute("SELECT is_locked FROM pdi_locks WHERE vin_no=?", (vin,)).fetchone()
        if r and r[0] == 1:
            await update.message.reply_text(f"คันนี้ถูกส่งเข้าคิว PDI และล็อคอยู่แล้ว (VIN {vin})")
            return
        job_id = _get_or_create_pdi_job(conn, vin, idvan)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""INSERT INTO pdi_locks(vin_no,job_id,is_locked,locked_at)
                        VALUES(?,?,1,?)
                        ON CONFLICT(vin_no) DO UPDATE SET
                          job_id=excluded.job_id, is_locked=1, locked_at=excluded.locked_at, unlocked_at=NULL""",
                     (vin, job_id, now))
        conn.commit()
        await update.message.reply_text(
            f"✅ ส่งรถเข้าคิว PDI และล็อคสต็อกแล้ว\nVIN: {vin} | ID VAN: {idvan}\n"
            f"ไปทำขั้นตอนผ่านบอท/เว็บ PDI ได้เลย"
        )
    finally:
        conn.close()

async def pdmi_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("ใช้: /PDMI <VIN หรือ IDVAN> <SLOT/ช่อง>")
        return
    token = context.args[0].strip()
    slot  = " ".join(context.args[1:]).strip()
    conn = sqlite3.connect(CONFIG["db_path"])
    try:
        vin, idvan = _resolve_vin_and_idvan(token)
        if not vin:
            await update.message.reply_text("ไม่พบรถในระบบ")
            return
        ensure_pdi_core(conn)
        ensure_export_core(conn)
        j = conn.execute("""SELECT id, percent_ok, status
                            FROM pdi_jobs WHERE vin_no=? ORDER BY id DESC LIMIT 1""", (vin,)).fetchone()
        if not j:
            await update.message.reply_text("ยังไม่มีงาน PDI สำหรับคันนี้ (ใช้ /PDMO ก่อน)")
            return
        job_id, pct, st = j
        if pct < 100 or st != 'complete':
            await update.message.reply_text(f"PDI ยังไม่ครบ 100% (สถานะ {st}, {pct}%)")
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""UPDATE pdi_locks
                        SET is_locked=0, to_slot=?, unlocked_at=?
                        WHERE vin_no=?""", (slot, now, vin))
        conn.commit()
        try:
            conn.execute("ALTER TABLE vehicles ADD COLUMN slot TEXT")
        except Exception:
            pass
        conn.execute("UPDATE vehicles SET slot=? WHERE vin_no=?", (slot, vin))
        conn.commit()
        await update.message.reply_text(
            f"✅ นำรถกลับเข้าสต็อกและปลดล็อคแล้ว\nVIN: {vin} | เข้าช่อง: {slot}\nสามารถ Out ได้ตามปกติ"
        )
    finally:
        conn.close()

def ensure_views():
    conn = sqlite3.connect(CONFIG["db_path"])
    try:
        ensure_db(conn)
        ensure_pdi_core(conn)
        ensure_export_core(conn)
        conn.commit()
    finally:
        conn.close()
    stockout_db = CONFIG.get("stockout_db_path", "stockout.db")
    sconn = sqlite3.connect(stockout_db)
    try:
        ensure_stockout_db(sconn)
    finally:
        sconn.close()

def _attach_stockout(conn: sqlite3.Connection):
    stockout_db = CONFIG.get("stockout_db_path", "stockout.db")
    safe_path = stockout_db.replace("'", "''")
    conn.execute(f"ATTACH DATABASE '{safe_path}' AS sdb")

def get_inventory_summary():
    base_db = CONFIG["db_path"]
    conn = sqlite3.connect(base_db)
    try:
        _attach_stockout(conn)
        total = conn.execute("""
        WITH import_m AS (
          SELECT vin_no AS vin_resolved, at AS at_iso, 'in' AS action
          FROM import_logs
          WHERE COALESCE(TRIM(vin_no),'') <> ''
        ),
        stockout_raw AS (
          SELECT
            substr(at,7,4)||'-'||substr(at,4,2)||'-'||substr(at,1,2) || ' ' || substr(at,12) AS at_iso,
            vin_no, id_van
          FROM sdb.stockout_logs
        ),
        stockout_m AS (
          SELECT
            COALESCE(NULLIF(vin_no,''), (
              SELECT v.vin_no FROM vehicles v WHERE v.id_van = sor.id_van LIMIT 1
            )) AS vin_resolved,
            at_iso,
            'out' AS action
          FROM stockout_raw sor
        ),
        movements AS (
          SELECT * FROM import_m
          UNION ALL
          SELECT * FROM stockout_m
        ),
        last_move AS (
          SELECT v.vin_no,
                 (
                   SELECT m2.action
                   FROM movements m2
                   WHERE m2.vin_resolved = v.vin_no
                   ORDER BY datetime(m2.at_iso) DESC
                   LIMIT 1
                 ) AS last_action
          FROM vehicles v
          GROUP BY v.vin_no
        )
        SELECT COUNT(*)
        FROM last_move
        WHERE COALESCE(last_action, 'in') <> 'out'
        """).fetchone()[0] or 0

        top_models = conn.execute("""
        WITH import_m AS (
          SELECT vin_no AS vin_resolved, at AS at_iso, 'in' AS action
          FROM import_logs
          WHERE COALESCE(TRIM(vin_no),'') <> ''
        ),
        stockout_raw AS (
          SELECT
            substr(at,7,4)||'-'||substr(at,4,2)||'-'||substr(at,1,2) || ' ' || substr(at,12) AS at_iso,
            vin_no, id_van
          FROM sdb.stockout_logs
        ),
        stockout_m AS (
          SELECT
            COALESCE(NULLIF(vin_no,''), (
              SELECT v.vin_no FROM vehicles v WHERE v.id_van = sor.id_van LIMIT 1
            )) AS vin_resolved,
            at_iso,
            'out' AS action
          FROM stockout_raw sor
        ),
        movements AS (
          SELECT * FROM import_m
          UNION ALL
          SELECT * FROM stockout_m
        ),
        last_move AS (
          SELECT v.vin_no, v.model,
                 (
                   SELECT m2.action
                   FROM movements m2
                   WHERE m2.vin_resolved = v.vin_no
                   ORDER BY datetime(m2.at_iso) DESC
                   LIMIT 1
                 ) AS last_action
          FROM vehicles v
          GROUP BY v.vin_no
        )
        SELECT model, COUNT(*) AS c
        FROM last_move
        WHERE COALESCE(last_action, 'in') <> 'out'
        GROUP BY model
        ORDER BY c DESC
        LIMIT 5
        """).fetchall()

        details_all = conn.execute("""
        WITH import_m AS (
          SELECT vin_no AS vin_resolved, at AS at_iso, 'in' AS action
          FROM import_logs
          WHERE COALESCE(TRIM(vin_no),'') <> ''
        ),
        stockout_raw AS (
          SELECT
            substr(at,7,4)||'-'||substr(at,4,2)||'-'||substr(at,1,2) || ' ' || substr(at,12) AS at_iso,
            vin_no, id_van
          FROM sdb.stockout_logs
        ),
        stockout_m AS (
          SELECT
            COALESCE(NULLIF(vin_no,''), (
              SELECT v.vin_no FROM vehicles v WHERE v.id_van = sor.id_van LIMIT 1
            )) AS vin_resolved,
            at_iso,
            'out' AS action
          FROM stockout_raw sor
        ),
        movements AS (
          SELECT * FROM import_m
          UNION ALL
          SELECT * FROM stockout_m
        ),
        last_move AS (
          SELECT v.vin_no,
                 (
                   SELECT m2.action
                   FROM movements m2
                   WHERE m2.vin_resolved = v.vin_no
                   ORDER BY datetime(m2.at_iso) DESC
                   LIMIT 1
                 ) AS last_action
          FROM vehicles v
          GROUP BY v.vin_no
        )
        SELECT v.vin_no, v.id_van, v.model, v.exterior_color, v.interior_color, v.stock_in, v.ref_on
        FROM vehicles v
        JOIN last_move lm ON lm.vin_no = v.vin_no
        WHERE COALESCE(lm.last_action, 'in') <> 'out'
        ORDER BY v.model, v.vin_no
        """).fetchall()

        conn.execute("DETACH DATABASE sdb")
        return total, top_models, details_all
    finally:
        conn.close()

def _df_from_rows(rows, columns):
    return pd.DataFrame(rows, columns=columns)

def export_inv_excel(total, top_models, details_all) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(EXPORT_DIR, f"inv_{ts}.xlsx")

    df_summary = pd.DataFrame([{"InStockTotal": total}])
    df_model = _df_from_rows(top_models, ["Model", "Count"])
    df_details = _df_from_rows(details_all, [
        "VIN", "ID VAN.", "Model", "Exterior Color",
        "Interior Color", "Stock In (DD-MM-YYYY)", "Ref On"
    ])

    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df_summary.to_excel(w, index=False, sheet_name="Summary")
        df_model.to_excel(w, index=False, sheet_name="ByModel")
        df_details.to_excel(w, index=False, sheet_name="Details")
    return out_path

def get_out_today():
    stockout_db = CONFIG.get("stockout_db_path", "stockout.db")
    conn = sqlite3.connect(stockout_db)
    conn.row_factory = sqlite3.Row
    try:
        ensure_stockout_db(conn)
        rows = conn.execute("""
        SELECT vin_no, id_van, at as stock_out_at, location
        FROM stockout_logs
        WHERE at LIKE strftime('%d-%m-%Y', 'now', 'localtime') || '%'
        AND lower(action) = 'out_yard'
        ORDER BY datetime(substr(at,7,4)||'-'||substr(at,4,2)||'-'||substr(at,1,2) || ' ' || substr(at,12)) DESC
        """).fetchall()
        return [tuple(row) for row in rows]
    finally:
        conn.close()


def export_otoday_excel(rows_today) -> str:
    ts_day = datetime.now().strftime("%Y%m%d")
    out_path = os.path.join(EXPORT_DIR, f"otoday_{ts_day}.xlsx")

    df_details = _df_from_rows(rows_today, ["VIN", "ID VAN.", "StockOutAt (DD-MM-YYYY HH:MM:SS)", "Location"])

    vin_to_model = {}
    if len(rows_today):
        conn = sqlite3.connect(CONFIG["db_path"])
        try:
            for vin, _, _, _ in rows_today:
                if vin and vin not in vin_to_model:
                    r = conn.execute("SELECT model FROM vehicles WHERE vin_no = ?", (vin,)).fetchone()
                    vin_to_model[vin] = (r[0] if r else None)
        finally:
            conn.close()
    df_details["Model"] = df_details["VIN"].map(vin_to_model)

    df_summary = pd.DataFrame([{"OutTodayTotal": len(rows_today)}])
    df_bymodel = (
        df_details.groupby("Model").size().reset_index(name="Count").sort_values("Count", ascending=False)
        if not df_details.empty else pd.DataFrame(columns=["Model","Count"])
    )
    df_bylocation = (
        df_details.groupby("Location").size().reset_index(name="Count").sort_values("Count", ascending=False)
        if not df_details.empty else pd.DataFrame(columns=["Location","Count"])
    )


    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df_summary.to_excel(w, index=False, sheet_name="Summary")
        df_bymodel.to_excel(w, index=False, sheet_name="ByModel")
        df_bylocation.to_excel(w, index=False, sheet_name="ByLocation")
        df_details.to_excel(w, index=False, sheet_name="Details")
    return out_path

def get_out_range(start_ddmmyyyy: str, end_ddmmyyyy: str):
    def to_iso(d):
        dd,mm,yy = d.split('-')
        return f"{yy}-{mm}-{dd}"

    stockout_db = CONFIG.get("stockout_db_path", "stockout.db")
    conn = sqlite3.connect(stockout_db)
    conn.row_factory = sqlite3.Row
    try:
        ensure_stockout_db(conn)
        rows = conn.execute("""
        WITH norm AS (
          SELECT vin_no, id_van, at as stock_out_at, location,
                 substr(at,7,4)||'-'||substr(at,4,2)||'-'||substr(at,1,2) || ' ' || substr(at,12) AS out_iso
          FROM stockout_logs
          WHERE lower(action) = 'out_yard'
        )
        SELECT vin_no, id_van, stock_out_at, location
        FROM norm
        WHERE date(out_iso) BETWEEN date(?) AND date(?)
        ORDER BY out_iso DESC
        """, (to_iso(start_ddmmyyyy), to_iso(end_ddmmyyyy))).fetchall()
        return [tuple(row) for row in rows]
    finally:
        conn.close()

def export_oto_excel(rows_range, start_ddmmyyyy: str, end_ddmmyyyy: str) -> str:
    def compact_iso(d):
        dd, mm, yy = d.split('-')
        return f"{yy}{mm}{dd}"

    out_path = os.path.join(EXPORT_DIR, f"oto_{compact_iso(start_ddmmyyyy)}_{compact_iso(end_ddmmyyyy)}.xlsx")

    df_details = _df_from_rows(rows_range, ["VIN", "ID VAN.", "StockOutAt (DD-MM-YYYY HH:MM:SS)", "Location"])

    vin_to_model = {}
    if len(rows_range):
        conn = sqlite3.connect(CONFIG["db_path"])
        try:
            for vin, _, _, _ in rows_range:
                if vin and vin not in vin_to_model:
                    r = conn.execute("SELECT model FROM vehicles WHERE vin_no = ?", (vin,)).fetchone()
                    vin_to_model[vin] = (r[0] if r else None)
        finally:
            conn.close()
    df_details["Model"] = df_details["VIN"].map(vin_to_model)

    df_summary = pd.DataFrame([{
        "OutTotal": len(rows_range),
        "StartDate": start_ddmmyyyy,
        "EndDate": end_ddmmyyyy
    }])
    df_bymodel = (
        df_details.groupby("Model").size().reset_index(name="Count").sort_values("Count", ascending=False)
        if not df_details.empty else pd.DataFrame(columns=["Model","Count"])
    )
    df_bylocation = (
        df_details.groupby("Location").size().reset_index(name="Count").sort_values("Count", ascending=False)
        if not df_details.empty else pd.DataFrame(columns=["Location","Count"])
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df_summary.to_excel(w, index=False, sheet_name="Summary")
        df_bymodel.to_excel(w, index=False, sheet_name="ByModel")
        df_bylocation.to_excel(w, index=False, sheet_name="ByLocation")
        df_details.to_excel(w, index=False, sheet_name="Details")
    return out_path

async def s_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("พิมพ์เลข 4 หรือ 5 หลักท้ายของ VIN เช่น /s 05264")
        return
    suffix = args[0].strip()
    if not re.fullmatch(r"\d{4,5}", suffix):
        await update.message.reply_text("โปรดระบุตัวเลข 4 หรือ 5 หลักเท่านั้น เช่น /s 05264")
        return
    rows = _search_vin_suffix(suffix)
    if not rows:
        await update.message.reply_text(f"ไม่พบ VIN ที่ลงท้ายด้วย {suffix}")
        return
    lines = []
    for vin_no, model, exterior_color, id_van in rows:
        lines.append(f"• {vin_no} | {model} | {exterior_color} | {id_van}")
    header = f"ผลการค้นหา (VIN ลงท้ายด้วย {suffix}):\nVIN เต็ม | MODEL | ภายนอก | ID VAN"
    msg = header + "\n" + "\n".join(lines[:50])
    if len(rows) > 50:
        msg += f"\n… และอีก {len(rows)-50} รายการ (แสดงสูงสุด 50)"
    await update.message.reply_text(msg)

async def out_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(
            "ใช้คำสั่ง:\n"
            "1. `/out <VIN หรือ ID VAN>` -> เพื่อแสดงปุ่มเลือกสถานที่\n"
            "2. `/out <VIN หรือ ID VAN> <สถานที่>` -> เพื่อบันทึกทันที"
        )
        return

    token = args[0].strip()
    vin_no, _idvan = _resolve_vin_and_idvan(token)

    if not vin_no and not _idvan:
        await update.message.reply_text(f"❌ ไม่พบรถ `{token}` ในระบบ")
        return

    if vin_no:
        if is_pdi_locked(vin_no):
            await update.message.reply_text(f"คันนี้ (`{vin_no}`) ถูกล็อคโดยระบบ PDI — ต้องใช้ /PDMI เพื่อปลดล็อคก่อน")
            return
        if is_damage_locked(vin_no):
            await update.message.reply_text(f"คันนี้ (`{vin_no}`) 🔒 **ถูกล็อคในระบบแจ้งซ่อม** — ต้องแก้ไขและปลดล็อคในเว็บ PDI ก่อน")
            return
        # --- START: MODIFIED SECTION ---
        if is_longterm_locked(vin_no):
            await update.message.reply_text(f"คันนี้ (`{vin_no}`) 🔒 **ถูกล็อคโดยระบบ Longterm (ติด NG)** — ต้องปลดล็อคในเว็บ Longterm ก่อน")
            return
        # --- END: MODIFIED SECTION ---

    if len(args) > 1:
        location = " ".join(args[1:]).strip()
        when = datetime.now()
        rec = _insert_stockout_record(token, when, source="cmd", location=location)
        vin_show = rec.get("vin_no") or "-"
        idvan_show = rec.get("id_van") or "-"
        loc_show = rec.get("location") or "-"
        await update.message.reply_text(
            f"✅ Stock Out สำเร็จ\n"
            f"VIN: {vin_show}\n"
            f"ID VAN.: {idvan_show}\n"
            f"เวลา: {rec['at']}\n"
            f"➡️ ปลายทาง: {loc_show}"
        )
    else:
        keyboard = [
            [InlineKeyboardButton(loc, callback_data=f"out_idx:{idx}:{token}")]
            for idx, loc in enumerate(OUT_LOCATIONS)
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(f"กรุณาเลือกปลายทางสำหรับ: *{token}*", reply_markup=reply_markup, parse_mode="Markdown")

async def location_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        _, idx_str, token = query.data.split(":", 2)
        idx = int(idx_str)
        location = OUT_LOCATIONS[idx]
    except (ValueError, IndexError):
        await query.edit_message_text(text="เกิดข้อผิดพลาด: ไม่สามารถประมวลผลข้อมูลปุ่มได้")
        return

    when = datetime.now()
    rec = _insert_stockout_record(token, when, source="cmd", location=location)

    vin_show = rec.get("vin_no") or "-"
    idvan_show = rec.get("id_van") or "-"
    loc_show = rec.get("location") or "-"

    await query.edit_message_text(
        text=f"✅ Stock Out สำเร็จ\n"
             f"VIN: `{vin_show}`\n"
             f"ID VAN.: `{idvan_show}`\n"
             f"เวลา: {rec['at']}\n"
             f"➡️ ปลายทาง: *{loc_show}*",
        parse_mode="Markdown"
    )

async def rein_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("ใช้: /rein <VIN หรือ IDVAN>")
        return

    token = context.args[0].strip()
    db = CONFIG["db_path"]
    conn = sqlite3.connect(db)
    try:
        ensure_db(conn)
        vin, idvan = _resolve_vin_and_idvan(token)
        if not vin and not idvan:
            await update.message.reply_text("❌ ไม่พบรถในระบบ (ตรวจ VIN/IDVAN)")
            return

        row = conn.execute(
            "SELECT COALESCE(slot,'') FROM vehicles WHERE vin_no=? OR id_van=? LIMIT 1",
            (vin or token, idvan or token)
        ).fetchone()
        old_slot = (row[0] if row else "") or ""

        today_ddmmyyyy = datetime.now().strftime("%d-%m-%Y")
        conn.execute(
            "UPDATE vehicles SET stock_in=?, slot='rein' WHERE vin_no=? OR id_van=?",
            (today_ddmmyyyy, vin or token, idvan or token)
        )
        conn.commit()

        log_movement(conn, vin or token, action="stock_ins",
                     from_slot=old_slot, to_slot="rein",
                     note="rein cmd", source="inventory_cmd")

        confirm_in_stock(conn, vin or token, idvan or "")

        def _write_in_yard_after_rein(vin_no: str, source: str = "cmd"):
            sdb = CONFIG.get("stockout_db_path", "stockout.db")
            _conn = sqlite3.connect(sdb)
            try:
                ensure_stockout_db(_conn)
                _at = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                _conn.execute(
                    "INSERT INTO stockout_logs(vin_no, at, action, source) VALUES(?,?,?,?)",
                    (vin_no, _at, "in_yard", source)
                )
                _conn.commit()
            finally:
                _conn.close()

        _write_in_yard_after_rein(vin or token, source="cmd")

        await update.message.reply_text(
            f"✅ Stock-in (รอบ 2+) สำเร็จ\n"
            f"VIN: {vin or '-'}\nID VAN: {idvan or '-'}\n"
            f"Stock In (ใหม่): {today_ddmmyyyy}\nSlot: rein\n"
            f"บันทึก Movement + in_yard log แล้ว"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error (/rein): {e}")
    finally:
        conn.close()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "ส่งไฟล์ .xlsx ได้เลยครับ\n"
        "- Export CF: /EXPCF <VIN หรือ IDVAN> (ยืนยันพร้อมส่งออก + ปลดล็อค, ไม่ Out อัตโนมัติ)\n"
        "- วันที่: แปลงเป็น DD-MM-YYYY (รองรับไทย/พ.ศ./อังกฤษ/serial/datetime)\n"
        "- VIN ซ้ำ: อัปเดต | VIN ใหม่: gen vin_id ต่อเนื่อง และ ID VAN. = VAN000001 …\n"
        "- Logs ครบ และสรุปผลเป็น Excel ส่งกลับในแชท\n"
        "- ค้นหา VIN ท้าย 4-5 หลัก: /s <เลขท้าย> เช่น /s 05264\n"
        "- **บันทึก Stock Out**: /out <VIN/ID VAN> [สถานที่]\n"
        "- **Stock-in รอบ 2+**: /rein <VIN หรือ IDVAN> (stamp วันที่ใหม่, slot=rein, log movement)\n"
        "- รายงาน: /inv (คงเหลือ → ส่ง Excel), /otoday (ออกวันนี้), /oto <DD-MM-YYYY> <DD-MM-YYYY>\n"
        "- PDI Flow: /PDMO <VIN/IDVAN> (ล็อค + ส่งเข้าคิว PDI), /PDMI <VIN/IDVAN> <SLOT> (ปลดล็อคเมื่อ PDI 100%)\n"
        "- เช็คสถานะรถ: /STATUS <VIN หรือ IDVAN>\n"
        "หากส่งไฟล์ชื่อมีคำว่า 'stockout' ระบบจะบันทึก Stock Out จากไฟล์และประทับเวลาในคอลัมน์ C ให้เอง"
    )

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if not doc.file_name.lower().endswith(".xlsx"):
        await update.message.reply_text("กรุณาส่งไฟล์ .xlsx เท่านั้น")
        return
    file = await context.bot.get_file(doc.file_id)
    local_path = os.path.join(UPLOADS_DIR, doc.file_name)
    await file.download_to_drive(local_path)

    fname = (doc.file_name or "").lower()
    if "stockout" in fname:
        try:
            actions, stamped_path = _process_stockout_excel(local_path)
        except Exception as e:
            await update.message.reply_text(f"นำเข้า Stock Out ล้มเหลว: {e}")
            return
        await update.message.reply_text(f"บันทึก Stock Out จำนวน {len(actions)} รายการ เวลาอัปโหลด {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
        try:
            await context.bot.send_document(chat_id=update.effective_chat.id, document=open(stamped_path, "rb"), filename=os.path.basename(stamped_path))
        except Exception as ex:
            await update.message.reply_text(f"ส่งไฟล์ที่ประทับเวลาไม่สำเร็จ: {ex}")
        return

    try:
        df = df_from_excel(local_path)
    except Exception as e:
        await update.message.reply_text(f"ไฟล์ไม่ผ่านเงื่อนไข: {e}")
        return

    vin_to_idvan, actions = upsert_sqlite_and_return_info(df)
    if vin_to_idvan:
        df["ID VAN."] = df["Vin No."].apply(lambda v: vin_to_idvan.get(str(v).strip(), str(df.get("ID VAN.", ""))))

    ws = get_sheet()
    result = upsert_sheet(ws, df)

    report_path = os.path.join(UPLOADS_DIR, f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
    build_summary_excel(actions, report_path)

    total_insert = sum(1 for a in actions if a["Action"] == "insert")
    total_update = sum(1 for a in actions if a["Action"] == "update")
    await update.message.reply_text(
        f"อัปเดตสำเร็จ\nเพิ่มใหม่: {result['inserted']} แถว\nปรับปรุง: {result['updated']} แถว\n"
        f"(Log: insert {total_insert}, update {total_update})\n"
        f"กำลังส่งสรุปเป็นไฟล์ Excel ให้ครับ"
    )
    try:
        await context.bot.send_document(chat_id=update.effective_chat.id, document=open(report_path, "rb"), filename=os.path.basename(report_path))
    except Exception as ex:
        await update.message.reply_text(f"ส่งไฟล์สรุปไม่สำเร็จ: {ex}")

async def expcf_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("ใช้: /EXPCF <VIN หรือ IDVAN>")
        return
    token = context.args[0].strip()
    conn = sqlite3.connect(CONFIG["db_path"])
    try:
        vin, idvan = _resolve_vin_and_idvan(token)
        if not vin:
            await update.message.reply_text("ไม่พบรถในระบบ")
            return
        ensure_export_core(conn)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO export_jobs(vin_no,id_van,status,created_at,updated_at) VALUES(?,?,?,?,?)",
            (vin, idvan, 'complete', now, now)
        )
        conn.execute("UPDATE pdi_locks SET is_locked=0, unlocked_at=? WHERE vin_no=?", (now, vin))
        conn.commit()
        await update.message.reply_text(
            f"✅ Export CF แล้ว (complete) และปลดล็อคสำเร็จ\nVIN: {vin} | ID VAN: {idvan or '-'}\n"
            f"หมายเหตุ: ยังไม่บันทึก OUT อัตโนมัติ — ใช้คำสั่ง /out ได้เลยเมื่อพร้อม"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error (EXPCF): {e}")
    finally:
        conn.close()

async def inv_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    total, top_models, details_all = get_inventory_summary()
    path = export_inv_excel(total, top_models, details_all)
    try:
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=open(path, "rb"),
            filename=os.path.basename(path),
            caption="Export Inventory (Excel)"
        )
    except Exception as ex:
        await update.message.reply_text(f"ส่งไฟล์ไม่สำเร็จ: {ex}")

async def otoday_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_out_today()
    lines = [f"🚚 รถออกวันนี้ ({datetime.now().strftime('%d-%m-%Y')})",
             f"ทั้งหมด: {len(rows):,} คัน\n",
             "รายการ (สูงสุด 20):"]
    for vin, idvan, at, loc in rows[:20]:
        hhmm = at.split(" ")[1] if " " in at else at
        lines.append(f"• {hhmm} | {vin or '-'} | {idvan or '-'} | {loc or '-'}")
    await update.message.reply_text("\n".join(lines))

    path = export_otoday_excel(rows)
    try:
        await context.bot.send_document(chat_id=update.effective_chat.id,
                                        document=open(path, "rb"),
                                        filename=os.path.basename(path),
                                        caption="Export Out Today (Excel)")
    except Exception as ex:
        await update.message.reply_text(f"ส่งไฟล์ไม่สำเร็จ: {ex}")

async def oto_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) != 2:
        await update.message.reply_text("ใช้คำสั่ง: /oto <DD-MM-YYYY> <DD-MM-YYYY>\nตัวอย่าง: /oto 01-10-2025 03-10-2025")
        return
    start_d, end_d = args
    if not re.fullmatch(r"\d{2}-\d{2}-\d{4}", start_d) or not re.fullmatch(r"\d{2}-\d{2}-\d{4}", end_d):
        await update.message.reply_text("รูปแบบวันที่ไม่ถูกต้อง (ต้องเป็น DD-MM-YYYY)")
        return

    rows = get_out_range(start_d, end_d)
    lines = [f"🗓️ รถออกช่วง {start_d} ถึง {end_d}",
             f"ทั้งหมด: {len(rows):,} คัน\n",
             "รายการ (สูงสุด 50):"]
    for vin, idvan, at, loc in rows[:50]:
        lines.append(f"• {at} | {vin or '-'} | {idvan or '-'} | {loc or '-'}")
    await update.message.reply_text("\n".join(lines))

    path = export_oto_excel(rows, start_d, end_d)
    try:
        await context.bot.send_document(chat_id=update.effective_chat.id,
                                        document=open(path, "rb"),
                                        filename=os.path.basename(path),
                                        caption="Export Out by Range (Excel)")
    except Exception as ex:
        await update.message.reply_text(f"ส่งไฟล์ไม่สำเร็จ: {ex}")

def build_summary_excel(actions: List[dict], out_path: str):
    if not actions:
        cols = ["VIN","ID VAN.","Action","When","Stock In","Model","Exterior Color","Interior Color"]
        pd.DataFrame(columns=cols).to_excel(out_path, index=False, sheet_name="Changes")
        return
    df_changes = pd.DataFrame(actions, columns=["VIN","ID VAN.","Action","When","Stock In","Model","Exterior Color","Interior Color"])
    group = df_changes.groupby(["Model","Exterior Color"]).size().reset_index(name="Count")
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_changes.to_excel(writer, index=False, sheet_name="Changes")
        group.sort_values(["Model","Exterior Color"]).to_excel(writer, index=False, sheet_name="ModelColor")

def check_changes_and_notify(app):
    try:
        ws = get_sheet()
        df = read_sheet_as_df(ws)
        h = calc_hash_of_df(df)
        last = load_snapshot()
        if h != last:
            save_snapshot(h)
            text = f"Sheet มีการเปลี่ยนแปลง (รวม {len(df)} แถว)"
            chat_id = CONFIG.get("notify_chat_id")
            if chat_id:
                app.create_task(app.bot.send_message(chat_id=chat_id, text=text))
    except Exception as ex:
        print("Polling error:", ex)

def scheduler_thread(app, interval_min: int):
    schedule.every(interval_min).minutes.do(check_changes_and_notify, app=app)
    while True:
        schedule.run_pending()
        time.sleep(1)

def _search_vin_suffix(suffix: str) -> list:
    sfx = suffix.strip().upper()
    if not re.fullmatch(r"\d{4,5}", sfx):
        return []
    conn = sqlite3.connect(CONFIG["db_path"])
    try:
        cur = conn.execute(
            "SELECT vin_no, model, exterior_color, id_van "
            "FROM vehicles WHERE UPPER(vin_no) LIKE ? ORDER BY vin_no LIMIT 100",
            (f"%{sfx}",)
        )
        return cur.fetchall()
    finally:
        conn.close()

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("ใช้: /STATUS <VIN หรือ IDVAN>")
        return

    token = context.args[0].strip()
    conn = sqlite3.connect(CONFIG["db_path"])
    conn.row_factory = sqlite3.Row
    try:
        try:
            vin_no, id_van = _resolve_vin_and_idvan(token)
        except Exception:
            row = conn.execute("SELECT vin_no,id_van FROM vehicles WHERE vin_no=? OR id_van=? LIMIT 1",
                               (token, token)).fetchone()
            vin_no, id_van = (row["vin_no"], row["id_van"]) if row else ("","")

        if not vin_no and not id_van:
            await update.message.reply_text("❌ ไม่พบรถในระบบ (ตรวจ VIN/IDVAN)")
            return

        cols = {r[1] for r in conn.execute("PRAGMA table_info(vehicles)").fetchall()}
        slot, v_status = "-", "-"
        try:
            row = conn.execute(
                "SELECT "
                + ("COALESCE(slot,'') AS slot," if "slot" in cols else "'' AS slot,")
                + ("COALESCE(status,'') AS vstatus " if "status" in cols else "'' AS vstatus ")
                + "FROM vehicles WHERE vin_no=? OR id_van=? LIMIT 1",
                (vin_no or token, id_van or token)
            ).fetchone()
            if row:
                slot = row["slot"] or "-"
                v_status = row["vstatus"] or "-"
        except Exception:
            pass

        pdi_state, pct = "-", 0
        try:
            r = conn.execute(
                "SELECT status, percent_ok FROM pdi_jobs WHERE vin_no=? ORDER BY id DESC LIMIT 1",
                (vin_no or token,)
            ).fetchone()
            if r:
                pdi_state, pct = (r["status"] or "-"), int(r["percent_ok"] or 0)
        except Exception:
            pass

        lock_state = "-"
        try:
            r = conn.execute("SELECT is_locked FROM pdi_locks WHERE vin_no=? LIMIT 1", (vin_no or token,)).fetchone()
            if r:
                lock_state = "🔒 Locked" if int(r[0]) == 1 else "🔓 Unlocked"
            else:
                lock_state = "🔓 Unlocked"
        except Exception:
            pass

        export_state = "-"
        try:
            r = conn.execute("SELECT status FROM export_jobs WHERE vin_no=? ORDER BY id DESC LIMIT 1",
                             (vin_no or token,)).fetchone()
            if r:
                export_state = r["status"] or "-"
        except Exception:
            pass

        msg = (
            f"🚗 **สถานะรถ**\n"
            f"VIN: `{vin_no or '-'}`\n"
            f"ID VAN: `{id_van or '-'}`\n"
            f"📍 Slot: {slot}\n"
            f"🔹 Vehicle Status: {v_status}\n"
            f"🔹 PDI: {pdi_state} ({pct}%)\n"
            f"🔹 Export: {export_state}\n"
            f"🔹 Lock: {lock_state}"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
    finally:
        conn.close()

def main():
    load_config()

    if not _acquire_singleton_lock():
        print("Another instance is running. Exiting to avoid getUpdates conflict.")
        return

    ensure_views()

    application = ApplicationBuilder().token(CONFIG["telegram_bot_token"]).build()
    application.add_error_handler(_on_error)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("s", s_cmd))
    application.add_handler(CommandHandler("out", out_cmd))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    application.add_handler(CallbackQueryHandler(location_button_handler, pattern="^out_idx:"))


    application.add_handler(CommandHandler("inv", inv_cmd))
    application.add_handler(CommandHandler("otoday", otoday_cmd))
    application.add_handler(CommandHandler("oto", oto_cmd))

    application.add_handler(CommandHandler("PDMO", pdmo_cmd))
    application.add_handler(CommandHandler("PDMI", pdmi_cmd))
    application.add_handler(CommandHandler("EXPCF", expcf_cmd))
    application.add_handler(CommandHandler("STATUS", status_cmd))
    application.add_handler(CommandHandler("rein", rein_cmd))

    interval = int(CONFIG.get("poll_interval_minutes", 3))
    t = threading.Thread(target=scheduler_thread, args=(application, interval), daemon=True)
    t.start()

    try:
        application.run_polling(drop_pending_updates=True)
    finally:
        _release_singleton_lock()

if __name__ == "__main__":
    main()