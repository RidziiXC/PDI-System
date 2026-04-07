# pdi_bridge.py
# -*- coding: utf-8 -*-
# Helpers for registering Telegram commands in a separate module (imported by main)

import json, re, sqlite3
from datetime import datetime
from typing import Tuple, Optional
from telegram.ext import CommandHandler, ContextTypes


# ---------- REIN: write in_yard log to stockout.db (CMD source) ----------
import json as _json_mod_for_rein

def _rein_load_cfg():
    try:
        with open("config.json","r",encoding="utf-8") as _f:
            return _json_mod_for_rein.load(_f)
    except Exception:
        return {}

def _rein_ensure_stockout_db(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS stockout_logs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vin_no TEXT NOT NULL,
        at TEXT NOT NULL,
        action TEXT,
        slot TEXT,
        id_van TEXT,
        source TEXT
    )""")
    conn.commit()

def write_in_yard_after_rein(vin_no: str, source: str = "cmd"):
    vin_no = (vin_no or "").strip()
    if not vin_no:
        return
    cfg = _rein_load_cfg()
    sdb = cfg.get("stockout_db_path","stockout.db")
    db  = cfg.get("db_path","stock.db")

    id_van, slot = None, None
    try:
        _m = sqlite3.connect(db)
        _r = _m.execute("SELECT id_van, COALESCE(slot,'') FROM vehicles WHERE vin_no=?", (vin_no,)).fetchone()
        if _r:
            id_van, slot = _r[0], _r[1]
    finally:
        try: _m.close()
        except Exception: pass

    _conn = sqlite3.connect(sdb)
    try:
        _rein_ensure_stockout_db(_conn)
        _at = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        _conn.execute(
            "INSERT INTO stockout_logs(vin_no, at, action, slot, id_van, source) VALUES(?,?,?,?,?,?)",
            (vin_no, _at, "in_yard", slot, id_van, source)
        )
        _conn.commit()
    finally:
        _conn.close()
from telegram import Update

def _load_cfg():
    with open("config.json","r",encoding="utf-8") as f:
        return json.load(f)

CFG = _load_cfg()
DB_PATH = CFG.get("db_path","stock.db")
STOCKOUT_DB_PATH = CFG.get("stockout_db_path","stockout.db")

# ---------- DB helpers ----------
def _ensure_pdi_core(conn: sqlite3.Connection):
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
        from_slot TEXT, to_slot TEXT,
        locked_at TEXT, unlocked_at TEXT
    )""")
    conn.commit()

def _ensure_movements(conn: sqlite3.Connection):
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

def _exists_in_stock(conn: sqlite3.Connection, vin_no: str, id_van: Optional[str]) -> bool:
    for tbl, col in [("stocks","vin_no"),("stock","vin_no"),("vehicles","vin_no")]:
        try:
            if conn.execute(f"SELECT 1 FROM {tbl} WHERE {col}=?", (vin_no,)).fetchone():
                return True
        except Exception:
            pass
    if id_van:
        try:
            if conn.execute("SELECT 1 FROM vehicles WHERE id_van=?", (id_van,)).fetchone():
                return True
        except Exception:
            pass
    return False

def _resolve_vin_idvan(conn: sqlite3.Connection, token: str) -> Tuple[str,str]:
    t = (token or "").strip()
    if not t:
        return "",""
    is_idvan = bool(re.fullmatch(r"[A-Za-z]+[0-9]+", t))
    if is_idvan:
        r = conn.execute("SELECT vin_no,id_van FROM vehicles WHERE id_van=?", (t,)).fetchone()
    else:
        r = conn.execute("SELECT vin_no,id_van FROM vehicles WHERE vin_no=?", (t,)).fetchone()
    if not r:
        return "",""
    vin, idv = r[0], r[1]
    if not _exists_in_stock(conn, vin, idv):
        return "",""
    return vin, idv or ""

def _get_or_create_job(conn: sqlite3.Connection, vin_no: str, id_van: str) -> int:
    _ensure_pdi_core(conn)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    r = conn.execute("SELECT id FROM pdi_jobs WHERE vin_no=? ORDER BY id DESC LIMIT 1",(vin_no,)).fetchone()
    if r:
        job_id = r[0]
        conn.execute("UPDATE pdi_jobs SET updated_at=? WHERE id=?", (now, job_id))
    else:
        conn.execute("""INSERT INTO pdi_jobs(vin_no,id_van,status,percent_ok,created_at,updated_at)
                        VALUES(?,?,?,?,?,?)""", (vin_no, id_van, 'pending', 0, now, now))
        job_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    return job_id

# ---------- Commands ----------
async def pdmo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("ใช้: /PDMO <VIN หรือ IDVAN>")
        return
    token = context.args[0].strip()
    conn = sqlite3.connect(DB_PATH)
    try:
        vin, idvan = _resolve_vin_idvan(conn, token)
        if not vin:
            await update.message.reply_text("❌ ไม่พบรถในสต็อก/vehicles")
            return
        _ensure_pdi_core(conn)
        if conn.execute("SELECT 1 FROM pdi_locks WHERE vin_no=? AND is_locked=1",(vin,)).fetchone():
            await update.message.reply_text("คันนี้ถูกส่งเข้าคิวและล็อคอยู่แล้ว")
            return
        job_id = _get_or_create_job(conn, vin, idvan)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            _row_slot = conn.execute("SELECT slot FROM vehicles WHERE vin_no=?", (vin,)).fetchone()
            _from_slot = (_row_slot[0] or None) if _row_slot else None
        except Exception:
            _from_slot = None
        conn.execute("""INSERT INTO pdi_locks(vin_no,job_id,is_locked,from_slot,locked_at)
                        VALUES(?,?,1,?,?)
                        ON CONFLICT(vin_no) DO UPDATE SET
                            job_id=excluded.job_id,
                            is_locked=1,
                            locked_at=excluded.locked_at,
                            unlocked_at=NULL""", (vin, job_id, _from_slot, now))
        conn.commit()
    finally:
        conn.close()
    await update.message.reply_text(f"✅ ส่งรถเข้าคิว PDI และล็อคสต็อกแล้ว\nVIN: {vin} | ID VAN: {idvan or '-'}")

async def pdmi_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("ใช้: /PDMI <VIN หรือ IDVAN> <SLOT> [OUT]")
        return
    token = context.args[0].strip()
    slot  = " ".join(context.args[1:]).strip()
    want_out = False
    parts = slot.split()
    if parts and parts[-1].upper() in ("OUT","CF","OUT!"):
        want_out = True
        slot = " ".join(parts[:-1]).strip() or None
    conn = sqlite3.connect(DB_PATH)
    try:
        vin, idvan = _resolve_vin_idvan(conn, token)
        if not vin:
            await update.message.reply_text("❌ ไม่พบรถในสต็อก/vehicles")
            return
        _ensure_pdi_core(conn)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""UPDATE pdi_locks SET is_locked=0, to_slot=?, unlocked_at=? WHERE vin_no=?""",
                     (slot, now, vin))
        conn.execute("UPDATE vehicles SET slot=? WHERE vin_no=?", (slot, vin))
        conn.commit()
    finally:
        conn.close()
    if want_out:
        _write_stockout(vin, slot)
        await update.message.reply_text(f"✅ ปลดล็อคและ 'นำออกจากระบบ' แล้ว\nVIN: {vin} | ช่อง: {slot}")
    else:
        await update.message.reply_text(f"✅ ปลดล็อคแล้ว (เข้าช่อง: {slot})")

async def cpdi_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("ใช้: /CPDI <VIN หรือ IDVAN> <SLOT>")
        return
    token = context.args[0].strip()
    slot  = " ".join(context.args[1:]).strip()
    conn = sqlite3.connect(DB_PATH)
    try:
        vin, idvan = _resolve_vin_idvan(conn, token)
        if not vin:
            await update.message.reply_text("❌ ไม่พบรถในสต็อก/vehicles")
            return
        _ensure_pdi_core(conn)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""UPDATE pdi_locks SET is_locked=0, to_slot=?, unlocked_at=? WHERE vin_no=?""",
                     (slot, now, vin))
        conn.commit()
    finally:
        conn.close()
    await update.message.reply_text(f"✅ ยกเลิกทำ PDI และนำรถเข้าช่อง {slot} แล้ว")

# ---------- RE-IN (stock_in รอบ 2+) ----------
async def rein_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
    write_in_yard_after_rein(vin, source=\"cmd\")

        await update.message.reply_text("ใช้: /rein <VIN หรือ IDVAN>")
        return
    token = context.args[0].strip()

    conn = sqlite3.connect(DB_PATH)
    try:
        vin, idvan = _resolve_vin_idvan(conn, token)
        if not vin:
            await update.message.reply_text("❌ ไม่พบรถในสต็อก/vehicles")
            return

        _ensure_movements(conn)

        # ดึง slot ปัจจุบัน เพื่อบันทึกเป็น "rein slot เสมอ"
        row = conn.execute("SELECT COALESCE(slot,''), COALESCE(status,'') FROM vehicles WHERE vin_no=?", (vin,)).fetchone()
        cur_slot = (row[0] if row else "") or ""

        # 1) อัปเดต stock_in = วันนี้ (DD-MM-YYYY) ทับค่าเดิม และตั้งสถานะเข้า yard
        today_ddmmyyyy = datetime.now().strftime("%d-%m-%Y")
        conn.execute("UPDATE vehicles SET stock_in=?, status='in_yard' WHERE vin_no=?", (today_ddmmyyyy, vin))

        # 2) บันทึก movement เป็น stock_ins (source=inventory_cmd, note=rein)
        now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO movements(vin_no,at,action,from_slot,to_slot,note,source) VALUES(?,?,?,?,?,?,?)",
            (vin, now_iso, "stock_ins", None, cur_slot, "rein", "inventory_cmd")
        )

        conn.commit()
    finally:
        conn.close()

    await update.message.reply_text(f"✅ RE-IN สำเร็จ\nVIN: {vin}\nslot: {cur_slot or '-'}\nstock_in: {today_ddmmyyyy}")

# ---------- stockout helper ----------
def _write_stockout(vin_no: str, slot: Optional[str]):
    at = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    conn = sqlite3.connect(STOCKOUT_DB_PATH)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS stockout_logs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vin_no TEXT NOT NULL,
            at TEXT NOT NULL,
            slot TEXT,
            id_van TEXT,
            action TEXT,
            source TEXT
        )""")
        m = sqlite3.connect(DB_PATH)
        try:
            row = m.execute("SELECT id_van FROM vehicles WHERE vin_no=?", (vin_no,)).fetchone()
            id_van = row[0] if row else None
        finally:
            m.close()
        conn.execute("INSERT INTO stockout_logs(vin_no,at,slot,id_van,action,source) VALUES(?,?,?,?,?,?)",
                     (vin_no, at, slot, id_van, "out", "pdmi"))
        conn.commit()
    finally:
        conn.close()

def register_handlers(application):
    application.add_handler(CommandHandler("PDMO", pdmo_cmd))
    application.add_handler(CommandHandler("PDMI", pdmi_cmd))
    application.add_handler(CommandHandler("CPDI", cpdi_cmd))
    application.add_handler(CommandHandler("rein", rein_cmd))
