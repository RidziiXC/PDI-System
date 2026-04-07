import os
import json
import sqlite3
import io
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Request, Form, UploadFile, File, Query
from fastapi.responses import RedirectResponse, HTMLResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates

try:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.colors import HexColor
    from reportlab.lib.utils import ImageReader
    from pypdf import PdfReader, PdfWriter
    from PIL import Image
    from xhtml2pdf import pisa
except ImportError:
    pass

CONFIG = {}
PDF_COORDS = {}

def load_config():
    global CONFIG, PDF_COORDS
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            CONFIG = json.load(f)
    except Exception:
        CONFIG = {}
    try:
        with open("pdf_coords.json", "r", encoding="utf-8") as f:
            PDF_COORDS = json.load(f)
    except Exception:
        PDF_COORDS = {}

def DB_PATH() -> str:
    return CONFIG.get("pdi_db", CONFIG.get("db_path", "stock.db"))

def UPLOAD_DIR() -> str:
    path = "uploads_claim"
    os.makedirs(path, exist_ok=True)
    return path

def PDF_TEMPLATE_DIR() -> str:
    path = "PDFC"
    os.makedirs(path, exist_ok=True)
    return path

def get_db():
    conn = sqlite3.connect(DB_PATH())
    conn.row_factory = sqlite3.Row
    return conn

def ensure_tables():
    conn = get_db()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS claim_battery_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vin_no TEXT NOT NULL,
                voltage TEXT,
                battery_health TEXT,
                charge_status TEXT,
                cca_value TEXT,
                image_path TEXT,
                created_at TEXT,
                created_by TEXT
            )
        """)
        conn.commit()
    finally:
        conn.close()

def get_vehicle_info(vin):
    conn = get_db()
    try:
        veh = conn.execute("SELECT * FROM vehicles WHERE vin_no=?", (vin,)).fetchone()
        vdci = conn.execute("SELECT * FROM vdci_report_pairs WHERE vin_no=? ORDER BY id DESC LIMIT 1", (vin,)).fetchone()
        batt = conn.execute("SELECT * FROM claim_battery_checks WHERE vin_no=? ORDER BY id DESC LIMIT 1", (vin,)).fetchone()
        return veh, vdci, batt
    finally:
        conn.close()

def get_battery_record(record_id):
    conn = get_db()
    try:
        return conn.execute("SELECT * FROM claim_battery_checks WHERE id=?", (record_id,)).fetchone()
    finally:
        conn.close()

def find_template_for_model(model_name: str):
    pdf_dir = PDF_TEMPLATE_DIR()
    if not os.path.exists(pdf_dir): return None
    files = [f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf")]
    if model_name:
        safe = model_name.strip().lower()
        for f in files:
            if safe in f.lower(): return os.path.join(pdf_dir, f)
    if files: return os.path.join(pdf_dir, files[0])
    return None

def list_pdf_templates():
    pdf_dir = PDF_TEMPLATE_DIR()
    if not os.path.exists(pdf_dir): return []
    return [f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf")]

def get_coords_for_file(filename: str):
    fname_key = os.path.basename(filename).lower().replace(".pdf", "")
    if fname_key in PDF_COORDS:
        return PDF_COORDS[fname_key]
    for key in PDF_COORDS:
        if key != "default" and key in fname_key:
            return PDF_COORDS[key]
    return PDF_COORDS.get("default", {})

def create_overlay_pdf(data: dict, coords: dict):
    packet = io.BytesIO()
    can = canvas.Canvas(packet, pagesize=A4)
    for key, val in data.items():
        if key in coords and val:
            cfg = coords[key]
            x, y, size, color = 0, 0, 10, "#000000"
            if isinstance(cfg, list):
                x, y = cfg
            elif isinstance(cfg, dict):
                x, y = cfg.get("x", 0), cfg.get("y", 0)
                size = int(cfg.get("size", 10))
                color = cfg.get("color", "#000000")
            try: can.setFillColor(HexColor(color))
            except: can.setFillColor(HexColor("#000000"))
            can.setFont("Helvetica", size)
            can.drawString(float(x), float(y), str(val))
    can.save()
    packet.seek(0)
    return packet

def image_to_pdf(image_path: str):
    if not os.path.exists(image_path): return None
    try:
        img = Image.open(image_path)
        if img.mode in ("RGBA", "P"): img = img.convert("RGB")
        max_width = 1000
        if img.width > max_width:
            img = img.resize((max_width, int(img.height * (max_width / img.width))), Image.Resampling.LANCZOS)
        img_byte = io.BytesIO()
        img.save(img_byte, format='JPEG', quality=50, optimize=True)
        img_byte.seek(0)
        packet = io.BytesIO()
        c = canvas.Canvas(packet, pagesize=A4)
        a4_w, a4_h = A4
        margin = 40
        scale = min((a4_w - 2*margin)/img.width, (a4_h - 2*margin)/img.height)
        final_w, final_h = img.width * scale, img.height * scale
        c.drawImage(ImageReader(img_byte), (a4_w - final_w) / 2, (a4_h - final_h) / 2, width=final_w, height=final_h)
        c.showPage()
        c.save()
        packet.seek(0)
        return PdfReader(packet)
    except Exception: return None

def html_to_pdf(html_path: str):
    if not os.path.exists(html_path): return None
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            source_html = f.read()
        css = "<style>@page { size: A4; margin: 0.5cm; } body { font-family: Helvetica; font-size: 8px; zoom: 0.7; } table { width: 100%; border-collapse: collapse; } th, td { border: 1px solid #ddd; padding: 3px; } img { max-width: 90%; }</style>"
        output = io.BytesIO()
        pisa.CreatePDF(css + source_html, dest=output, encoding='utf-8')
        output.seek(0)
        return PdfReader(output)
    except Exception: return None

app = FastAPI(title="Claim Management System")
load_config()
ensure_tables()

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/uploads_claim", StaticFiles(directory=UPLOAD_DIR()), name="uploads_claim")
app.mount("/uploads_damage", StaticFiles(directory="uploads_damage"), name="uploads_damage")
templates = Jinja2Templates(directory="templatesx")

@app.get("/", response_class=RedirectResponse)
def root(): return "/pdi"

@app.get("/others", response_class=HTMLResponse)
def others_page(request: Request):
    return templates.TemplateResponse("others.html", {"request": request, "active_tab": "others"})

@app.get("/pdi", response_class=HTMLResponse)
def pdi_search_page(request: Request):
    return templates.TemplateResponse("pdi_search.html", {"request": request, "active_tab": "pdi"})

@app.get("/api/search")
def api_search(q: str):
    conn = get_db()
    try:
        rows = conn.execute("SELECT vin_no, model FROM vehicles WHERE vin_no LIKE ? OR id_van LIKE ? LIMIT 10", (f"%{q}%", f"%{q}%")).fetchall()
        return [{"vin_no": r["vin_no"], "model": r["model"]} for r in rows]
    finally: conn.close()

@app.get("/pdi/{vin}", response_class=HTMLResponse)
def pdi_detail_page(request: Request, vin: str):
    veh, vdci, batt = get_vehicle_info(vin)
    if not veh: return templates.TemplateResponse("base.html", {"request": request, "active_tab": "pdi", "error_message": "ไม่พบรถ"})
    return templates.TemplateResponse("pdi_detail.html", {"request": request, "active_tab": "pdi", "vin": vin, "veh": veh, "vdci": vdci, "batt": batt, "all_templates": list_pdf_templates()})

@app.get("/pdi/{vin}/battery_popup", response_class=HTMLResponse)
def battery_popup_page(request: Request, vin: str, id: Optional[int] = None):
    record = None
    if id: record = get_battery_record(id)
    return templates.TemplateResponse("battery_popup.html", {"request": request, "vin": vin, "record": record})

@app.post("/api/save_battery")
async def save_battery_api(vin: str = Form(...), voltage: str = Form(...), health: str = Form(...), soc: str = Form(...), cca: str = Form(...), image: UploadFile = File(...)):
    conn = get_db()
    try:
        existing = conn.execute("SELECT id, image_path FROM claim_battery_checks WHERE vin_no=?", (vin,)).fetchone()
        filename = f"{vin}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{image.filename}"
        filepath = os.path.join(UPLOAD_DIR(), filename)
        with open(filepath, "wb") as f: f.write(await image.read())
        path_db = f"uploads_claim/{filename}"
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if existing:
            if existing["image_path"] and os.path.isfile(existing["image_path"]):
                try: os.remove(existing["image_path"])
                except: pass
            conn.execute("UPDATE claim_battery_checks SET voltage=?, battery_health=?, charge_status=?, cca_value=?, image_path=?, created_at=? WHERE id=?", (voltage, health, soc, cca, path_db, now_str, existing["id"]))
        else:
            conn.execute("INSERT INTO claim_battery_checks (vin_no, voltage, battery_health, charge_status, cca_value, image_path, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)", (vin, voltage, health, soc, cca, path_db, now_str))
        conn.commit()
        return {"ok": True}
    except Exception as e: return {"ok": False, "error": str(e)}
    finally: conn.close()

@app.post("/api/update_battery")
async def update_battery_api(id: int = Form(...), vin: str = Form(...), voltage: str = Form(...), health: str = Form(...), soc: str = Form(...), cca: str = Form(...), image: UploadFile = File(None)):
    conn = get_db()
    try:
        old = conn.execute("SELECT image_path FROM claim_battery_checks WHERE id=?", (id,)).fetchone()
        img_p = old["image_path"] if old else None
        if image and image.filename:
            fname = f"{vin}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{image.filename}"
            fpath = os.path.join(UPLOAD_DIR(), fname)
            with open(fpath, "wb") as f: f.write(await image.read())
            if img_p and os.path.isfile(img_p):
                try: os.remove(img_p)
                except: pass
            img_p = f"uploads_claim/{fname}"
        conn.execute("UPDATE claim_battery_checks SET voltage=?, battery_health=?, charge_status=?, cca_value=?, image_path=? WHERE id=?", (voltage, health, soc, cca, img_p, id))
        conn.commit()
        return {"ok": True}
    except Exception as e: return {"ok": False, "error": str(e)}
    finally: conn.close()

@app.post("/api/delete_battery")
def delete_battery_api(id: int = Form(...)):
    conn = get_db()
    try:
        r = conn.execute("SELECT image_path FROM claim_battery_checks WHERE id=?", (id,)).fetchone()
        if r and r["image_path"] and os.path.isfile(r["image_path"]):
            try: os.remove(r["image_path"])
            except: pass
        conn.execute("DELETE FROM claim_battery_checks WHERE id=?", (id,))
        conn.commit()
        return {"ok": True}
    except Exception as e: return {"ok": False, "error": str(e)}
    finally: conn.close()

@app.get("/config/pdf", response_class=HTMLResponse)
def pdf_config_page(request: Request):
    return templates.TemplateResponse("pdf_config.html", {"request": request})

@app.get("/api/pdf/list")
def list_pdfs(): return list_pdf_templates()

@app.get("/api/pdf/file/{filename}")
def get_pdf_file(filename: str):
    path = os.path.join(PDF_TEMPLATE_DIR(), filename)
    if os.path.exists(path): return FileResponse(path)
    return HTMLResponse("Not found", 404)

@app.get("/api/pdf/coords/{filename}")
def get_pdf_coords(filename: str):
    key = filename.lower().replace(".pdf", "")
    if key in PDF_COORDS: return {"key": key, "data": PDF_COORDS[key]}
    res_k = "default"
    for k in PDF_COORDS:
        if k != "default" and k in key: res_k = k; break
    return {"key": key if "pdpa" in key else res_k, "data": PDF_COORDS.get(res_k, {})}

@app.post("/api/pdf/save_coords")
async def save_pdf_coords(request: Request):
    data = await request.json()
    key, coords = data.get("key") or "default", data.get("coords")
    try:
        with open("pdf_coords.json", "r", encoding="utf-8") as f: cfg = json.load(f)
    except: cfg = {}
    cfg[key] = coords
    with open("pdf_coords.json", "w", encoding="utf-8") as f: json.dump(cfg, f, ensure_ascii=False, indent=2)
    global PDF_COORDS
    PDF_COORDS = cfg
    return {"ok": True}

@app.get("/api/gen_pdf/{vin}")
def generate_pdf_endpoint(vin: str, template_name: Optional[str] = Query(None)):
    veh, vdci, batt = get_vehicle_info(vin)
    if not veh: return HTMLResponse("Not found", 404)
    base_dt = datetime.now()
    ck_date = ""
    if vdci and vdci["after_report_time"]:
        try:
            raw = str(vdci["after_report_time"]).replace('.', '-').replace('/', '-')
            base_dt = datetime.strptime(raw.split()[0], "%Y-%m-%d")
            ck_date = base_dt.strftime("%d/%m/%Y")
        except: ck_date = str(vdci["after_report_time"])
    p1 = (base_dt + timedelta(days=1)).strftime("%d/%m/%Y")
    data = {
        "vin": vin, "vin_2": vin, "check_date": ck_date, "report_date_2": ck_date,
        "voltage": batt["voltage"] if batt else "", "soh": batt["battery_health"] if batt else "",
        "soc": batt["charge_status"] if batt else "", "cca": batt["cca_value"] if batt else "",
        "delivery_date_plus_1": p1, "ad_date_plus_1": p1, "phone_locked": "020177756"
    }
    writer = PdfWriter()
    if template_name:
        tp = os.path.join(PDF_TEMPLATE_DIR(), template_name)
        if os.path.exists(tp):
            page = PdfReader(tp).pages[0]
            page.merge_page(PdfReader(create_overlay_pdf(data, get_coords_for_file(tp))).pages[0])
            writer.add_page(page)
    for f in ["PDPA1.pdf", "PDPA2.pdf"]:
        p = os.path.join(PDF_TEMPLATE_DIR(), f)
        if os.path.exists(p):
            pg = PdfReader(p).pages[0]
            pg.merge_page(PdfReader(create_overlay_pdf(data, get_coords_for_file(f))).pages[0])
            writer.add_page(pg)
    if batt and batt["image_path"]:
        b = image_to_pdf(batt["image_path"])
        if b: writer.add_page(b.pages[0])
    if vdci and vdci["after_file_path"]:
        v = html_to_pdf(os.path.join("uploads_damage", vdci["after_file_path"]))
        if v:
            for pg in v.pages: writer.add_page(pg)
    out = io.BytesIO()
    writer.write(out)
    out.seek(0)
    return StreamingResponse(out, media_type="application/pdf", headers={"Content-Disposition": f"attachment; filename=Claim_{vin}.pdf"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9120)