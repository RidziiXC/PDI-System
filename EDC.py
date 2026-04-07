from flask import Flask, render_template_string, request, send_file, jsonify
from fpdf import FPDF
import random
import io
import json
import os
import pandas as pd
import zipfile
from datetime import datetime, timedelta

app = Flask(__name__)

# การจัดการไฟล์ตั้งค่าเทมเพลต
CONFIG_FILE = 'battery_template_config.json'
DEFAULT_CONFIG = {
    "m_left": 8.0,
    "label_w": 38.0,
    "l_height": 6.5,
    "f_body": 11,
    "txt_soc": "SOC:",
    "txt_vol": "VOLTAGE:",
    "txt_soh": "SOH:",
    "txt_mea": "MEASURED:",
    "txt_input": "SELECT INPUT:",
    "txt_rated": "Rated:",
    "txt_type": "TYPE:",
    "txt_res": "Internal R:"
}

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return DEFAULT_CONFIG
    return DEFAULT_CONFIG

def save_config(config_data):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config_data, f, ensure_ascii=False, indent=4)

# ฟังก์ชันสร้างใบรายงาน PDF
def create_battery_pdf(conf, model_type, test_date_str):
    pdf = FPDF(format=(65, 140))
    pdf.set_auto_page_break(False)
    pdf.add_page()
    
    m_left = float(conf.get('m_left', 8.0))
    label_w = float(conf.get('label_w', 38.0))
    l_height = float(conf.get('l_height', 6.5))
    f_size = int(conf.get('f_body', 11))

    pdf.set_font('Courier', 'B', 16)
    pdf.cell(0, 10, 'UT675A', 0, 1, 'C')
    
    pdf.set_font('Courier', '', f_size)
    pdf.cell(0, 6, test_date_str, 0, 1, 'C')
    pdf.cell(0, 6, "TEST REPORT", 0, 1, 'C')
    pdf.ln(2)
    
    # ส่วนหัว BATTERY TEST แบบแถบดำ
    pdf.set_fill_color(0, 0, 0)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Courier', 'B', 12)
    pdf.cell(0, 7, "BATTERY TEST", 0, 1, 'C', fill=True)
    
    pdf.set_text_color(0, 0, 0)
    pdf.ln(2)
    pdf.set_font('Courier', 'B', 11)
    pdf.cell(0, 8, "GOOD BATTERY", 0, 1, 'C')
    pdf.ln(3)

    # แยกแยะรุ่น YP(370A) และ ES(420A)
    model_type = str(model_type).upper()
    if "ES" in model_type or "420" in model_type:
        rated, measured, res = 420, random.randint(460, 530), round(random.uniform(5.6, 6.3), 2)
    else:
        rated, measured, res = 370, random.randint(380, 410), round(random.uniform(7.1, 7.6), 2)
        
    voltage = round(random.uniform(12.75, 13.78), 2)

    pdf.set_font('Courier', '', f_size)
    def add_row(txt_key, val):
        pdf.set_x(m_left)
        pdf.cell(label_w, l_height, conf.get(txt_key), 0, 0)
        pdf.cell(0, l_height, val, 0, 1)

    add_row("txt_soc", "100%")
    add_row("txt_vol", f"{voltage}V")
    add_row("txt_soh", "100%")
    add_row("txt_mea", f"{measured}A")
    add_row("txt_input", "CCA")
    add_row("txt_rated", f"{rated}A")
    
    pdf.set_x(m_left)
    pdf.cell(15, l_height, conf.get("txt_type"), 0, 0)
    pdf.cell(0, l_height, " Regular Flooded", 0, 1)
    
    add_row("txt_res", f"{res}m$")

    pdf.ln(8)
    pdf.set_font('Courier', '', 10)
    pdf.cell(0, 5, "*" * 28, 0, 1, 'C')
    pdf.cell(0, 5, "*" * 28, 0, 1, 'C')

    return pdf.output(dest='S').encode('latin-1')

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>UT675A PDF System (Random Time +3-7m)</title>
    <style>
        body { font-family: 'Segoe UI', sans-serif; background: #f0f2f5; padding: 40px; display: flex; justify-content: center; }
        .container { background: white; padding: 30px; border-radius: 15px; width: 750px; box-shadow: 0 4px 25px rgba(0,0,0,0.1); }
        .section { background: #fdfdfd; padding: 20px; border-radius: 12px; margin-bottom: 25px; border: 1px solid #eee; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        input, select { width: 100%; padding: 12px; border: 1px solid #ddd; border-radius: 8px; box-sizing: border-box; }
        button { padding: 15px; cursor: pointer; border: none; border-radius: 8px; font-weight: bold; width: 100%; transition: 0.3s; }
        .btn-save { background: #3498db; color: white; }
        .btn-gen { background: #2ecc71; color: white; margin-top: 15px; }
        .file-input { border: 2px dashed #ddd; padding: 30px; text-align: center; border-radius: 8px; cursor: pointer; background: #fff; }
        .file-input:hover { border-color: #e67e22; background: #fffcf9; }
        .status-msg { margin-top: 10px; color: #e67e22; font-weight: bold; display: none; }
    </style>
</head>
<body>
    <div class="container">
        <h2>UT675A PDF Manager (Random Time +3-7m)</h2>
        
        <form id="templateForm">
            <div class="section">
                <strong>🛠️ ตั้งค่าเทมเพลต (มิลลิเมตร)</strong>
                <div class="grid" style="margin-top:10px;">
                    <div><label>ขอบซ้าย</label><input type="number" name="m_left" value="{{conf.m_left}}" step="0.1"></div>
                    <div><label>ความกว้างชื่อ</label><input type="number" name="label_w" value="{{conf.label_w}}" step="0.1"></div>
                    <div><label>ระยะห่างบรรทัด</label><input type="number" name="l_height" value="{{conf.l_height}}" step="0.1"></div>
                    <div><label>ฟอนต์เนื้อหา</label><input type="number" name="f_body" value="{{conf.f_body}}"></div>
                </div>
                <button type="button" class="btn-save" onclick="saveTemplate()" style="margin-top:15px;">💾 บันทึกเทมเพลต</button>
            </div>
            {% for key in ['txt_soc','txt_vol','txt_soh','txt_mea','txt_input','txt_rated','txt_type','txt_res'] %}
            <input type="hidden" name="{{key}}" value="{{conf[key]}}">
            {% endfor %}
        </form>

        <div class="grid">
            <div class="section">
                <strong>SINGLE: สร้างใบเดียว</strong>
                <form action="/generate" method="post" target="_blank">
                    <select name="model_type" style="margin-top:10px;">
                        <option value="YP">370A (YP)</option>
                        <option value="ES">420A (ES)</option>
                    </select>
                    <input type="datetime-local" name="test_date" id="test_date" required style="margin-top:10px;">
                    <button type="submit" class="btn-gen">📄 ออก PDF</button>
                </form>
            </div>

            <div class="section">
                <strong>BATCH: นำเข้า Excel (A, B, D)</strong>
                <form id="excelForm" action="/batch_excel" method="post" enctype="multipart/form-data">
                    <div class="file-input" onclick="document.getElementById('excelFile').click()" style="margin-top:10px;">
                        📂 เลือกไฟล์ Excel<br>
                        <small>(เวลาจะถูกสุ่มบวกเพิ่ม 3-7 นาที)</small>
                        <div id="statusMsg" class="status-msg">กำลังประมวลผล...</div>
                    </div>
                    <input type="file" name="excel_file" id="excelFile" accept=".xlsx, .xls" required style="display:none;" onchange="handleFileSelect()">
                </form>
            </div>
        </div>
    </div>
    <script>
        document.getElementById('test_date').value = new Date().toISOString().slice(0, 16);
        function saveTemplate() {
            const formData = new FormData(document.getElementById('templateForm'));
            const data = {};
            formData.forEach((value, key) => { data[key] = value; });
            fetch('/save_config', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) })
            .then(res => res.json()).then(res => { alert(res.message); location.reload(); });
        }
        function handleFileSelect() {
            const statusMsg = document.getElementById('statusMsg');
            if (document.getElementById('excelFile').files.length > 0) {
                statusMsg.style.display = 'block';
                document.getElementById('excelForm').submit();
            }
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    conf = load_config()
    return render_template_string(HTML_TEMPLATE, conf=conf)

@app.route('/save_config', methods=['POST'])
def save_cfg():
    save_config(request.json)
    return jsonify({"status": "success", "message": "บันทึกเทมเพลตเรียบร้อย!"})

@app.route('/generate', methods=['POST'])
def generate_single():
    conf = load_config()
    f = request.form
    dt_str = datetime.strptime(f.get('test_date'), '%Y-%m-%dT%H:%M').strftime('%Y-%m-%d  %H:%M')
    pdf_content = create_battery_pdf(conf, f.get('model_type'), dt_str)
    return send_file(io.BytesIO(pdf_content), mimetype='application/pdf')

@app.route('/batch_excel', methods=['POST'])
def batch_excel():
    if 'excel_file' not in request.files: return "No file"
    file = request.files['excel_file']
    try:
        # อ่านไฟล์ Excel (คอลัมน์ A=0, B=1, D=3)
        df = pd.read_excel(file, header=None)
    except Exception as e:
        return f"Excel Error: {e}"
    
    conf = load_config()
    zip_buf = io.BytesIO()
    count = 0

    with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for _, row in df.iterrows():
            try:
                # สแกนหารุ่นในแถวนั้นๆ
                row_text = " ".join([str(val) for val in row.values if not pd.isna(val)])
                model = "YP"
                if "ES" in row_text.upper(): model = "ES"
                elif "YP" in row_text.upper(): model = "YP"
                
                vin = str(row[0]).strip()
                raw_time_data = row[1]
                
                # แปลงเวลาและสุ่มบวกเพิ่ม 3-7 นาที
                if isinstance(raw_time_data, datetime):
                    dt = raw_time_data
                else:
                    dt = datetime.strptime(str(raw_time_data).strip(), '%Y.%m.%d %H:%M:%S')
                
                # สุ่มบวกเวลาเพิ่ม 3 ถึง 7 นาที
                random_minutes = random.randint(3, 7)
                dt_final = dt + timedelta(minutes=random_minutes)
                clean_time = dt_final.strftime('%Y-%m-%d  %H:%M')

                pdf_content = create_battery_pdf(conf, model, clean_time)
                # ชื่อไฟล์ VIN_report.pdf
                zip_file.writestr(f"{vin}_Export_report.pdf", pdf_content)
                count += 1
            except: 
                continue
            
    zip_buf.seek(0)
    if count == 0: return "ไม่พบข้อมูลที่ถูกต้องในไฟล์ Excel"
    
    return send_file(zip_buf, mimetype='application/zip', as_attachment=True, 
                     download_name=f'reports_{datetime.now().strftime("%H%M%S")}.zip')

if __name__ == '__main__':
    # รันบนพอร์ต 7100
    app.run(host='0.0.0.0', port=7100)