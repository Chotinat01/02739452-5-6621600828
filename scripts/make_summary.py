"""Render the three-page Thai summary using actual screenshots and evidence."""
import json
import os
from pathlib import Path
from xml.sax.saxutils import escape
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, Table, TableStyle

ROOT = Path(__file__).resolve().parents[1]
FONT_DIR = Path(os.getenv('PDF_FONT_DIR', 'C:/Windows/Fonts'))
pdfmetrics.registerFont(TTFont('Thai', str(FONT_DIR / 'tahoma.ttf'), shapable=True))
pdfmetrics.registerFont(TTFont('ThaiBold', str(FONT_DIR / 'tahomabd.ttf'), shapable=True))
pdfmetrics.registerFontFamily('Thai', normal='Thai', bold='ThaiBold')
styles = {
    'title': ParagraphStyle('title', fontName='ThaiBold', fontSize=20, leading=27, spaceAfter=5, shaping=1),
    'h': ParagraphStyle('h', fontName='ThaiBold', fontSize=12, leading=18, spaceBefore=9, spaceAfter=5, shaping=1),
    'p': ParagraphStyle('p', fontName='Thai', fontSize=9.3, leading=14.8, spaceAfter=6, shaping=1),
    'small': ParagraphStyle('small', fontName='Thai', fontSize=7.7, leading=11.6, textColor=colors.HexColor('#4a5653'), spaceAfter=5, shaping=1),
}
story = []
W = A4[0] - 64


def para(text, style='p'):
    story.append(Paragraph(text, styles[style]))


def picture(name, max_height):
    from PIL import Image as PILImage
    p = ROOT / 'evidence' / name
    with PILImage.open(p) as im:
        width, height = im.size
    scale = min(W / width, max_height / height)
    story.append(Image(str(p), width=width*scale, height=height*scale, hAlign='CENTER'))


def footer(canvas, doc):
    canvas.saveState()
    canvas.setFont('Thai', 8)
    canvas.setFillColor(colors.HexColor('#59625e'))
    canvas.drawString(32, 22, '02739452 | 6621600828 | Recipe Atlas')
    canvas.drawRightString(A4[0]-32, 22, f'{doc.page} / 3')
    canvas.restoreState()


verification = json.loads((ROOT / 'evidence/verification.json').read_text(encoding='utf-8'))
assert verification.get('passed'), 'Need passing scheduled DAG evidence before authoring summary'
assert {'forkify_outage', 'wikibooks_outage'}.issubset({r['label'] for r in verification['runs']}), 'Need current three-source outage evidence'
assert verification['runs'][-1]['quality']['all_three_sources_available'], 'Final run must have three usable sources'
para('ระบบรวบรวมสูตรอาหารนานาชาติ', 'title')
para('และวิเคราะห์วัตถุดิบที่ใช้', 'h')
para('โชติณัช โสภาแปง · รหัสนิสิต 6621600828 · ลำดับที่ 5 · วิชา 02739452', 'small')
para('GitHub: <link href="https://github.com/Chotinat01/02739452-5-6621600828" color="#21624e">github.com/Chotinat01/02739452-5-6621600828</link>', 'small')
para('<b>ผลที่ตรวจได้จริง:</b> ทำครบองค์ประกอบระดับ 3 และมีหน้าสถานะ pipeline สำหรับโบนัส ดึงข้อมูลจริงผ่าน Airflow และ PostgreSQL รวม 290 สูตร จาก TheMealDB 100, Wikibooks 180 และ Forkify 10 สูตร โดยไม่ต้องสมัคร API key เพิ่ม', 'p')
picture('dashboard.png', 513)
para('ภาพ dashboard จากระบบจริง แสดง KPI กราฟวัตถุดิบ สัดส่วนแหล่งข้อมูล ตาราง และตัวกรอง อ่าน PostgreSQL โดยตรงด้วย role ที่มีสิทธิ์ SELECT เท่านั้น ตรวจวันที่ 23 ตุลาคม 2569', 'small')
para('ข้อมูลแต่ละสูตรมีชื่อ วัตถุดิบ หมวด ประเทศและเวลาปรุงเมื่อมีข้อมูล พร้อม URL ที่มา ค่าที่ไม่ทราบเก็บเป็น NULL ใช้ requests + BeautifulSoup สำหรับ Wikibooks ตรวจ robots.txt และหน่วงคำขอ ไม่ใช้ Selenium และไม่เก็บข้อมูลส่วนบุคคล', 'small')
story.append(PageBreak())

para('การทำงานของ DAG และผลทดสอบ', 'title')
para('TaskGroup สามแหล่งแยก extract → transform → load ทำงานขนาน รวมเป็น schema เดียวใน staging ก่อนตัดซ้ำและตรวจคุณภาพ แล้วเลือก publish หรือ quarantine_and_stop', 'p')
picture('airflow_graph.png', 277)
para('Graph view ของ run คืนระบบที่สำเร็จ แยกสามแหล่งเป็น mealdb, wikibooks และ forkify จากนั้นรวม ตรวจคุณภาพ และเลือก branch รอบนี้ source_status ยืนยันทั้งสามแหล่งมีข้อมูลจริง', 'small')
picture('airflow_grid.png', 277)
para('Grid view แสดง extract transform load ของแต่ละกลุ่มและประวัติรอบทดสอบ แดงคือรอบที่จงใจทำ quality ไม่ผ่าน ชมพูคือ task ที่ถูก skip ตาม branch', 'small')
para('UPSERT ใช้ canonical_id จากชื่อและส่วนผสมที่ normalize รวมข้ามแหล่งเฉพาะข้อมูลตรงกัน เก็บ provenance สูตรชื่อเหมือนแต่ปริมาณต่างกันยังแยกกัน เมื่อ quality ผ่านจึงเปลี่ยน publication ใน transaction เดียว ข้อมูลเก่าที่ไม่อยู่ใน run ล่าสุดไม่ถูกนับปน', 'small')
story.append(PageBreak())

para('ผลตรวจ ปัญหา และการจัดลำดับงาน', 'title')
para('หลักฐานทดสอบจริง', 'h')
rows = [[Paragraph(t, styles['small']) for t in row] for row in [
    ['กรณีทดสอบ', 'ผลที่ตรวจได้'],
    ['Unit และ PostgreSQL integration', 'ผ่าน 8 tests รวม parser, cache, cross-source dedupe และ UPSERT'],
    ['รัน DAG ซ้ำ', 'canonical 290 → 290 แถว ไม่เพิ่มซ้ำ'],
    ['จำลองแหล่งล่มทีละแหล่ง', 'TheMealDB ล่มเหลือ 190, Forkify ล่ม 280, Wikibooks ล่ม 110 สูตร'],
    ['บังคับคุณภาพไม่ผ่าน', 'DAG failed ตามคาด pointer เดิมและ current 190 คงอยู่'],
    ['คืนระบบและตรวจเว็บ', 'กลับเป็น 290 สูตรจากสามแหล่ง ตัวกรองและหน้า pipeline ผ่าน AppTest'],
]]
table = Table(rows, colWidths=[173, W-173])
table.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#e6eee9')),
                          ('GRID',(0,0),(-1,-1),.5,colors.HexColor('#d9d9d9')),
                          ('VALIGN',(0,0),(-1,-1),'MIDDLE'),('LEFTPADDING',(0,0),(-1,-1),8),
                          ('RIGHTPADDING',(0,0),(-1,-1),8),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),4)]))
story.append(table)
para('ปัญหาที่พบและวิธีแก้', 'h')
para('<b>1. Airflow เขียน cache ไม่ได้</b><br/>อาการ: extract task ขึ้น PermissionError ที่ /opt/airflow/data/http_cache<br/>หาสาเหตุ: อ่าน task log พบว่า named volume เป็นของ root แต่ Airflow ใช้ UID 50000<br/>แก้ไข: เพิ่ม volume-init ตั้ง owner ก่อนเริ่มระบบ แล้วทดสอบ DAG ใหม่จนเผยแพร่สำเร็จ')
para('<b>2. API response ขาดกลางทาง</b><br/>อาการ: TheMealDB ขึ้น ChunkedEncodingError ทำให้รอบทดสอบแรกไม่มีข้อมูล usable<br/>หาสาเหตุ: ตรวจ source_status.detail และ task log พบว่า network ล้มเหลวก่อน parse JSON<br/>แก้ไข: เพิ่ม retry สูงสุด 3 attempts พร้อม backoff หากยังล่มบันทึก unavailable ให้แหล่งอื่นทำต่อ ทดสอบรอบปกติและรอบคืนระบบได้ 100 สูตรจาก API')
para('<b>การปรับแหล่งข้อมูล:</b> เปลี่ยน Spoonacular ที่ต้องใช้ key เป็น Forkify ซึ่ง GET ไม่ใช้ key จำกัดดึง 10 สูตรและเก็บ cache 1 ชั่วโมงตามข้อจำกัด 100 คำขอ/ชั่วโมง ต้องแก้แหล่งที่สามในโพสต์จองให้ตรงกัน การตัดซ้ำข้ามแหล่งผ่าน unit test แต่ชุดจริง 290 สูตรไม่พบ duplicate ตามกฎนี้', 'small')
para('แผนจัดเวลาสามชั่วโมง', 'h')
para('นาที 0–20 ตรวจโจทย์และแหล่งข้อมูล · 20–70 ทำ extract/transform · 70–110 ทำ PostgreSQL และ DAG · 110–145 ทำ dashboard · 145–180 ทดสอบ ถ่ายภาพ สรุป PDF และตรวจชุดส่ง โดยเป็นแผนจัดสรรเวลา ไม่ใช่บันทึกเวลาสอบจริง')
para('ให้ความสำคัญกับระบบที่รันได้ระดับ 2 ก่อน แล้วเพิ่ม fault tolerance และ quality branch ตัดงานโภชนาการ ระบบสมาชิก fuzzy matching และ Dynamic Task Mapping ออก เพราะไม่จำเป็นต่อแกนโจทย์และเพิ่มภาระทดสอบ ใช้หน้าสถานะ pipeline เป็นงานโบนัส')
para('หลักฐาน: evidence/verification.json, evidence/live_sources.json และ docs/TESTING.md ใน repo ผู้ส่งต้องตรวจ repo เป็น public และ commit ก่อนกำหนด พร้อมส่งลิงก์และ PDF ใน Classroom เอง', 'small')

out = ROOT / 'output/pdf/5_6621600828_summary.pdf'
out.parent.mkdir(parents=True, exist_ok=True)
doc = SimpleDocTemplate(str(out), pagesize=A4, rightMargin=32, leftMargin=32, topMargin=27, bottomMargin=37,
                        title='ระบบรวบรวมสูตรอาหารนานาชาติและวิเคราะห์วัตถุดิบที่ใช้', author='โชติณัช โสภาแปง')
doc.build(story, onFirstPage=footer, onLaterPages=footer)
print(out)
