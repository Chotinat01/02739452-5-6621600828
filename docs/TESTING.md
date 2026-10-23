# ผลทดสอบจริงฉบับสามแหล่ง

ตรวจวันที่ 23 ตุลาคม 2569 บน Docker Desktop, Airflow 2.10.4, PostgreSQL 16 และ Streamlit 1.41.1 ผู้ใช้เลือกเปลี่ยนแหล่งที่สามจาก Spoonacular เป็น Forkify ซึ่งอ่านด้วย GET ได้โดยไม่ต้องใช้ key

## ข้อมูลจริงที่ใช้

- TheMealDB 100 สูตร ผ่าน validation 100 และ unique 100
- Wikibooks 180 สูตร ผ่าน validation 180 และ unique 180 ข้าม 8 หน้าที่ไม่มีส่วน Ingredients
- Forkify 10 สูตร ผ่าน validation 10 และ unique 10 จาก GET search=pizza แล้วอ่านรายละเอียดตาม id ไม่เก็บชื่อ publisher หรือข้อมูลผู้ใช้
- รวม 290 สูตร ไม่พบ cross-source duplicate ตามกฎ fingerprint ของชุดจริงนี้ กรณีซ้ำข้ามแหล่งทดสอบแยกใน unit test
- HTML Wikibooks เก็บจาก HTTP จริงและ cache 24 ชั่วโมง DAG ตรวจ robots.txt ใหม่ เว้นคำขออย่างน้อย 1.1 วินาที
- Forkify จำกัด 100 requests/hour หนึ่งรอบที่ไม่มี cache ใช้ 11 requests ในกรณีปกติ เก็บ payload 1 ชั่วโมงแล้วใช้ซ้ำโดยคง fetched_at เดิม หลีกเลี่ยง refresh ซ้ำถี่ ๆ การ retry อาจเพิ่มคำขอ

หลักฐานล่าสุดอยู่ที่ evidence/live_sources.json และ evidence/verification.json ประวัติเดิมที่ใช้สองแหล่งยังอยู่ใน Airflow และประวัติ Git แต่ไม่ใช่ผลรอบปัจจุบัน

## การทดสอบโปรแกรม

ผ่าน 8 tests ด้วย pytest ใน Airflow container รวม parser, validation, conservative cross-source dedupe, quality threshold, การแปลง Forkify ที่ไม่มีปริมาณ, cache ที่ไม่เรียก HTTP ซ้ำ และ integration test PostgreSQL จริงสำหรับ UPSERT, staging และการเก็บ publication เดิม การทดสอบ DB ใช้ transaction แล้ว rollback จึงไม่ปะปน fixture ในชุดเผยแพร่

ไม่พบ DAG import error และ Streamlit AppTest ตรวจจำนวนอย่างน้อย 100 สูตร, แหล่งข้อมูล 3/3, ค้น apple, ตัวกรองไม่มีผลลัพธ์ และหน้าสถานะ pipeline โดยไม่มี exception

## DAG ผ่าน scheduler จริง

ชุด verification วันที่ 23 ตุลาคม 2569 เวลา 07:24:56 UTC (14:24:56 น. กรุงเทพฯ)

| กรณี | Run ID | ผล Airflow | ข้อมูลเผยแพร่ |
|---|---|---|---|
| baseline | forkify_baseline | success | 290 สูตร สามแหล่งใช้งานได้ |
| รันซ้ำ | verify_repeat_20261023T072456 | success | canonical 290 → 290 และ current 290 |
| TheMealDB ล่ม | verify_mealdb_outage_20261023T072456 | success | current 190 จาก Wikibooks + Forkify |
| บังคับ quality fail | verify_quality_failure_20261023T072456 | failed ตามที่คาด | pointer และ current 190 คงเดิม |
| Forkify ล่ม | verify_forkify_outage_20261023T072456 | success | current 280 จาก TheMealDB + Wikibooks |
| Wikibooks ล่ม | verify_wikibooks_outage_20261023T072456 | success | current 110 จาก TheMealDB + Forkify |
| คืนระบบ | verify_restore_20261023T072456 | success | กลับเป็น 290 สูตร สามแหล่งใช้งานได้ |

ในกรณี quality fail staging มีข้อมูลผ่าน 290 สูตร แต่ flag ทำให้ quality gate ไม่ผ่าน จึงไม่เผยแพร่ unique_count ใน quality report จึงไม่จำเป็นต้องเท่าจำนวนที่กำลังเผยแพร่

## ปัญหาที่พบจริง

1. Airflow เขียน /opt/airflow/data/http_cache ไม่ได้ พบ PermissionError ใน task log เพราะ named volume เป็นของ root แต่ Airflow ใช้ UID 50000 แก้ด้วย service volume-init ตั้ง owner ก่อน airflow-init แล้วรันผ่าน
2. TheMealDB response ขาดกลางทางเป็น ChunkedEncodingError ตรวจ source_status.detail และ log พบ network ล้มเหลวก่อน parse JSON เพิ่ม retry สูงสุด 3 attempts พร้อม backoff 2 และ 4 วินาที หากยังล่มให้ unavailable แล้วแหล่งอื่นทำต่อ ผลรอบปกติได้ 100 สูตร
3. Spoonacular ต้องใช้ key แต่ผู้ใช้ยังไม่มี ผู้ใช้เลือกเปลี่ยนเป็น Forkify ตรวจเอกสารและ GET จริงแล้วได้รายละเอียดสูตร จึงแก้ extractor, DAG, dashboard และทดสอบสามแหล่งใหม่ครบ ต้องแก้โพสต์จองให้ตรงตาม docs/TOPIC_RESERVATION.md
4. ธีมมืดของ Streamlit ชนกับ CSS พื้นสว่าง ทำให้ตัวเลขและชื่อกราฟซีด แก้ธีมเริ่มต้นเป็น light เอาการฝืนสีพื้นออก และกำหนด Plotly เป็นพื้นขาวพร้อมข้อความเข้ม ตรวจภาพจริงแล้ว

## วิธีตรวจซ้ำ

รัน docker compose exec airflow-scheduler python -m pytest /opt/airflow/project/tests -q แล้ว Trigger DAG ปกติ ตรวจ source_status ว่าครบทั้งสามและ current_recipes อย่างน้อย 100 สูตร ทดสอบแต่ละแหล่งด้วย config {"fail_sources":["mealdb"]}, {"fail_sources":["wikibooks"]} หรือ {"fail_sources":["forkify"]} และทดสอบคุณภาพด้วย {"force_quality_failure":true}

scripts/verify_runs.py ตรวจ baseline สามแหล่ง รันซ้ำ แหล่งล่มทีละแหล่ง quality fail และคืนระบบ โดยใช้จำนวน baseline ที่เผยแพร่จริงของการทดสอบครั้งนั้น ต้องรอให้ baseline ปกติเผยแพร่ก่อนรัน script

ภาพ Graph, Grid, dashboard และ pipeline ใน evidence เป็นภาพจริงของระบบนี้ PDF สร้างจากภาพชุดล่าสุด

## สิ่งที่ผู้ใช้ต้องทำในการส่ง

Repo ถูกตั้ง public และอัปโหลด branch main แล้ว ผู้ใช้ต้องแก้แหล่งที่สามในโพสต์จองและส่ง repo กับ PDF ทาง Classroom เอง ยังไม่มีการส่ง Classroom แทน และไม่รับรองคะแนนหรือกำหนดสอบจริง
