# ผลทดสอบจริง

ตรวจวันที่ 23 ตุลาคม 2569 ใช้ Docker Desktop บน Windows, Airflow 2.10.4 Python 3.11, PostgreSQL 16 และ Streamlit 1.41.1

## ข้อมูลจากต้นทาง

- TheMealDB: 100 สูตร ผ่าน validation 100 และ unique 100
- Wikibooks: 180 สูตร ผ่าน validation 180 และ unique 180 ข้าม 8 หน้าเนื่องจากไม่มีส่วน Ingredients ซึ่งมักเป็นหน้ารวมหรือสารบัญ
- Spoonacular: unavailable เนื่องจากผู้ใช้ยังไม่มี API key ไม่ได้ใช้ข้อมูลตัวอย่างแทนผลจริง
- รวมผ่าน DAG baseline: 280 สูตร กฎ hash ชื่อและส่วนผสมไม่พบรายการซ้ำในชุดนี้ การรวมข้ามแหล่งพิสูจน์ด้วย unit test แยกต่างหาก
- cache Wikibooks เก็บจาก HTTP จริงในวันทดสอบ แล้วนำเข้า volume เพื่อไม่เรียก HTML ซ้ำโดยไม่จำเป็น DAG ยังตรวจ robots.txt ใหม่ทุก run

หลักฐาน machine-readable: `evidence/live_sources.json` และ `evidence/verification.json`

## การทดสอบโปรแกรม

`docker compose exec -T airflow-scheduler python -m pytest /opt/airflow/project/tests -q` ผ่าน 6 tests บน PostgreSQL จริง ครอบคลุม parser, validation, conservative cross-source dedupe, quality threshold, UPSERT, stage retry และการเก็บ publication เดิมเมื่อ quality fail การทดสอบ DB ใช้ transaction แล้ว rollback จึงไม่ปะปน test fixture ในชุดเผยแพร่

`airflow dags list-import-errors` ไม่พบ import error

รอบสุดท้าย `final_categories` เผยแพร่ 280 สูตรสำเร็จ หลังปรับ parser ให้เลือกหมวดอาหาร `Recipes for ...` แทนหมวดระดับความยาก พบหมวดอาหาร 56 หมวด และรัน 6 tests ผ่านอีกครั้ง

Streamlit AppTest ต่อ PostgreSQL ด้วย role สำหรับอ่าน ผ่าน KPI 280, ค้น apple, กรองคำที่ไม่มีผลให้เหลือ 0 และเปิดหน้า Pipeline status โดยไม่มี exception

## การทดสอบ DAG ผ่าน scheduler จริง

| กรณี | Run ID | ผล Airflow | ผลในฐานข้อมูล |
|---|---|---|---|
| baseline | live_baseline | success | เผยแพร่ 280 สูตร |
| รันซ้ำ | verify_repeat_20261023T065230 | success | canonical 280 → 280 และ current 280 |
| TheMealDB ล่ม | verify_mealdb_outage_20261023T065230 | success | current 180 จาก Wikibooks canonical ยัง 280 |
| บังคับ quality fail | verify_quality_failure_20261023T065230 | failed ตามที่คาด | current ยัง 180 และ pointer ไม่เปลี่ยน |
| คืนระบบ | verify_restore_20261023T065230 | success | current กลับเป็น 280 |

ในกรณี quality fail ข้อมูล staging ผ่าน 280 สูตร แต่ flag ทำให้ quality gate ไม่ผ่าน จึงไม่เผยแพร่ ไม่ควรตีความว่า unique_count ใน quality report เท่ากับข้อมูลที่เผยแพร่เสมอ

ตอนเริ่มเคยจำลอง Wikibooks ล่มใน run `test_wiki_outage` แล้ว TheMealDB พบ ChunkedEncodingError พร้อมกัน จึงไม่มีแหล่ง usable และ DAG เข้า quality_failed ถูกต้อง กรณีนี้ไม่ใช่หลักฐานว่ารอดจาก Wikibooks ล่มเพียงแหล่งเดียว หลังจากนั้นเพิ่ม network retry และทดสอบกรณี TheMealDB ล่มโดย Wikibooks ยังใช้ได้สำเร็จ

## ปัญหาที่พบจริงและวิธีแก้

1. Airflow extract task เขียน `/opt/airflow/data/http_cache` ไม่ได้ ข้อความ `PermissionError: [Errno 13] Permission denied` ตรวจ task log พบว่า named volume เป็นของ root แก้ด้วย service `volume-init` ตั้ง owner เป็น UID 50000 ก่อน airflow-init แล้วรัน task ซ้ำสำเร็จ
2. TheMealDB response ขาดกลางทางเป็น `ChunkedEncodingError` ตรวจ `source_status.detail` และ task log แล้วพบว่าเกิดก่อน parse JSON เพิ่ม retry สำหรับ requests.RequestException สูงสุด 3 attempts พร้อม backoff 2 และ 4 วินาที หากยังล้มเหลวเก็บ unavailable ให้แหล่งอื่นทำต่อ ทดสอบรอบปกติและคืนระบบได้ 100 สูตรจาก API หลังแก้
3. Spoonacular ไม่มี API key จึงแสดง missing_api_key ตามจริง ระบบใช้งานได้สองแหล่ง แต่ยังไม่ยืนยันระดับ 3 ครบ ต้องสมัคร key ใส่ใน .env และทดสอบใหม่

## ทดสอบซ้ำเอง

Trigger DAG ปกติ รอ published แล้วตรวจ Pipeline status ก่อนและหลัง ถ้าต้องการจำลองแหล่งล่ม ให้ใส่ config `{"fail_sources":["mealdb"]}` ถ้าต้องการจำลอง quality ไม่ผ่าน ให้ใส่ `{"force_quality_failure":true}` จากนั้นตรวจว่า run failed แต่ dashboard ยังแสดง publication เดิม

`scripts/verify_runs.py` เป็นสคริปต์ตรวจ snapshot ชุดทดสอบนี้ ใช้ expected counts 280 และ 180 ซึ่งอาจต้องปรับเมื่อข้อมูลต้นทางเปลี่ยน หรือมี Spoonacular key แล้ว ไม่ใช่เงื่อนไขถาวรของ production pipeline

ภาพจริงอยู่ใน `evidence/airflow_graph.png`, `airflow_grid.png`, `dashboard.png`, `pipeline_status.png` Grid อาจเห็นแดงในรอบที่จงใจทดสอบ quality failure และชมพูหมายถึง skipped ไม่ใช่ข้อผิดพลาด

## ขอบเขตที่ยังไม่ได้ทดสอบ

ยังไม่ได้ทดสอบการดึงข้อมูล Spoonacular ด้วย key จริงหรือโควตาจริง และยังไม่ได้ส่ง Classroom ไม่มีการรับรองคะแนนหรือเวลา commit เทียบกำหนดสอบ

อัปโหลดโค้ดและ PDF ไปยัง branch main ของ repo ที่ผู้ใช้ให้แล้ว ตรวจ GitHub API แบบไม่ใช้ token ได้ `private: false` และตรวจ remote commit ตรงกับ local ก่อนส่งมอบ
