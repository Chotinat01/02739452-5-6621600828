# ตรวจเทียบข้อสอบก่อนส่ง

ตรวจจากไฟล์ final-02739452-2569.docx ที่ผู้ใช้ส่งใหม่ วันที่ตรวจ 23 ตุลาคม 2569 ไม่ถือว่าได้ยืนยันวันสอบจริงหรือคะแนนจากอาจารย์

## สิ่งที่ต้องส่งใน Classroom

1. ลิงก์ public repository https://github.com/Chotinat01/02739452-5-6621600828
2. ไฟล์ output/pdf/5_6621600828_summary.pdf จำนวน 3 หน้า

ZIP เป็นไฟล์สำรองที่เตรียมเพิ่ม ไม่ใช่สิ่งที่โจทย์บังคับให้ส่งแทน repo และ PDF

## รายการตรวจ

| ข้อกำหนดในโจทย์ | ผลตรวจและหลักฐาน |
|---|---|
| ข้อมูลเรื่องเดียวกันจากหลายแหล่ง | สูตรอาหารและวัตถุดิบจาก TheMealDB, Wikibooks Cookbook และ Forkify |
| Airflow extract → transform → load → PostgreSQL | มี DAG และ TaskGroup แยกสามแหล่ง มี staging และ canonical ใน PostgreSQL |
| เว็บมีตารางและกราฟ | มีตารางสูตร กราฟวัตถุดิบ และกราฟแหล่งข้อมูล |
| อย่างน้อยสองแหล่ง ทำงานขนานด้วย TaskGroup | ทั้งสามกลุ่มเริ่มจาก begin โดยไม่มี dependency ข้ามกลุ่ม |
| Schema เดียวและ UPSERT รันซ้ำแถวไม่เพิ่ม | การทดสอบ scheduler จริง canonical 290 → 290 และมี PostgreSQL integration test |
| KPI และตัวกรองบนเว็บ | จำนวนสูตร แหล่งข้อมูล วัตถุดิบ หมวด และตัวกรองชื่อ/วัตถุดิบ/ประเภท/แหล่ง |
| สามแหล่งและทนแหล่งล่ม | ทดสอบล่มทีละแหล่ง เหลือ 190, 280 และ 110 สูตร โดย DAG success |
| ตัดซ้ำข้ามแหล่ง | fingerprint ชื่อ+ส่วนผสม เก็บ provenance ทดสอบข้อมูลซ้ำจากทั้งสามแหล่งแล้วรวมได้ ชุดจริง 290 สูตรไม่มีรายการซ้ำตามกฎนี้ |
| ตรวจคุณภาพและแตกเส้นทางเมื่อไม่ผ่าน | BranchPythonOperator เลือก publish หรือ quarantine_and_stop ทดสอบ fail แล้ว publication เดิมไม่เปลี่ยน |
| โบนัสหน้าสถานะ pipeline หรือ Dynamic Task Mapping | ใช้หน้าสถานะ pipeline จึงไม่จำเป็นต้องทำ Dynamic Task Mapping เพิ่ม |
| Scraping อย่างน้อยหนึ่งแหล่ง ห้าม Selenium | Wikibooks ใช้ requests + BeautifulSoup ไม่มี Selenium ใน dependencies |
| เคารพ robots.txt ไม่เก็บข้อมูลส่วนบุคคล | ตรวจ robots ก่อนดึง HTML หน่วงคำขอ ฟิลด์เผยแพร่ไม่มีชื่อผู้เขียน/ผู้ใช้/ข้อมูลผู้รีวิว |
| ข้อมูลรวมอย่างน้อย 100 รายการ | publication ปัจจุบัน 290 สูตรหลังตัดซ้ำ |
| Dashboard อ่าน PostgreSQL โดยตรง | psycopg2 ใช้ role recipe_reader ที่มี SELECT ใน schema recipes ไม่มี CSV/JSON fallback |
| ชื่อ repo ตามรูปแบบ | 02739452-5-6621600828 ใช้ลำดับที่ 5 และรหัส 6621600828 ตรงกับ PDF |
| Repo เป็น public | ตรวจ GitHub API ได้ private=false |
| dags/ web/ docker-compose.yml requirements.txt | มีครบใน tracked files และ clone ใหม่จาก GitHub |
| README หัวข้อ แหล่งข้อมูล วิธีรัน | มีครบ รวม setup, Docker Compose, Trigger DAG และวิธีดูผล |
| ห้าม commit รหัสผ่าน/API key และต้องมี .env.example | มี .env.example ไม่พบ .env ในประวัติ Git และตรวจค่า credential ที่ใช้อยู่กับทุก Git blob แล้วไม่พบ |
| PDF ไม่เกินสามหน้า | PdfReader/pdfinfo ตรวจได้ 3 หน้า และตรวจภาพแต่ละหน้าแล้ว |
| PDF ระบุระดับ ภาพ Graph/Grid/dashboard และ repo | มีครบ ใช้ภาพจากระบบจริง |
| PDF ปัญหาอย่างน้อยสองข้อ มีอาการ หาสาเหตุ วิธีแก้ | PermissionError ของ volume และ ChunkedEncodingError ของ API มีครบทั้งสามส่วน |
| PDF แบ่งเวลาและตัดงานพร้อมเหตุผล | มีแผน 3 ชั่วโมง และเหตุผลตัดงานโภชนาการ สมาชิก fuzzy matching และ Dynamic Task Mapping ระบุว่าเป็นแผน ไม่แต่งเวลาสอบจริง |
| ชื่อ PDF ตามรูปแบบ | 5_6621600828_summary.pdf ตรงกับลำดับและรหัสใน repo |
| หมายเหตุอาจารย์รันจาก repo | clone GitHub ใหม่ เริ่มฐานข้อมูล 0 สูตรและ data volume ใหม่ ดึงครบ 290 สูตรทั้งสามแหล่ง cached_pages=0 ทุกแหล่ง tests 8 ข้อและ AppTest ผ่าน ผลอยู่ evidence/clean_clone_audit.json |

## สิ่งที่ต้องตรวจโดยผู้ส่ง

- โพสต์จองต้องไม่ซ้ำขอบเขตกับคนที่จองก่อน เราไม่ได้ตรวจกลุ่ม LINE แทนผู้ใช้
- เติมหมู่ในโพสต์จอง และแก้แหล่งที่สามจาก Spoonacular เป็น Forkify ตาม docs/TOPIC_RESERVATION.md
- โจทย์ยกตัวอย่างลำดับแบบ 01 แต่ไม่ได้เขียนบังคับว่าต้องสองหลัก ชุดนี้ใช้เลข 5 ตรงกันทั้ง repo และ PDF ตาม repo ที่ผู้ใช้ให้ หากอาจารย์มีคำสั่งเพิ่มเติมเรื่องสองหลัก ต้องทำตามคำสั่งนั้น
- ส่งลิงก์ repo และ PDF ภายใน 16:00 ของวันสอบ และตรวจว่า commit ที่จะให้ตรวจเกิดก่อน 16:00 ด้วย ห้ามเปลี่ยนหรือย้อนหลังเวลา commit
- ยังไม่ได้ส่ง Classroom แทนผู้ใช้

## ขอบเขตผลตรวจความลับ

scripts/audit_submission.py ตรวจไฟล์บังคับและ PDF ตรวจ path ประวัติ Git ว่าไม่มี .env และตรวจค่า password/secret/key ที่ใช้อยู่ใน .env กับ Git blob ทุกตัว ผลอยู่ evidence/submission_audit.json การตรวจนี้ไม่ใช่การรับรองว่าเครื่องมือค้นหา secret ทุกชนิดที่ไม่รู้จักล่วงหน้าครบทั้งหมด

การทดสอบ clean clone ใช้ Docker image build cache ที่มีในเครื่อง แต่สร้างฐานข้อมูลและ data volume ใหม่ทั้งหมด ไม่คัดลอก HTML/API cache จากระบบเดิม เปลี่ยนพอร์ตเป็น 18080/18501 เพื่อไม่ชนระบบที่ผู้ใช้เปิด และจำกัด worker สำหรับชุดตรวจแยกเพื่อประหยัด RAM โค้ดแอปเป็น commit d0b19e6176ff5972d5fd2c4897070316e682f3f1 ที่ clone จาก GitHub
