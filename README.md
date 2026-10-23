# Thai Advertising & Marketing News Pipeline (02739452)

6621653697 เจษฎากร นันทสุภาวัฒนา sec.820

โปรเจกต์รวบรวมข่าวการตลาดและโฆษณาไทยจากหลายเว็บ ด้วย **Apache Airflow** → **PostgreSQL** → **Streamlit dashboard**

## หัวข้อและขอบเขต
- หมวด: ข่าวการตลาดและโฆษณา
- ขอบเขต: บทความข่าวการตลาด/แคมเปญโฆษณาของแบรนด์ไทยล่าสุด (เก็บเฉพาะ title, URL, วันที่, หมวด; ตรวจจับชื่อแบรนด์จาก title)
- **ไม่เก็บข้อมูลส่วนบุคคล** (ไม่เก็บชื่อผู้เขียน/ผู้ใช้) และปฏิบัติตาม robots.txt (หน้า `/page/` ของ Marketing Oops ถูก disallow จึงไม่เข้า)

## แหล่งข้อมูล
| # | แหล่ง | วิธีดึง |
|---|---|---|
| 1 | Marketing Oops (marketingoops.com) | WordPress REST API สาธารณะ `/wp-json/wp/v2/posts` |
| 2 | Positioning Magazine (positioningmag.com) | Web scraping (requests + BeautifulSoup, ไม่ใช้ Selenium) |
| 3 | Brand Inside (brandinside.asia) | Web scraping (requests + BeautifulSoup, ไม่ใช้ Selenium) |

## โครงสร้าง DAG `ad_news_pipeline`
```
create_tables → TaskGroup extract (3 แหล่งรันขนานกัน)
              → merge_transform (normalize + detect brand + dedup ข้ามแหล่ง)
              → quality_gate (BranchPythonOperator)
                   ├─ pass → load_to_postgres (UPSERT)
                   └─ fail → quarantine
              → done
```
- ระดับ 1: extract → transform → load → PostgreSQL → เว็บแสดงตาราง + กราฟ
- ระดับ 2: 3 แหล่งใน TaskGroup, schema เดียวกัน, UPSERT (`ON CONFLICT (url)`), เว็บมี KPI + ตัวกรอง
- ระดับ 3: แหล่งใดล่ม → `merge_transform` ใช้ `trigger_rule=all_done` รวมข้อมูลจากแหล่งที่เหลือ, dedup ข้ามแหล่ง (URL / title ซ้ำ / ใกล้เคียง ≥ 90%), quality gate แตกเส้นทาง
- โบนัส: แท็บ **Pipeline status** บนเว็บ (อ่านจากตาราง `pipeline_runs`)

## วิธีรัน
ต้องมี Docker + Docker Compose
```bash
cp .env.example .env
docker compose up -d --build
```
- Airflow UI: http://localhost:8080 (user/password ตามใน `.env`) — DAG `ad_news_pipeline` จะ unpause อัตโนมัติ กด **Trigger DAG** ได้เลย (รอ ~1–2 นาทีให้ webserver ขึ้น และรอบแรกใช้เวลา ~2 นาทีเพราะมี delay ระหว่าง request)
- Dashboard: http://localhost:8501
- ตรวจจำนวนแถว: `docker compose exec postgres psql -U adnews -d adnews -c "SELECT source, COUNT(*) FROM ad_news GROUP BY 1;"`
- ทดสอบ UPSERT: trigger DAG ซ้ำ จำนวนแถวต้องไม่เพิ่ม (ยกเว้นมีบทความใหม่จริง)

## ทดสอบกรณีพิเศษ
```bash
# แหล่งล่ม → DAG ยังรวมข้อมูลจากแหล่งที่เหลือ
SIMULATE_DOWN=positioning docker compose up -d airflow
# บังคับให้ quality gate ไม่ผ่าน → เข้าเส้นทาง quarantine
QUALITY_MIN_ROWS=9999 docker compose up -d airflow
```

## โครงสร้างไฟล์
```
dags/ad_news_pipeline.py   # DAG
dags/scrapers.py           # extract / transform / quality functions
web/app.py                 # Streamlit dashboard (อ่าน PostgreSQL โดยตรง)
docker-compose.yml  requirements.txt  .env.example
```
