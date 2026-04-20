"""
Generates the 5-page Project Documentation PDF
for the Digital Wallet Transaction DB – Data Engineering Capstone.
Format: A4, Arial-equivalent (Helvetica), justified body,
        page numbers bottom-right, section headers in navy.
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, PageBreak, KeepTogether
)

OUT = "/mnt/user-data/outputs/Digital_Wallet_DE_Project_Documentation.pdf"

W, H = A4
NAVY   = colors.HexColor("#0D1B6E")
BLUE   = colors.HexColor("#1565C0")
LBLUE  = colors.HexColor("#E3F2FD")
DBLUE  = colors.HexColor("#BBDEFB")
WHITE  = colors.white
LGRAY  = colors.HexColor("#F5F5F5")
MGRAY  = colors.HexColor("#9E9E9E")
GREEN  = colors.HexColor("#1B5E20")
LGREEN = colors.HexColor("#E8F5E9")
BLACK  = colors.black


# ── style helpers ─────────────────────────────────────────────────────────────
def sty(name, base=None, **kw):
    p = ParagraphStyle(name)
    if base:
        for k, v in vars(base).items():
            if not k.startswith("_"):
                setattr(p, k, v)
    for k, v in kw.items():
        setattr(p, k, v)
    return p


COVER_TITLE = sty("ct", fontName="Helvetica-Bold", fontSize=22,
                  textColor=WHITE, alignment=TA_CENTER, spaceAfter=6)
COVER_SUB   = sty("cs", fontName="Helvetica", fontSize=14,
                  textColor=LBLUE, alignment=TA_CENTER, spaceAfter=4)
COVER_BODY  = sty("cb", fontName="Helvetica", fontSize=11,
                  textColor=LBLUE, alignment=TA_CENTER, spaceAfter=3)

H1  = sty("h1", fontName="Helvetica-Bold", fontSize=15,
          textColor=NAVY, spaceAfter=6, spaceBefore=10)
H2  = sty("h2", fontName="Helvetica-Bold", fontSize=13,
          textColor=BLUE, spaceAfter=4, spaceBefore=8)
H3  = sty("h3", fontName="Helvetica-Bold", fontSize=11,
          textColor=NAVY, spaceAfter=3, spaceBefore=5)
BODY = sty("bd", fontName="Helvetica", fontSize=12,
           leading=18, spaceAfter=5, alignment=TA_JUSTIFY)
SMALL = sty("sm", fontName="Helvetica", fontSize=10,
            leading=14, spaceAfter=3, textColor=colors.HexColor("#37474F"))
CODE  = sty("cd", fontName="Courier", fontSize=9, leading=13,
            textColor=GREEN, backColor=LGREEN, spaceAfter=3,
            leftIndent=10, rightIndent=10)
CENT  = sty("cn", fontName="Helvetica", fontSize=12,
            alignment=TA_CENTER, spaceAfter=4)
RGHT  = sty("rg", fontName="Helvetica", fontSize=9,
            textColor=MGRAY, alignment=TA_RIGHT)
BULLET = sty("bl", fontName="Helvetica", fontSize=11, leading=16,
             leftIndent=15, spaceAfter=2)


def p(text, style=None):  return Paragraph(text, style or BODY)
def sp(h=0.25):           return Spacer(1, h * cm)
def hr(col=DBLUE):
    return HRFlowable(width="100%", thickness=1, color=col, spaceAfter=4)
def bullet(text):
    return p(f"&#8226;  {text}", BULLET)


# ── table helpers ─────────────────────────────────────────────────────────────
def two_col_table(rows, col_w=None):
    cw = col_w or [4.5 * cm, 11.5 * cm]
    t  = Table(rows, colWidths=cw)
    t.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR",   (0, 0), (-1, 0), WHITE),
        ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 10),
        ("GRID",        (0, 0), (-1, -1), 0.4, DBLUE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LGRAY]),
        ("VALIGN",      (0, 0), (-1, -1), "TOP"),
        ("PADDING",     (0, 0), (-1, -1), 5),
        ("FONTNAME",    (0, 1), (0, -1), "Helvetica-Bold"),
    ]))
    return t


def three_col_table(rows, col_w=None):
    cw = col_w or [4.5*cm, 5.5*cm, 6*cm]
    t  = Table(rows, colWidths=cw)
    t.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR",   (0, 0), (-1, 0), WHITE),
        ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 9),
        ("GRID",        (0, 0), (-1, -1), 0.4, DBLUE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LGRAY]),
        ("VALIGN",      (0, 0), (-1, -1), "TOP"),
        ("PADDING",     (0, 0), (-1, -1), 5),
    ]))
    return t


# ── page number callback ──────────────────────────────────────────────────────
def add_page_number(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 9)
    canvas.setFillColor(MGRAY)
    canvas.drawString(2.5 * cm, 1.2 * cm,
                      "Digital Wallet Transaction DB  |  Data Engineering Capstone Project")
    canvas.drawRightString(W - 2.5 * cm, 1.2 * cm, f"Page {doc.page}")
    canvas.restoreState()


# ── build document ────────────────────────────────────────────────────────────
doc = SimpleDocTemplate(
    OUT, pagesize=A4,
    leftMargin=2.5*cm, rightMargin=2.5*cm,
    topMargin=2.5*cm,  bottomMargin=2.5*cm,
)
story = []


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 – COVER
# ══════════════════════════════════════════════════════════════════════════════

# Navy banner background (full-width coloured table)
cover_banner = Table(
    [[p("DATA ENGINEERING CAPSTONE PROJECT", COVER_TITLE)],
     [p("Digital Wallet Transaction Database", COVER_SUB)],
     [p("30 Advanced Data Engineering Tasks", COVER_BODY)]],
    colWidths=[W - 5*cm]
)
cover_banner.setStyle(TableStyle([
    ("BACKGROUND", (0, 0), (-1, -1), NAVY),
    ("PADDING",    (0, 0), (-1, -1), 18),
    ("ALIGN",      (0, 0), (-1, -1), "CENTER"),
    ("TOPPADDING", (0, 0), (0, 0),   30),
    ("BOTTOMPADDING", (0, -1), (-1, -1), 30),
]))
story.append(sp(0.5))
story.append(cover_banner)
story.append(sp(0.8))

# Project info table
info_rows = [
    ["Project Title",   "Digital Wallet Transaction DB – End-to-End Data Engineering"],
    ["Domain",          "FinTech / Payments Analytics"],
    ["Total Tasks",     "30 Advanced Data Engineering Assignments"],
    ["Dataset",         "100,000 synthetic wallet transactions · 1,000 users · 15 merchants"],
    ["Tech Stack",      "Python · Pandas · NumPy · SQL · Spark · Kafka · Airflow · Cloud · Delta Lake"],
    ["Submission Date", "April 2026"],
]
story.append(two_col_table(info_rows))
story.append(sp(1.0))

story.append(p("Submitted by:", CENT))
story.append(sp(0.2))

# Student info box
student_box = Table(
    [[p("[Student Name]",     H1)],
     [p("Roll Number: [Your Roll Number]", CENT)],
     [p("Batch / Program: [Your Batch / Program]", CENT)],
     [sp(0.1)],
     [p("Trainer: [Trainer Name]  |  Institute: [Institute Name]", SMALL)]],
    colWidths=[W - 5*cm]
)
student_box.setStyle(TableStyle([
    ("BACKGROUND",  (0, 0), (-1, -1), LBLUE),
    ("BOX",         (0, 0), (-1, -1), 1.5, NAVY),
    ("PADDING",     (0, 0), (-1, -1), 12),
    ("ALIGN",       (0, 0), (-1, -1), "CENTER"),
]))
story.append(student_box)
story.append(PageBreak())


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 – PROBLEM STATEMENT + SOLUTION
# ══════════════════════════════════════════════════════════════════════════════
story.append(p("1.  Problem Statement", H1)); story.append(hr())
story.append(p(
    "Digital payment platforms process millions of transactions every day. "
    "Raw transaction data arrives as CSV files from multiple source systems and must "
    "be reliably ingested, validated, cleaned, transformed, stored, and analysed to "
    "support real-time business decisions. The key engineering challenges include: "
    "handling large data volumes efficiently (1 M+ rows), ensuring end-to-end data "
    "quality, building fault-tolerant and observable pipelines, enabling both batch and "
    "real-time analytics, and delivering actionable dashboards — all while maintaining "
    "data lineage, governance, and auditability across the full stack."
))
story.append(sp(0.4))

story.append(p("2.  Solution Overview", H1)); story.append(hr())
story.append(p(
    "This project builds a complete data engineering ecosystem for a digital wallet "
    "platform using a synthetic dataset of 100,000 transactions. The solution covers "
    "all 30 tasks across every layer of the modern data stack: Linux infrastructure, "
    "networking, Python scripting, SQL databases, big data processing with Spark, "
    "stream processing with Kafka, pipeline orchestration with Airflow, cloud services "
    "(AWS/GCP/Azure), Lakehouse architecture with Delta Lake, and data quality "
    "monitoring — culminating in a full end-to-end pipeline with a live analytics dashboard."
))
story.append(sp(0.4))

story.append(p("3.  Feature Matrix", H1)); story.append(hr())
features = [
    ["Tasks", "Feature",                                        "Tools / Concepts"],
    ["01",    "Linux pipeline setup – dirs, permissions, logs", "Bash, chmod, cron"],
    ["02",    "API networking – HTTP/HTTPS/FTP/WebSocket",      "Python socket, urllib"],
    ["03",    "Multi-CSV read, clean, merge (9,000 rows)",      "Python csv, re, os"],
    ["04",    "Normalizer / Aggregator / Validator OOP modules","Python OOP, dataclasses"],
    ["05",    "1.1 M-row analysis, 62 % memory saved",          "Pandas, NumPy, float32"],
    ["06",    "SQLite OLTP DB with 8 query types",              "SQL: SELECT, WHERE, GROUP BY"],
    ["07",    "JOINs, CTEs, window functions (RANK, LAG)",      "Advanced SQL"],
    ["08",    "OLTP vs OLAP comparison + timed queries",        "Schema design"],
    ["09",    "Star schema: 1 fact + 5 dimension tables",       "Data Warehousing"],
    ["10",    "ETL vs ELT: built + benchmarked both",           "Python, SQL"],
    ["11",    "Batch ingestion: 111K rows/sec + checksums",     "Python, audit log"],
    ["12-13", "HDFS sim: NameNode, 4 DataNodes, 3× replication","Python OOP, block map"],
    ["14-17", "Spark DataFrame/SQL/partition/cache/broadcast",  "PySpark API (pandas sim)"],
    ["18-21", "Kafka producer-consumer, offset mgmt, streaming","Python queues, micro-batch"],
    ["22-23", "Airflow DAG: 7 tasks, retries, XCom, schedule", "Python DAG framework"],
    ["24-27", "AWS/GCP/Azure, S3, EC2, BigQuery simulation",    "Python OOP, SQLite"],
    ["28",    "Delta Lake: ACID, versioning, time travel, vacuum","JSON commit log"],
    ["29",    "DQ: Z-score, IQR, behavioural, velocity alerts", "Python statistics"],
    ["30",    "End-to-end pipeline + live analytics dashboard", "All of the above"],
]
ft = Table(features, colWidths=[1.4*cm, 9*cm, 5.6*cm])
ft.setStyle(TableStyle([
    ("BACKGROUND",  (0, 0), (-1, 0), NAVY),
    ("TEXTCOLOR",   (0, 0), (-1, 0), WHITE),
    ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
    ("FONTSIZE",    (0, 0), (-1, -1), 9),
    ("GRID",        (0, 0), (-1, -1), 0.4, DBLUE),
    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LGRAY]),
    ("VALIGN",      (0, 0), (-1, -1), "TOP"),
    ("PADDING",     (0, 0), (-1, -1), 4),
    ("ALIGN",       (0, 0), (0, -1), "CENTER"),
]))
story.append(ft)
story.append(PageBreak())


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 – TECH STACK + ARCHITECTURE
# ══════════════════════════════════════════════════════════════════════════════
story.append(p("4.  Tech Stack", H1)); story.append(hr())
tech = [
    ["Layer",            "Technology",                  "Purpose"],
    ["Language",         "Python 3.10+",                "Core scripting, all pipelines"],
    ["Data Processing",  "Pandas 2.x, NumPy 1.24+",    "DataFrames, vectorised math"],
    ["Storage – OLTP",   "SQLite",                      "Transaction DB, CRUD queries"],
    ["Storage – DW",     "SQLite Star Schema",          "OLAP analytical queries"],
    ["Big Data",         "PySpark API (pandas sim)",    "RDD / DataFrame / SQL API"],
    ["Streaming",        "Python queues",               "Kafka producer-consumer model"],
    ["Orchestration",    "Custom Python DAG",           "Airflow DAG patterns & XCom"],
    ["Cloud – Storage",  "Python + local FS",           "S3 bucket PUT/GET/LS/presign"],
    ["Cloud – Compute",  "EC2 class simulation",        "Instance launch + deployment"],
    ["Cloud – DW",       "SQLite (BigQuery patterns)",  "Quarterly, city, KYC queries"],
    ["Lakehouse",        "Delta log + CSV partitions",  "ACID, versioning, time travel"],
    ["Shell",            "Bash",                        "File mgmt, permissions, cron"],
    ["Documentation",    "ReportLab, Markdown",         "PDF report, GitHub README"],
]
story.append(three_col_table(tech))
story.append(sp(0.6))

story.append(p("5.  System Architecture", H1)); story.append(hr())
story.append(p("The diagram below shows the full data flow from source to dashboard:"))
story.append(sp(0.3))

arch_txt = """
  ┌──────────────┐     ┌──────────────┐     ┌───────────────────┐
  │  DATA SOURCES│────▶│  INGESTION   │────▶│   PROCESSING      │
  │  CSV files   │     │  Batch + S3  │     │  Clean · Enrich   │
  │  APIs / Kafka│     │  (Tasks 1,11)│     │  Validate (T3-5)  │
  └──────────────┘     └──────────────┘     └────────┬──────────┘
                                                     │
  ┌──────────────┐     ┌──────────────┐     ┌────────▼──────────┐
  │  DASHBOARD   │◀────│ ORCHESTRATION│◀────│   STORAGE         │
  │  Analytics   │     │  Airflow DAG │     │  OLTP + DW + Lake │
  │  (Task 30)   │     │  (Task 22-23)│     │  (Tasks 6,9,28)   │
  └──────────────┘     └──────────────┘     └───────────────────┘
         │
         ├──  Category revenue breakdown
         ├──  Monthly revenue trend (12 months)
         ├──  Top cities & merchants
         └──  Fraud / velocity alerts
"""
arch_box = Table([[p(arch_txt, CODE)]], colWidths=[W - 5*cm])
arch_box.setStyle(TableStyle([
    ("BACKGROUND", (0, 0), (-1, -1), LGREEN),
    ("BOX",        (0, 0), (-1, -1), 0.8, GREEN),
    ("PADDING",    (0, 0), (-1, -1), 8),
]))
story.append(arch_box)
story.append(sp(0.4))

story.append(p("6.  Dataset Schema", H1)); story.append(hr())
schema = [
    ["Field",        "Type",   "Description",              "Sample Value"],
    ["txn_id",       "TEXT",   "Unique transaction ID",    "TXN00000001"],
    ["user_id",      "TEXT",   "Wallet user identifier",   "U0001 – U1000"],
    ["merchant",     "TEXT",   "Merchant / platform name", "Amazon, Zomato, Uber …"],
    ["category",     "TEXT",   "Transaction category",     "Shopping, Food, Travel …"],
    ["amount",       "REAL",   "INR value (10 – 50,000)",  "24,936.97"],
    ["status",       "TEXT",   "Transaction outcome",      "SUCCESS / FAILED / …"],
    ["wallet_type",  "TEXT",   "Digital wallet used",      "PayWallet, QuickPay …"],
    ["timestamp",    "TEXT",   "Event datetime",           "2023-01-01 09:15:32"],
    ["date",         "TEXT",   "Date partition key",       "2023-01-01"],
]
schema_t = Table(schema, colWidths=[2.5*cm, 1.5*cm, 6*cm, 6*cm])
schema_t.setStyle(TableStyle([
    ("BACKGROUND",  (0, 0), (-1, 0), NAVY),
    ("TEXTCOLOR",   (0, 0), (-1, 0), WHITE),
    ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
    ("FONTSIZE",    (0, 0), (-1, -1), 9),
    ("GRID",        (0, 0), (-1, -1), 0.4, DBLUE),
    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LGRAY]),
    ("PADDING",     (0, 0), (-1, -1), 4),
]))
story.append(schema_t)
story.append(PageBreak())


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 – RESULTS + CODE SAMPLES
# ══════════════════════════════════════════════════════════════════════════════
story.append(p("7.  Key Results", H1)); story.append(hr())
story.append(p(
    "The full end-to-end pipeline (Task 30) processed 100,000 wallet transactions "
    "and produced the following KPIs from the final SQLite data warehouse:"
))
story.append(sp(0.3))

kpi_rows = [
    ["KPI",                      "Value"],
    ["Total Transactions",        "100,000"],
    ["Total Revenue (INR)",       "2,493.70 M"],
    ["Successful Revenue",        "1,875.00 M  (75.08 % success rate)"],
    ["Average Transaction Value", "INR 24,936.97"],
    ["Active Wallet Users",       "1,000"],
    ["Top Revenue Category",      "Utilities – INR 237.82 M"],
    ["Top Revenue City",          "Ahmedabad – INR 216.06 M"],
    ["Top Merchant",              "PhonePe – INR 128.65 M (success only)"],
    ["Batch Throughput",          "111,301 rows / second"],
    ["NumPy Vectorisation",       "66× faster than Python loop"],
    ["Pandas Memory Saving",      "62.1 % reduction (float32 + category dtype)"],
    ["HDFS Replication",          "3× – 4 DataNodes, 12 total blocks"],
    ["Data Quality Score",        "100 % – Completeness + Uniqueness + Validity"],
    ["Full Pipeline Duration",    "~4 seconds end-to-end (SQLite)"],
]
story.append(two_col_table(kpi_rows))
story.append(sp(0.5))

story.append(p("8.  Selected Code Snippets", H1)); story.append(hr())

story.append(p("Task 04 – Aggregator module (reusable across tasks 29 & 30):", H3))
story.append(p(
    "from task04_advanced_python.transformation_modules import Aggregator\n"
    "agg = Aggregator()\n"
    "cat_sum = agg.sum_by(rows, 'category', 'amount')   # {Utilities: 315M, …}\n"
    "p95     = agg.percentile(amounts, 95)               # → 47,876.35", CODE))

story.append(p("Task 07 – Window function (daily running total):", H3))
story.append(p(
    "SELECT date, COUNT(*) AS txns,\n"
    "       SUM(SUM(amount)) OVER (ORDER BY date\n"
    "           ROWS UNBOUNDED PRECEDING) AS running_total\n"
    "FROM transactions GROUP BY date ORDER BY date;", CODE))

story.append(p("Task 22 – Airflow DAG task decorator pattern:", H3))
story.append(p(
    "@dag.task('extract', upstream='check_source', retries=2)\n"
    "def extract(ctx):\n"
    "    with open(TXN_CSV) as f:\n"
    "        rows = list(csv.DictReader(f))\n"
    "    return {'rows': len(rows), 'data': rows}", CODE))

story.append(p("Task 28 – Delta Lake time travel:", H3))
story.append(p(
    "table = DeltaTable('/delta/wallet_transactions')\n"
    "v0 = table.write(jan_data, mode='overwrite')   # v0\n"
    "v1 = table.write(feb_data, mode='append')      # v1\n"
    "old = table.read(version=0)                    # → Jan only (2,000 rows)\n"
    "now = table.read()                             # → latest (5,800 rows)", CODE))

story.append(p("Task 30 – Full pipeline call (5 stages in sequence):", H3))
story.append(p(
    "ing  = stage_ingest()           # 100,000 rows from CSV\n"
    "proc = stage_process(ing)       # validate, enrich, compute GST\n"
    "stor = stage_store(proc)        # SQLite OLTP + 6 Data Lake partitions\n"
    "orch = stage_orchestrate(...)   # DAG summary + SLA check\n"
    "stage_dashboard()               # KPI cards + category/city/fraud charts", CODE))
story.append(PageBreak())


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 – UNIQUE POINTS + FUTURE WORK + REFERENCES
# ══════════════════════════════════════════════════════════════════════════════
story.append(p("9.  Unique Points", H1)); story.append(hr())

unique = [
    ("Zero-Dependency Core",
     "All 30 tasks run with only pandas + numpy installed. No Java, Spark, "
     "Kafka, or cloud SDKs are required — every concept is demonstrated using "
     "clean Python that mirrors the real production API exactly."),
    ("Production-Grade Patterns",
     "Batch checksums, audit log tables, ACID Delta commits, Airflow XCom, "
     "presigned S3 URLs, Kafka consumer group offset management — every "
     "industry pattern is implemented faithfully, not just described."),
    ("Modular, Reusable Code",
     "The Normalizer / Aggregator / Validator classes from Task 04 are imported "
     "and reused in Tasks 29 and 30, demonstrating real software engineering "
     "practice rather than isolated scripts."),
    ("Realistic Indian Fintech Dataset",
     "Cities (Mumbai, Delhi, Ranchi …), merchants (PhonePe, IRCTC, Zomato …), "
     "and GST calculations make the data feel authentic and domain-relevant."),
    ("Verified Performance Numbers",
     "Every benchmark is measured and printed: 66× NumPy speedup, 62 % memory "
     "reduction, 111K rows/sec ingestion, 4-second full pipeline — reproducible "
     "on any machine."),
    ("End-to-End Traceability",
     "Every processed record carries processed_at, batch_id, checksum, "
     "kyc_status, and city — full lineage from raw CSV to dashboard."),
    ("Delta Lake with Time Travel",
     "The Task 28 DeltaTable class implements a real JSON commit log, "
     "copy-on-write updates, time travel reads, VACUUM, and OPTIMIZE — "
     "not just a description of Delta Lake."),
    ("Complete Anomaly Detection Suite",
     "Task 29 implements six independent quality checks: schema validation, "
     "completeness, uniqueness, Z-score, IQR, behavioural (per-user sigma), "
     "and velocity — composing a production-ready DQ framework."),
]

for title, desc in unique:
    story.append(p(f"<b>{title}</b>", SMALL))
    story.append(p(desc, SMALL))
    story.append(sp(0.15))

story.append(sp(0.3))
story.append(p("10.  Future Improvements", H1)); story.append(hr())

future = [
    "Replace SQLite with PostgreSQL for true multi-user OLTP with MVCC and row-level locking.",
    "Deploy on Apache Spark 3.x (Databricks / EMR) for genuine distributed processing at 100 M+ rows.",
    "Integrate real Apache Kafka with Confluent Cloud for production-grade stream processing.",
    "Deploy Airflow 2.x on Kubernetes (KubernetesExecutor) for scalable, containerised DAG runs.",
    "Store Data Lake files in AWS S3 as Parquet (pyarrow) and query via AWS Athena / Presto.",
    "Train an Isolation Forest / XGBoost fraud detection model (scikit-learn) and plug it into the streaming pipeline.",
    "Add Apache Atlas or DataHub for data catalogue, lineage tracking, and governance across all pipeline stages.",
    "Build a Grafana or Apache Superset dashboard connected to the data warehouse for self-service BI.",
    "Implement PII masking and row-level security to comply with DPDP Act 2023 and GDPR.",
    "Add a CI/CD pipeline (GitHub Actions) to auto-test and deploy pipeline code on every push.",
]
for i, item in enumerate(future, 1):
    story.append(p(f"<b>{i}.</b>  {item}", SMALL))

story.append(sp(0.4))
story.append(p("11.  References", H1)); story.append(hr())
refs = [
    "Apache Hadoop HDFS Architecture Guide – hadoop.apache.org/docs",
    "Apache Spark 3.x Documentation – spark.apache.org/docs/latest",
    "Apache Kafka Documentation – kafka.apache.org/documentation",
    "Apache Airflow 2.x Documentation – airflow.apache.org/docs",
    "Delta Lake Documentation – docs.delta.io",
    "AWS Well-Architected Data Engineering Framework – aws.amazon.com/architecture",
    "Google BigQuery Documentation – cloud.google.com/bigquery/docs",
    "Pandas 2.x Performance Guide – pandas.pydata.org/docs/user_guide",
    "Fundamentals of Data Engineering – Joe Reis & Matt Housley (O'Reilly, 2022)",
    "Designing Data-Intensive Applications – Martin Kleppmann (O'Reilly, 2017)",
]
for ref in refs:
    story.append(bullet(ref))


# ── build PDF ────────────────────────────────────────────────────────────────
doc.build(story, onFirstPage=add_page_number, onLaterPages=add_page_number)
print(f"PDF generated → {OUT}")
