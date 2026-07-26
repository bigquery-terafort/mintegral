"""
Mintegral Publisher → BigQuery  ·  COMPLETE PIPELINE v3
========================================================
REPO: bigquery-terafort/mintegral

DATA HAAL (BigQuery se verify): ✅ May/June/July mein 0 missing days
   (by_app, daily, by_unit — teeno). daily vs by_app farq ~$7/3 mahine
   (0.03%, per-row rounding).

v3 KE FIX — LANDMINE DEFUSE (aaj ka output BILKUL WAISA HI rehta hai):
  🛡️ 1. `fetch_mintegral` CHAAR jagah chup-chaap adhoori list deta tha:
            if not resp.text.strip():  log.warning(...); break     # chunk gaya
            if code != "ok":           log.warning(...); break     # chunk gaya
            except Exception as e:     log.warning(...)            # chunk gaya
            (aur pagination ke beech mein bhi wahi break → aadha page)
        Phir load_to_bq POORI 30-din window DELETE karke sirf bacha hua
        likhta tha → jo chunk fail hua, us ke 7 din UR GAYE.
        Ab: koi bhi chunk fail → poori fetch fail (RuntimeError), kuch
        delete nahi hota.
  🛡️ 2. DELETE fail ho to load bilkul nahi (warna duplicate rows).
  🛡️ 3. Load fail ho to raise — DELETE ho chuki hai, chup rehna sab se bura.

v2 se BARQARAR:
  ✅ batch load jobs (streaming nahi)
  ✅ delete-before-insert per date range
  ✅ fetch_pub_revenue_by_adformat ka group_by="date,ad_format"

Auth: md5(SECRET + md5(time))
API limit: max 7 days per request, max 60 days back
"""

import os, json, logging, hashlib, requests, time, sys
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MINTEGRAL_SKEY       = os.environ["MINTEGRAL_SKEY"]
MINTEGRAL_SECRET     = os.environ["MINTEGRAL_SECRET"]
GCP_PROJECT          = os.environ["GCP_PROJECT"]
BQ_DATASET           = os.environ.get("BQ_DATASET", "mintegral_data")
GCP_CREDENTIALS_JSON = os.environ["GCP_CREDENTIALS_JSON"]
LOOKBACK_DAYS        = int(os.environ.get("LOOKBACK_DAYS", "30"))

BASE_URL = "https://api.mintegral.com/reporting/v2/data"

# ─── BQ SCHEMAS ──────────────────────────────────────────────────────────────
SCHEMAS = {
    "pub_revenue_daily": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("request",         "INTEGER"),
        bigquery.SchemaField("filled",          "INTEGER"),
        bigquery.SchemaField("fill_rate",       "FLOAT"),
        bigquery.SchemaField("impression",      "INTEGER"),
        bigquery.SchemaField("click",           "INTEGER"),
        bigquery.SchemaField("est_revenue",     "FLOAT"),
        bigquery.SchemaField("ecpm",            "FLOAT"),
        bigquery.SchemaField("ctr",             "FLOAT"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "pub_revenue_by_country": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("country",         "STRING"),
        bigquery.SchemaField("request",         "INTEGER"),
        bigquery.SchemaField("filled",          "INTEGER"),
        bigquery.SchemaField("impression",      "INTEGER"),
        bigquery.SchemaField("click",           "INTEGER"),
        bigquery.SchemaField("est_revenue",     "FLOAT"),
        bigquery.SchemaField("ecpm",            "FLOAT"),
        bigquery.SchemaField("ctr",             "FLOAT"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "pub_revenue_by_platform": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("platform",        "STRING"),
        bigquery.SchemaField("request",         "INTEGER"),
        bigquery.SchemaField("filled",          "INTEGER"),
        bigquery.SchemaField("impression",      "INTEGER"),
        bigquery.SchemaField("click",           "INTEGER"),
        bigquery.SchemaField("est_revenue",     "FLOAT"),
        bigquery.SchemaField("ecpm",            "FLOAT"),
        bigquery.SchemaField("ctr",             "FLOAT"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "pub_revenue_by_adformat": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("ad_format",       "STRING"),
        bigquery.SchemaField("request",         "INTEGER"),
        bigquery.SchemaField("filled",          "INTEGER"),
        bigquery.SchemaField("impression",      "INTEGER"),
        bigquery.SchemaField("click",           "INTEGER"),
        bigquery.SchemaField("est_revenue",     "FLOAT"),
        bigquery.SchemaField("ecpm",            "FLOAT"),
        bigquery.SchemaField("ctr",             "FLOAT"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "pub_revenue_by_app": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("app_id",          "STRING"),
        bigquery.SchemaField("app_name",        "STRING"),
        bigquery.SchemaField("app_package",     "STRING"),
        bigquery.SchemaField("platform",        "STRING"),
        bigquery.SchemaField("placement_id",    "STRING"),
        bigquery.SchemaField("placement_name",  "STRING"),
        bigquery.SchemaField("request",         "INTEGER"),
        bigquery.SchemaField("filled",          "INTEGER"),
        bigquery.SchemaField("fill_rate",       "FLOAT"),
        bigquery.SchemaField("impression",      "INTEGER"),
        bigquery.SchemaField("click",           "INTEGER"),
        bigquery.SchemaField("est_revenue",     "FLOAT"),
        bigquery.SchemaField("ecpm",            "FLOAT"),
        bigquery.SchemaField("ctr",             "FLOAT"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "pub_revenue_by_unit": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("app_id",          "STRING"),
        bigquery.SchemaField("app_name",        "STRING"),
        bigquery.SchemaField("app_package",     "STRING"),
        bigquery.SchemaField("placement_id",    "STRING"),
        bigquery.SchemaField("placement_name",  "STRING"),
        bigquery.SchemaField("unit_id",         "STRING"),
        bigquery.SchemaField("unit_name",       "STRING"),
        bigquery.SchemaField("ad_format",       "STRING"),
        bigquery.SchemaField("platform",        "STRING"),
        bigquery.SchemaField("country",         "STRING"),
        bigquery.SchemaField("request",         "INTEGER"),
        bigquery.SchemaField("filled",          "INTEGER"),
        bigquery.SchemaField("fill_rate",       "FLOAT"),
        bigquery.SchemaField("impression",      "INTEGER"),
        bigquery.SchemaField("click",           "INTEGER"),
        bigquery.SchemaField("est_revenue",     "FLOAT"),
        bigquery.SchemaField("ecpm",            "FLOAT"),
        bigquery.SchemaField("ctr",             "FLOAT"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
    "pub_revenue_by_bidding": [
        bigquery.SchemaField("date",            "DATE"),
        bigquery.SchemaField("app_id",          "STRING"),
        bigquery.SchemaField("app_name",        "STRING"),
        bigquery.SchemaField("bidding_type",    "STRING"),
        bigquery.SchemaField("impression",      "INTEGER"),
        bigquery.SchemaField("click",           "INTEGER"),
        bigquery.SchemaField("est_revenue",     "FLOAT"),
        bigquery.SchemaField("ecpm",            "FLOAT"),
        bigquery.SchemaField("hb_load",         "INTEGER"),
        bigquery.SchemaField("hb_load_filled",  "INTEGER"),
        bigquery.SchemaField("_ingested_at",    "TIMESTAMP"),
    ],
}

REPORTING_TABLES = set(SCHEMAS.keys())

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def safe_float(v):
    try: return float(v) if v not in (None, "", "N/A") else None
    except Exception: return None

def safe_int(v):
    try: return int(float(v)) if v not in (None, "", "N/A") else None
    except Exception: return None

def now_ts():
    return datetime.utcnow().isoformat()

def make_sign(secret, ts):
    """Generate signature: md5(SECRET + md5(time))"""
    inner = hashlib.md5(str(ts).encode()).hexdigest()
    return hashlib.md5((secret + inner).encode()).hexdigest()

def fmt_date(d):
    return d.strftime("%Y%m%d")

def parse_date(d):
    s = str(d)
    if len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s

def get_date_range():
    end   = datetime.utcnow().date() - timedelta(days=1)
    start = end - timedelta(days=LOOKBACK_DAYS - 1)
    return start, end

# 🛡️ v3: koi bhi chunk/page fail hua to poori fetch fail
def fetch_mintegral(group_by, start_date, end_date):
    """v3: adhoori list KABHI wapas nahi jayegi.

    Wajah: caller (load_to_bq) POORI 30-din window DELETE karta hai. Adhoore
    data ke saath chalne dena = jo chunk fail hua us ke din hamesha ke liye
    gaye. (Bilkul yehi shakl Apple ke saath hui — aadha saal gaya.)
    """
    all_rows = []
    failed_chunks = []
    current = start_date

    while current <= end_date:
        chunk_end = min(current + timedelta(days=6), end_date)
        ts   = int(time.time())
        sign = make_sign(MINTEGRAL_SECRET, ts)
        params = {
            "skey":     MINTEGRAL_SKEY,
            "sign":     sign,
            "time":     ts,
            "start":    fmt_date(current),
            "end":      fmt_date(chunk_end),
            "group_by": group_by,
            "limit":    10000,
            "page":     1,
            "timezone": 0,
        }
        log.info(f"  GET group_by={group_by} {fmt_date(current)}→{fmt_date(chunk_end)}")

        chunk_ok = True                                   # 🛡️ v3
        try:
            while True:
                resp = requests.get(BASE_URL, params=params, timeout=60)
                if not resp.text.strip():
                    log.error(f"  🚨 empty response {current}→{chunk_end}")
                    chunk_ok = False                      # 🛡️ v2: chup-chaap break
                    break
                data = resp.json()
                if data.get("code", "").lower() != "ok":
                    log.error(f"  🚨 API error {current}→{chunk_end}: {data}")
                    chunk_ok = False                      # 🛡️
                    break
                lists = data.get("data", {}).get("lists", [])
                if lists:
                    log.info(f"  Got {len(lists)} rows (page {params['page']})")
                all_rows.extend(lists)
                total_page = data.get("data", {}).get("total_page", 1)
                if params["page"] >= total_page:
                    break
                params["page"] += 1
                ts = int(time.time())
                params["time"] = ts
                params["sign"] = make_sign(MINTEGRAL_SECRET, ts)
        except Exception as e:
            log.error(f"  🚨 request failed {current}→{chunk_end}: {e}")
            chunk_ok = False                              # 🛡️

        if not chunk_ok:
            failed_chunks.append(f"{fmt_date(current)}→{fmt_date(chunk_end)}")
        current = chunk_end + timedelta(days=1)

    if failed_chunks:
        raise RuntimeError(
            f"[{group_by}] {len(failed_chunks)} chunk(s) failed "
            f"({', '.join(failed_chunks)}) — refusing to return a partial "
            f"result. Existing data will be left untouched.")
    return all_rows

# ─── FETCH FUNCTIONS ──────────────────────────────────────────────────────────
def fetch_pub_revenue_daily():
    log.info("Fetching Publisher Revenue Daily...")
    start, end = get_date_range()
    rows = []
    for r in fetch_mintegral("date", start, end):
        rows.append({
            "date":         parse_date(r.get("date", "")),
            "request":      safe_int(r.get("request")),
            "filled":       safe_int(r.get("filled")),
            "fill_rate":    safe_float(r.get("fill_rate")),
            "impression":   safe_int(r.get("impression")),
            "click":        safe_int(r.get("click")),
            "est_revenue":  safe_float(r.get("est_revenue")),
            "ecpm":         safe_float(r.get("ecpm")),
            "ctr":          safe_float(r.get("ctr")),
            "_ingested_at": now_ts(),
        })
    return rows

def fetch_pub_revenue_by_country():
    log.info("Fetching Publisher Revenue by Country...")
    start, end = get_date_range()
    rows = []
    for r in fetch_mintegral("date,country", start, end):
        rows.append({
            "date":         parse_date(r.get("date", "")),
            "country":      r.get("country"),
            "request":      safe_int(r.get("request")),
            "filled":       safe_int(r.get("filled")),
            "impression":   safe_int(r.get("impression")),
            "click":        safe_int(r.get("click")),
            "est_revenue":  safe_float(r.get("est_revenue")),
            "ecpm":         safe_float(r.get("ecpm")),
            "ctr":          safe_float(r.get("ctr")),
            "_ingested_at": now_ts(),
        })
    return rows

def fetch_pub_revenue_by_platform():
    log.info("Fetching Publisher Revenue by Platform...")
    start, end = get_date_range()
    rows = []
    for r in fetch_mintegral("date,platform", start, end):
        rows.append({
            "date":         parse_date(r.get("date", "")),
            "platform":     r.get("platform"),
            "request":      safe_int(r.get("request")),
            "filled":       safe_int(r.get("filled")),
            "impression":   safe_int(r.get("impression")),
            "click":        safe_int(r.get("click")),
            "est_revenue":  safe_float(r.get("est_revenue")),
            "ecpm":         safe_float(r.get("ecpm")),
            "ctr":          safe_float(r.get("ctr")),
            "_ingested_at": now_ts(),
        })
    return rows

def fetch_pub_revenue_by_adformat():
    log.info("Fetching Publisher Revenue by Ad Format...")
    start, end = get_date_range()
    rows = []
    for r in fetch_mintegral("date,ad_format", start, end):
        rows.append({
            "date":         parse_date(r.get("date", "")),
            "ad_format":    r.get("ad_format"),
            "request":      safe_int(r.get("request")),
            "filled":       safe_int(r.get("filled")),
            "impression":   safe_int(r.get("impression")),
            "click":        safe_int(r.get("click")),
            "est_revenue":  safe_float(r.get("est_revenue")),
            "ecpm":         safe_float(r.get("ecpm")),
            "ctr":          safe_float(r.get("ctr")),
            "_ingested_at": now_ts(),
        })
    return rows

def fetch_pub_revenue_by_app():
    log.info("Fetching Publisher Revenue by App + Placement...")
    start, end = get_date_range()
    rows = []
    for r in fetch_mintegral("date,app_id,platform,placement_id", start, end):
        rows.append({
            "date":           parse_date(r.get("date", "")),
            "app_id":         str(r.get("app_id", "")),
            "app_name":       r.get("app_name"),
            "app_package":    r.get("app_package"),
            "platform":       r.get("platform"),
            "placement_id":   str(r.get("placement_id", "")),
            "placement_name": r.get("placement_name"),
            "request":        safe_int(r.get("request")),
            "filled":         safe_int(r.get("filled")),
            "fill_rate":      safe_float(r.get("fill_rate")),
            "impression":     safe_int(r.get("impression")),
            "click":          safe_int(r.get("click")),
            "est_revenue":    safe_float(r.get("est_revenue")),
            "ecpm":           safe_float(r.get("ecpm")),
            "ctr":            safe_float(r.get("ctr")),
            "_ingested_at":   now_ts(),
        })
    return rows

def fetch_pub_revenue_by_unit():
    log.info("Fetching Publisher Revenue by Unit (most granular)...")
    start, end = get_date_range()
    rows = []
    for r in fetch_mintegral("date,app_id,placement_id,unit_id,country", start, end):
        rows.append({
            "date":           parse_date(r.get("date", "")),
            "app_id":         str(r.get("app_id", "")),
            "app_name":       r.get("app_name"),
            "app_package":    r.get("app_package"),
            "placement_id":   str(r.get("placement_id", "")),
            "placement_name": r.get("placement_name"),
            "unit_id":        str(r.get("unit_id", "")),
            "unit_name":      r.get("unit_name"),
            "ad_format":      r.get("ad_format"),
            "platform":       r.get("platform"),
            "country":        r.get("country"),
            "request":        safe_int(r.get("request")),
            "filled":         safe_int(r.get("filled")),
            "fill_rate":      safe_float(r.get("fill_rate")),
            "impression":     safe_int(r.get("impression")),
            "click":          safe_int(r.get("click")),
            "est_revenue":    safe_float(r.get("est_revenue")),
            "ecpm":           safe_float(r.get("ecpm")),
            "ctr":            safe_float(r.get("ctr")),
            "_ingested_at":   now_ts(),
        })
    return rows

def fetch_pub_revenue_by_bidding():
    log.info("Fetching Publisher Revenue by Bidding Type...")
    start, end = get_date_range()
    rows = []
    for r in fetch_mintegral("date,app_id,bidding_type", start, end):
        rows.append({
            "date":           parse_date(r.get("date", "")),
            "app_id":         str(r.get("app_id", "")),
            "app_name":       r.get("app_name"),
            "bidding_type":   r.get("bidding_type"),
            "impression":     safe_int(r.get("impression")),
            "click":          safe_int(r.get("click")),
            "est_revenue":    safe_float(r.get("est_revenue")),
            "ecpm":           safe_float(r.get("ecpm")),
            "hb_load":        safe_int(r.get("hb_load")),
            "hb_load_filled": safe_int(r.get("hb_load_filled")),
            "_ingested_at":   now_ts(),
        })
    return rows

# ─── BIGQUERY ─────────────────────────────────────────────────────────────────
def get_bq_client():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GCP_CREDENTIALS_JSON),
        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return bigquery.Client(project=GCP_PROJECT, credentials=creds)

def ensure_dataset(client):
    try: client.get_dataset(BQ_DATASET)
    except Exception:
        log.info(f"Creating dataset {BQ_DATASET}")
        client.create_dataset(bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}"))

def ensure_table(client, name):
    ref = client.dataset(BQ_DATASET).table(name)
    try: client.get_table(ref)
    except Exception:
        log.info(f"Creating table {name}")
        client.create_table(bigquery.Table(ref, schema=SCHEMAS[name]))

def load_to_bq(client, name, rows):
    """v3: DELETE fail ho to load bilkul nahi; load fail ho to chillao."""
    if not rows:
        log.warning(f"  ⚠️  No rows for {name} — nothing deleted, nothing loaded")
        return

    table_ref  = f"{GCP_PROJECT}.{BQ_DATASET}.{name}"
    start, end = get_date_range()

    try:
        client.query(
            f"DELETE FROM `{table_ref}` WHERE date BETWEEN '{start}' AND '{end}'"
        ).result()
        log.info(f"  Cleared {name} ({start} to {end})")
    except Exception as e:
        # 🛡️ DELETE fail + APPEND = duplicate rows
        log.error(f"  🚨 Could not clear {name}: {e}")
        raise

    try:
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMAS[name],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        client.load_table_from_json(rows, table_ref, job_config=job_config).result()
        log.info(f"  ✅ {len(rows):,} rows → {name}")
    except Exception as e:
        # 🛡️ DELETE ho chuki hai aur load fail — data gaya. Chup mat raho.
        log.error(f"  🚨 Load job failed [{name}] AFTER delete: {e}")
        raise

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    log.info("🚀 Mintegral Publisher → BigQuery COMPLETE sync v3")
    log.info(f"   Lookback: {LOOKBACK_DAYS} days")

    bq = get_bq_client()
    ensure_dataset(bq)
    for t in SCHEMAS:
        ensure_table(bq, t)

    failed = []
    tasks = [
        ("pub_revenue_daily",       fetch_pub_revenue_daily),
        ("pub_revenue_by_country",  fetch_pub_revenue_by_country),
        ("pub_revenue_by_platform", fetch_pub_revenue_by_platform),
        ("pub_revenue_by_adformat", fetch_pub_revenue_by_adformat),
        ("pub_revenue_by_app",      fetch_pub_revenue_by_app),
        ("pub_revenue_by_unit",     fetch_pub_revenue_by_unit),
        ("pub_revenue_by_bidding",  fetch_pub_revenue_by_bidding),
    ]
    for name, fn in tasks:
        try:
            load_to_bq(bq, name, fn())
        except Exception as e:
            # 🛡️ v3: ek table fail ho to baaki chalne do, LEKIN exit code 1
            log.error(f"  🚨 {name} FAILED: {e}")
            failed.append(name)

    if failed:
        log.error(f"❌ {len(failed)} table(s) NOT refreshed: {failed}")
        sys.exit(1)
    log.info("✅ Mintegral Publisher sync v3 complete! 7 tables loaded.")

if __name__ == "__main__":
    main()
