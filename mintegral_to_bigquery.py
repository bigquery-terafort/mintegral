"""
Mintegral Publisher → BigQuery  ·  COMPLETE PIPELINE v2
========================================================
Fixes vs v1:
  1. Replaced insert_rows_json (streaming) with load_table_from_json (batch load jobs)
  2. Delete-before-insert for all 7 tables — no duplicates on re-runs
  3. fetch_pub_revenue_by_adformat: fixed group_by from "date,unit_id" to "date,ad_format"

Auth: md5(SECRET + md5(time))
API limit: max 7 days per request, max 60 days back

Tables:
  1. pub_revenue_daily         — daily revenue totals
  2. pub_revenue_by_country    — revenue by country
  3. pub_revenue_by_platform   — revenue by platform (Android/iOS)
  4. pub_revenue_by_adformat   — revenue by ad format
  5. pub_revenue_by_app        — revenue by app + placement
  6. pub_revenue_by_unit       — revenue by ad unit (most granular)
  7. pub_revenue_by_bidding    — revenue by bidding type
"""

import os, json, logging, hashlib, requests, time
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

# All 7 tables are date-based — delete by date range before loading
REPORTING_TABLES = set(SCHEMAS.keys())

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def safe_float(v):
    try: return float(v) if v not in (None, "", "N/A") else None
    except: return None

def safe_int(v):
    try: return int(float(v)) if v not in (None, "", "N/A") else None
    except: return None

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

def fetch_mintegral(group_by, start_date, end_date):
    """Fetch with 7-day chunking and pagination."""
    all_rows = []
    current  = start_date
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
        try:
            while True:
                resp = requests.get(BASE_URL, params=params, timeout=60)
                if not resp.text.strip():
                    log.warning(f"  Empty response — skipping chunk")
                    break
                data = resp.json()
                if data.get("code", "").lower() != "ok":
                    log.warning(f"  API error: {data}")
                    break
                lists = data.get("data", {}).get("lists", [])
                if lists:
                    log.info(f"  Got {len(lists)} rows (page {params['page']})")
                all_rows.extend(lists)
                total_page = data.get("data", {}).get("total_page", 1)
                if params["page"] >= total_page:
                    break
                params["page"] += 1
                # Regenerate sign for next page
                ts = int(time.time())
                params["time"] = ts
                params["sign"] = make_sign(MINTEGRAL_SECRET, ts)
        except Exception as e:
            log.warning(f"  Request failed: {e}")
        current = chunk_end + timedelta(days=1)
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
    # FIX v2: group_by="date,ad_format" — was wrongly "date,unit_id"
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
    """
    FIX v2: Batch load jobs — no streaming buffer issues.
    Delete date range before loading to prevent duplicates.
    """
    if not rows:
        log.info(f"  No rows for {name}")
        return

    table_ref  = f"{GCP_PROJECT}.{BQ_DATASET}.{name}"
    start, end = get_date_range()

    # Delete existing rows for this date range
    try:
        client.query(
            f"DELETE FROM `{table_ref}` WHERE date BETWEEN '{start}' AND '{end}'"
        ).result()
        log.info(f"  Cleared {name} ({start} to {end})")
    except Exception as e:
        log.warning(f"  Could not clear {name}: {e}")

    # Load using batch load job (not streaming)
    try:
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMAS[name],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job = client.load_table_from_json(rows, table_ref, job_config=job_config)
        load_job.result()
        log.info(f"  ✅ {len(rows):,} rows → {name}")
    except Exception as e:
        log.error(f"  Load job failed [{name}]: {e}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    log.info("🚀 Mintegral Publisher → BigQuery COMPLETE sync v2")
    log.info(f"   Lookback: {LOOKBACK_DAYS} days")

    bq = get_bq_client()
    ensure_dataset(bq)
    for t in SCHEMAS:
        ensure_table(bq, t)

    load_to_bq(bq, "pub_revenue_daily",       fetch_pub_revenue_daily())
    load_to_bq(bq, "pub_revenue_by_country",  fetch_pub_revenue_by_country())
    load_to_bq(bq, "pub_revenue_by_platform", fetch_pub_revenue_by_platform())
    load_to_bq(bq, "pub_revenue_by_adformat", fetch_pub_revenue_by_adformat())
    load_to_bq(bq, "pub_revenue_by_app",      fetch_pub_revenue_by_app())
    load_to_bq(bq, "pub_revenue_by_unit",     fetch_pub_revenue_by_unit())
    load_to_bq(bq, "pub_revenue_by_bidding",  fetch_pub_revenue_by_bidding())

    log.info("✅ Mintegral Publisher sync v2 complete! 7 tables loaded.")

if __name__ == "__main__":
    main()
