"""
Orange County FL – Motivated Seller Lead Scraper
Fetches clerk records + enriches with property appraiser data.
Outputs: dashboard/records.json, data/records.json, data/leads_export.csv
"""

import asyncio
import json
import os
import re
import csv
import time
import logging
import traceback
import zipfile
import io
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── optional dbfread ──────────────────────────────────────────────────────────
try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False
    logging.warning("dbfread not installed – parcel enrichment disabled")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", 7))
CLERK_BASE      = "https://www.myorangeclerk.com"
CLERK_SEARCH    = f"{CLERK_BASE}/officialrecords/search"

# Orange County Property Appraiser bulk data
OCPA_BULK_BASE  = "https://www.ocpafl.org"
OCPA_BULK_URL   = "https://www.ocpafl.org/downloads/Parcel_Data.zip"

LEAD_TYPES = {
    "LP":       ("Lis Pendens",             "Pre-Foreclosure"),
    "NOFC":     ("Notice of Foreclosure",   "Pre-Foreclosure"),
    "TAXDEED":  ("Tax Deed",                "Tax / Govt Lien"),
    "JUD":      ("Judgment",                "Judgment Lien"),
    "CCJ":      ("Certified Judgment",      "Judgment Lien"),
    "DRJUD":    ("Domestic Judgment",       "Judgment Lien"),
    "LNCORPTX": ("Corp Tax Lien",           "Tax / Govt Lien"),
    "LNIRS":    ("IRS Lien",                "Tax / Govt Lien"),
    "LNFED":    ("Federal Lien",            "Tax / Govt Lien"),
    "LN":       ("Lien",                    "General Lien"),
    "LNMECH":   ("Mechanic Lien",           "Mechanic Lien"),
    "LNHOA":    ("HOA Lien",                "HOA Lien"),
    "MEDLN":    ("Medicaid Lien",           "Govt / Medical Lien"),
    "PRO":      ("Probate Document",        "Probate"),
    "NOC":      ("Notice of Commencement",  "NOC"),
    "RELLP":    ("Release Lis Pendens",     "Release"),
}

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR      = ROOT / "data"
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR.mkdir(exist_ok=True)
DASHBOARD_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120 Safari/537.36"
})

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def safe_strip(v) -> str:
    return str(v).strip() if v else ""

def parse_amount(raw: str) -> Optional[float]:
    try:
        cleaned = re.sub(r"[^\d.]", "", raw)
        return float(cleaned) if cleaned else None
    except Exception:
        return None

def retry(fn, attempts=3, delay=4):
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            log.warning(f"Attempt {i+1}/{attempts} failed: {exc}")
            if i < attempts - 1:
                time.sleep(delay * (i + 1))
    return None

# ─────────────────────────────────────────────────────────────────────────────
# PARCEL DATA
# ─────────────────────────────────────────────────────────────────────────────

class ParcelLookup:
    """Downloads OCPA bulk parcel ZIP/DBF and builds owner→address index."""

    def __init__(self):
        self.by_owner: dict[str, dict] = {}   # normalized name → parcel
        self.by_parcel: dict[str, dict] = {}  # parcel_id → parcel

    def _norm_name(self, name: str) -> str:
        return re.sub(r"\s+", " ", name.upper().strip())

    def _name_variants(self, raw: str) -> list[str]:
        """Generate FIRST LAST / LAST FIRST / LAST, FIRST variants."""
        name = self._norm_name(raw)
        variants = [name]
        # if comma present: "LAST, FIRST" → also try "FIRST LAST"
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            if len(parts) == 2:
                variants.append(f"{parts[1]} {parts[0]}")
        else:
            parts = name.split()
            if len(parts) >= 2:
                variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
                variants.append(f"{parts[-1]} {' '.join(parts[:-1])}")
        return list(dict.fromkeys(variants))

    def _load_dbf(self, dbf_path: Path):
        if not HAS_DBF:
            log.warning("dbfread unavailable; skipping DBF parse")
            return
        log.info(f"Loading parcel DBF: {dbf_path}")
        col_map = {
            "owner":     ["OWNER", "OWN1", "OWN_NAME"],
            "site_addr": ["SITE_ADDR", "SITEADDR", "SITE_ADDRESS"],
            "site_city": ["SITE_CITY", "SITECITY"],
            "site_zip":  ["SITE_ZIP", "SITEZIP", "SITE_ZIP5"],
            "mail_addr": ["ADDR_1", "MAILADR1", "MAIL_ADDR"],
            "mail_city": ["CITY", "MAILCITY", "MAIL_CITY"],
            "mail_state":["STATE", "MAILSTATE", "MAIL_STATE"],
            "mail_zip":  ["ZIP", "MAILZIP", "MAIL_ZIP"],
            "parcel_id": ["PARCEL_ID", "PARCELID", "FOLIO", "PARNO"],
        }

        try:
            table = DBF(str(dbf_path), ignore_missing_memofile=True)
            fields = [f.name.upper() for f in table.fields]

            def find_col(keys):
                for k in keys:
                    if k in fields:
                        return k
                return None

            cols = {k: find_col(v) for k, v in col_map.items()}
            log.info(f"DBF columns mapped: {cols}")

            count = 0
            for rec in table:
                try:
                    owner_raw = safe_strip(rec.get(cols["owner"]) if cols["owner"] else "")
                    if not owner_raw:
                        continue
                    parcel = {
                        "owner":       owner_raw,
                        "site_addr":   safe_strip(rec.get(cols["site_addr"]) if cols["site_addr"] else ""),
                        "site_city":   safe_strip(rec.get(cols["site_city"]) if cols["site_city"] else ""),
                        "site_zip":    safe_strip(rec.get(cols["site_zip"]) if cols["site_zip"] else ""),
                        "mail_addr":   safe_strip(rec.get(cols["mail_addr"]) if cols["mail_addr"] else ""),
                        "mail_city":   safe_strip(rec.get(cols["mail_city"]) if cols["mail_city"] else ""),
                        "mail_state":  safe_strip(rec.get(cols["mail_state"]) if cols["mail_state"] else ""),
                        "mail_zip":    safe_strip(rec.get(cols["mail_zip"]) if cols["mail_zip"] else ""),
                        "parcel_id":   safe_strip(rec.get(cols["parcel_id"]) if cols["parcel_id"] else ""),
                    }
                    pid = parcel["parcel_id"]
                    if pid:
                        self.by_parcel[pid] = parcel
                    for v in self._name_variants(owner_raw):
                        self.by_owner.setdefault(v, parcel)
                    count += 1
                except Exception:
                    pass
            log.info(f"Loaded {count:,} parcel records; {len(self.by_owner):,} owner entries")
        except Exception as exc:
            log.error(f"DBF load failed: {exc}")

    def download_and_load(self):
        """Download OCPA bulk ZIP, extract DBF, load index."""
        zip_path = DATA_DIR / "parcel_data.zip"

        # Try download
        def do_download():
            log.info(f"Downloading parcel data from {OCPA_BULK_URL}")
            r = SESSION.get(OCPA_BULK_URL, timeout=120, stream=True)
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(65536):
                    f.write(chunk)
            log.info(f"Downloaded {zip_path.stat().st_size / 1_048_576:.1f} MB")

        # Use cached if < 20 hours old
        if zip_path.exists() and (time.time() - zip_path.stat().st_mtime) < 72_000:
            log.info("Using cached parcel ZIP")
        else:
            result = retry(do_download, attempts=3, delay=5)
            if result is None and not zip_path.exists():
                log.error("Could not download parcel data; enrichment disabled")
                return

        # Extract DBF
        try:
            with zipfile.ZipFile(zip_path) as zf:
                dbf_files = [n for n in zf.namelist() if n.upper().endswith(".DBF")]
                if not dbf_files:
                    log.warning("No DBF found in parcel ZIP")
                    return
                dbf_name = dbf_files[0]
                dbf_path = DATA_DIR / Path(dbf_name).name
                with zf.open(dbf_name) as src, open(dbf_path, "wb") as dst:
                    dst.write(src.read())
                log.info(f"Extracted {dbf_name}")
            self._load_dbf(dbf_path)
        except Exception as exc:
            log.error(f"ZIP extraction error: {exc}")

    def lookup(self, owner_name: str) -> Optional[dict]:
        for v in self._name_variants(owner_name):
            hit = self.by_owner.get(v)
            if hit:
                return hit
        return None

# ─────────────────────────────────────────────────────────────────────────────
# CLERK SCRAPER (Playwright async)
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_clerk_records(doc_type: str, date_from: str, date_to: str) -> list[dict]:
    """
    Uses Playwright to search the Orange County Clerk portal
    for a given document type within a date range.
    Returns raw record dicts.
    """
    records = []
    url = CLERK_SEARCH

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        page = await ctx.new_page()

        try:
            log.info(f"Fetching {doc_type} | {date_from} → {date_to}")
            await page.goto(url, timeout=30_000)
            await page.wait_for_load_state("networkidle", timeout=15_000)

            # ── Fill search form ──────────────────────────────────────────
            # Doc type dropdown / text field (varies by site version)
            try:
                await page.select_option("select[name*='DocType'], select[id*='docType']",
                                          value=doc_type, timeout=5_000)
            except Exception:
                try:
                    field = page.locator(
                        "input[name*='docType'], input[placeholder*='Document Type']"
                    ).first
                    await field.fill(doc_type, timeout=5_000)
                except Exception:
                    pass

            # Date from
            for sel in ["input[name*='DateFrom']", "input[id*='DateFrom']",
                        "input[placeholder*='Start Date']", "#BeginDate"]:
                try:
                    await page.fill(sel, date_from, timeout=3_000)
                    break
                except Exception:
                    pass

            # Date to
            for sel in ["input[name*='DateTo']", "input[id*='DateTo']",
                        "input[placeholder*='End Date']", "#EndDate"]:
                try:
                    await page.fill(sel, date_to, timeout=3_000)
                    break
                except Exception:
                    pass

            # County / instrument type path (some portals use separate field)
            for sel in ["input[name*='InstrumentType']", "select[name*='InstrumentType']"]:
                try:
                    el = page.locator(sel).first
                    tag = await el.evaluate("e => e.tagName")
                    if tag == "SELECT":
                        await page.select_option(sel, value=doc_type, timeout=3_000)
                    else:
                        await el.fill(doc_type, timeout=3_000)
                    break
                except Exception:
                    pass

            # Submit
            for sel in ["button[type='submit']", "input[type='submit']",
                        "button:has-text('Search')", "#btnSearch"]:
                try:
                    await page.click(sel, timeout=5_000)
                    break
                except Exception:
                    pass

            await page.wait_for_load_state("networkidle", timeout=20_000)

            # ── Paginate through results ──────────────────────────────────
            page_num = 0
            while True:
                page_num += 1
                await asyncio.sleep(0.8)
                html = await page.content()
                batch = _parse_clerk_results(html, doc_type)
                records.extend(batch)
                log.info(f"  {doc_type} page {page_num}: {len(batch)} records")

                # Try next page
                next_btn = page.locator(
                    "a:has-text('Next'), button:has-text('Next'), "
                    "a[rel='next'], .pagination-next, #nextPage"
                ).first
                try:
                    visible = await next_btn.is_visible(timeout=2_000)
                    enabled = not await next_btn.is_disabled(timeout=2_000)
                    if visible and enabled:
                        await next_btn.click(timeout=5_000)
                        await page.wait_for_load_state("networkidle", timeout=15_000)
                    else:
                        break
                except Exception:
                    break

        except PWTimeout:
            log.warning(f"Timeout on {doc_type}")
        except Exception as exc:
            log.error(f"Playwright error for {doc_type}: {exc}\n{traceback.format_exc()}")
        finally:
            await browser.close()

    return records


def _parse_clerk_results(html: str, doc_type: str) -> list[dict]:
    """Parse the search results table from the clerk portal."""
    soup = BeautifulSoup(html, "lxml")
    rows = []

    # Find the main results table
    tables = soup.find_all("table")
    for tbl in tables:
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if not headers:
            continue
        # Must have at least doc/instrument number or date columns
        if not any(k in " ".join(headers) for k in
                   ["doc", "instrument", "book", "date", "filed", "grantor"]):
            continue

        header_map = {}
        for i, h in enumerate(headers):
            if any(k in h for k in ["instrument", "doc", "book", "number"]):
                header_map.setdefault("doc_num", i)
            elif "type" in h:
                header_map.setdefault("doc_type", i)
            elif any(k in h for k in ["filed", "record", "date"]):
                header_map.setdefault("filed", i)
            elif "grantor" in h or "owner" in h:
                header_map.setdefault("grantor", i)
            elif "grantee" in h:
                header_map.setdefault("grantee", i)
            elif any(k in h for k in ["amount", "consider", "value"]):
                header_map.setdefault("amount", i)
            elif any(k in h for k in ["legal", "description"]):
                header_map.setdefault("legal", i)

        for tr in tbl.find_all("tr")[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            texts = [c.get_text(strip=True) for c in cells]

            def get(key, default=""):
                idx = header_map.get(key)
                return texts[idx] if idx is not None and idx < len(texts) else default

            # Try to find direct link in row
            link = ""
            for a in tr.find_all("a", href=True):
                href = a["href"]
                if any(k in href.lower() for k in
                       ["document", "instrument", "detail", "view", "record"]):
                    link = href if href.startswith("http") else f"{CLERK_BASE}{href}"
                    break

            raw_doc_num = get("doc_num") or (texts[0] if texts else "")
            if not raw_doc_num:
                continue

            rows.append({
                "doc_num":   raw_doc_num,
                "doc_type":  get("doc_type") or doc_type,
                "filed":     get("filed"),
                "grantor":   get("grantor"),
                "grantee":   get("grantee"),
                "amount":    get("amount"),
                "legal":     get("legal"),
                "clerk_url": link,
            })

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# ALTERNATIVE: requests-based fallback for clerk (POST search)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_clerk_records_requests(doc_type: str, date_from: str, date_to: str) -> list[dict]:
    """
    Fallback: direct POST to clerk search if Playwright fails.
    Handles __VIEWSTATE / __doPostBack pattern.
    """
    records = []

    def _get_form_state():
        r = SESSION.get(CLERK_SEARCH, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        vs  = soup.find("input", {"name": "__VIEWSTATE"})
        vsg = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
        ev  = soup.find("input", {"name": "__EVENTVALIDATION"})
        return {
            "__VIEWSTATE":          vs["value"]  if vs  else "",
            "__VIEWSTATEGENERATOR": vsg["value"] if vsg else "",
            "__EVENTVALIDATION":    ev["value"]  if ev  else "",
            "cookies":              dict(r.cookies),
        }

    try:
        state = retry(_get_form_state)
        if not state:
            return []

        payload = {
            "__VIEWSTATE":          state["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": state["__VIEWSTATEGENERATOR"],
            "__EVENTVALIDATION":    state["__EVENTVALIDATION"],
            "InstrumentType":       doc_type,
            "DocType":              doc_type,
            "DateFrom":             date_from,
            "DateTo":               date_to,
            "BeginDate":            date_from,
            "EndDate":              date_to,
            "btnSearch":            "Search",
        }

        page_num = 0
        while True:
            page_num += 1
            r = SESSION.post(CLERK_SEARCH, data=payload,
                             cookies=state["cookies"], timeout=30)
            r.raise_for_status()
            batch = _parse_clerk_results(r.text, doc_type)
            records.extend(batch)
            log.info(f"  [requests] {doc_type} page {page_num}: {len(batch)} records")

            # Check for next page __doPostBack
            soup = BeautifulSoup(r.text, "lxml")
            next_link = soup.find("a", string=re.compile(r"Next|>", re.I))
            if not next_link:
                break
            href = next_link.get("href", "")
            dpb = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
            if not dpb:
                break
            payload.update({
                "__EVENTTARGET":   dpb.group(1),
                "__EVENTARGUMENT": dpb.group(2),
                "btnSearch":       "",
            })

    except Exception as exc:
        log.error(f"requests fallback error for {doc_type}: {exc}")

    return records


# ─────────────────────────────────────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────────────────────────────────────

def compute_flags_score(rec: dict, cutoff_date: str) -> tuple[list[str], int]:
    flags = []
    score = 30  # base

    dt    = rec.get("doc_type", "")
    cat   = rec.get("cat", "")
    owner = rec.get("owner", "").upper()
    amt   = rec.get("amount_raw")
    filed = rec.get("filed", "")

    # Category flags
    if dt in ("LP",):
        flags.append("Lis pendens")
        score += 10
    if dt in ("LP", "NOFC"):
        flags.append("Pre-foreclosure")
        score += 10
    if dt in ("JUD", "CCJ", "DRJUD"):
        flags.append("Judgment lien")
        score += 10
    if dt in ("TAXDEED", "LNCORPTX", "LNIRS", "LNFED"):
        flags.append("Tax lien")
        score += 10
    if dt == "LNMECH":
        flags.append("Mechanic lien")
        score += 10
    if dt in ("PRO",):
        flags.append("Probate / estate")
        score += 10
    if "LLC" in owner or " INC" in owner or " CORP" in owner or " LTD" in owner:
        flags.append("LLC / corp owner")
        score += 10

    # LP + FC combo bonus
    if dt == "LP":
        score += 20

    # Amount bonuses
    if amt and amt > 100_000:
        score += 15
        flags.append("High debt (>$100k)")
    elif amt and amt > 50_000:
        score += 10
        flags.append("Significant debt (>$50k)")

    # New this week
    try:
        filed_dt = datetime.strptime(filed[:10], "%Y-%m-%d")
        if (datetime.today() - filed_dt).days <= 7:
            score += 5
            flags.append("New this week")
    except Exception:
        pass

    # Has address
    if rec.get("prop_address"):
        score += 5
        flags.append("Has address")

    return list(dict.fromkeys(flags)), min(score, 100)


# ─────────────────────────────────────────────────────────────────────────────
# ENRICH RECORDS
# ─────────────────────────────────────────────────────────────────────────────

def enrich(raw: dict, doc_type: str, parcel: ParcelLookup, cutoff_date: str) -> dict:
    """Merge raw clerk record with parcel data and compute score."""
    code  = doc_type
    label, cat_label = LEAD_TYPES.get(code, (code, "Other"))

    owner_raw  = safe_strip(raw.get("grantor", ""))
    grantee    = safe_strip(raw.get("grantee", ""))
    amount_str = safe_strip(raw.get("amount", ""))
    amount_raw = parse_amount(amount_str)

    # Normalize filed date
    filed_raw = safe_strip(raw.get("filed", ""))
    filed_norm = ""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            filed_norm = datetime.strptime(filed_raw, fmt).strftime("%Y-%m-%d")
            break
        except Exception:
            pass
    if not filed_norm:
        filed_norm = filed_raw

    # Parcel lookup
    parcel_hit = parcel.lookup(owner_raw) if owner_raw else None

    prop_address = ""
    prop_city    = "Orange County"
    prop_state   = "FL"
    prop_zip     = ""
    mail_address = ""
    mail_city    = ""
    mail_state   = ""
    mail_zip     = ""

    if parcel_hit:
        prop_address = parcel_hit.get("site_addr", "")
        prop_city    = parcel_hit.get("site_city", "") or "Orange County"
        prop_zip     = parcel_hit.get("site_zip", "")
        mail_address = parcel_hit.get("mail_addr", "")
        mail_city    = parcel_hit.get("mail_city", "")
        mail_state   = parcel_hit.get("mail_state", "")
        mail_zip     = parcel_hit.get("mail_zip", "")

    rec = {
        "doc_num":      safe_strip(raw.get("doc_num", "")),
        "doc_type":     code,
        "doc_type_raw": safe_strip(raw.get("doc_type", "")),
        "filed":        filed_norm,
        "cat":          code,
        "cat_label":    cat_label,
        "owner":        owner_raw,
        "grantee":      grantee,
        "amount":       amount_str,
        "amount_raw":   amount_raw,
        "legal":        safe_strip(raw.get("legal", "")),
        "prop_address": prop_address,
        "prop_city":    prop_city,
        "prop_state":   prop_state,
        "prop_zip":     prop_zip,
        "mail_address": mail_address,
        "mail_city":    mail_city,
        "mail_state":   mail_state,
        "mail_zip":     mail_zip,
        "clerk_url":    safe_strip(raw.get("clerk_url", "")),
        "flags":        [],
        "score":        0,
    }

    flags, score = compute_flags_score(rec, cutoff_date)
    rec["flags"] = flags
    rec["score"] = score
    return rec


# ─────────────────────────────────────────────────────────────────────────────
# GHL CSV EXPORT
# ─────────────────────────────────────────────────────────────────────────────

def export_ghl_csv(records: list[dict], out_path: Path):
    fieldnames = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed",
        "Document Number", "Amount/Debt Owed",
        "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]

    def split_name(full: str):
        parts = full.strip().split()
        if len(parts) >= 2:
            return parts[0].title(), " ".join(parts[1:]).title()
        return full.title(), ""

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            fn, ln = split_name(r.get("owner", ""))
            writer.writerow({
                "First Name":            fn,
                "Last Name":             ln,
                "Mailing Address":       r.get("mail_address", ""),
                "Mailing City":          r.get("mail_city", ""),
                "Mailing State":         r.get("mail_state", ""),
                "Mailing Zip":           r.get("mail_zip", ""),
                "Property Address":      r.get("prop_address", ""),
                "Property City":         r.get("prop_city", ""),
                "Property State":        r.get("prop_state", ""),
                "Property Zip":          r.get("prop_zip", ""),
                "Lead Type":             r.get("cat_label", ""),
                "Document Type":         r.get("doc_type", ""),
                "Date Filed":            r.get("filed", ""),
                "Document Number":       r.get("doc_num", ""),
                "Amount/Debt Owed":      r.get("amount", ""),
                "Seller Score":          r.get("score", 0),
                "Motivated Seller Flags": " | ".join(r.get("flags", [])),
                "Source":                "Orange County Clerk – myorangeclerk.com",
                "Public Records URL":    r.get("clerk_url", ""),
            })
    log.info(f"GHL CSV exported: {out_path} ({len(records)} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    now       = datetime.today()
    date_to   = now.strftime("%m/%d/%Y")
    date_from = (now - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    cutoff    = (now - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    log.info(f"Starting scrape: {date_from} → {date_to}")

    # 1. Load parcel data
    parcel = ParcelLookup()
    parcel.download_and_load()

    # 2. Fetch clerk records for each doc type
    all_records: list[dict] = []

    for code in LEAD_TYPES:
        raw_list = []

        # Try Playwright first
        try:
            raw_list = await asyncio.wait_for(
                fetch_clerk_records(code, date_from, date_to),
                timeout=90
            )
        except Exception as exc:
            log.warning(f"Playwright failed for {code}: {exc}; trying requests fallback")

        # Fallback to requests if nothing came back
        if not raw_list:
            try:
                raw_list = retry(
                    lambda: fetch_clerk_records_requests(code, date_from, date_to)
                ) or []
            except Exception as exc:
                log.error(f"requests fallback also failed for {code}: {exc}")

        log.info(f"{code}: {len(raw_list)} raw records found")

        for raw in raw_list:
            try:
                enriched = enrich(raw, code, parcel, cutoff)
                all_records.append(enriched)
            except Exception as exc:
                log.warning(f"Enrich failed for {code} record: {exc}")

    # Deduplicate by doc_num
    seen = set()
    deduped = []
    for r in all_records:
        key = r["doc_num"]
        if key and key not in seen:
            seen.add(key)
            deduped.append(r)
        elif not key:
            deduped.append(r)

    deduped.sort(key=lambda x: x["score"], reverse=True)

    with_addr = sum(1 for r in deduped if r.get("prop_address"))

    payload = {
        "fetched_at":   now.isoformat(),
        "source":       "Orange County Clerk – myorangeclerk.com",
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(deduped),
        "with_address": with_addr,
        "records":      deduped,
    }

    # Serialize (remove amount_raw internal field from output)
    for r in payload["records"]:
        r.pop("amount_raw", None)
        r.pop("doc_type_raw", None)

    # 3. Write JSON outputs
    for out_path in [DATA_DIR / "records.json", DASHBOARD_DIR / "records.json"]:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info(f"Wrote {out_path} ({len(deduped)} records)")

    # 4. GHL CSV
    export_ghl_csv(deduped, DATA_DIR / "leads_export.csv")

    log.info(
        f"✅ Done. Total: {len(deduped)} | With address: {with_addr} | "
        f"Top score: {deduped[0]['score'] if deduped else 'N/A'}"
    )


if __name__ == "__main__":
    asyncio.run(main())
