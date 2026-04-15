"""
Orange County FL – Motivated Seller Lead Scraper
Source: Orange County Comptroller Official Records Self-Service
URL: https://selfservice.or.occompt.com/ssweb/
"""

import asyncio
import json
import os
import re
import csv
import time
import logging
import zipfile
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

# ── CONFIG ────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", 7))
OR_BASE       = "https://selfservice.or.occompt.com/ssweb"
OR_DISCLAIMER = f"{OR_BASE}/user/disclaimer"
OR_SEARCH     = f"{OR_BASE}/document/search"
OCPA_BULK_URL = "https://www.ocpafl.org/downloads/Parcel_Data.zip"

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

ROOT          = Path(__file__).resolve().parent.parent
DATA_DIR      = ROOT / "data"
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR.mkdir(exist_ok=True)
DASHBOARD_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120 Safari/537.36"
})

# ── HELPERS ───────────────────────────────────────────────────────────────────

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

# ── PARCEL LOOKUP ─────────────────────────────────────────────────────────────

class ParcelLookup:
    def __init__(self):
        self.by_owner: dict = {}

    def _norm(self, name: str) -> str:
        return re.sub(r"\s+", " ", name.upper().strip())

    def _variants(self, raw: str) -> list:
        name = self._norm(raw)
        variants = [name]
        if "," in name:
            parts = [p.strip() for p in name.split(",", 1)]
            if len(parts) == 2:
                variants.append(f"{parts[1]} {parts[0]}")
        else:
            parts = name.split()
            if len(parts) >= 2:
                variants.append(f"{parts[-1]}, {' '.join(parts[:-1])}")
        return list(dict.fromkeys(variants))

    def _load_dbf(self, dbf_path: Path):
        if not HAS_DBF:
            return
        log.info(f"Loading DBF: {dbf_path}")
        col_map = {
            "owner":      ["OWNER", "OWN1"],
            "site_addr":  ["SITE_ADDR", "SITEADDR"],
            "site_city":  ["SITE_CITY", "SITECITY"],
            "site_zip":   ["SITE_ZIP", "SITEZIP"],
            "mail_addr":  ["ADDR_1", "MAILADR1"],
            "mail_city":  ["CITY", "MAILCITY"],
            "mail_state": ["STATE", "MAILSTATE"],
            "mail_zip":   ["ZIP", "MAILZIP"],
        }
        try:
            table = DBF(str(dbf_path), ignore_missing_memofile=True)
            fields = [f.name.upper() for f in table.fields]
            def fc(keys):
                for k in keys:
                    if k in fields: return k
                return None
            cols = {k: fc(v) for k, v in col_map.items()}
            count = 0
            for rec in table:
                try:
                    owner_raw = safe_strip(rec.get(cols["owner"]) if cols["owner"] else "")
                    if not owner_raw: continue
                    parcel = {k: safe_strip(rec.get(cols[k]) if cols[k] else "") for k in col_map}
                    for v in self._variants(owner_raw):
                        self.by_owner.setdefault(v, parcel)
                    count += 1
                except Exception:
                    pass
            log.info(f"Loaded {count:,} parcels")
        except Exception as exc:
            log.error(f"DBF error: {exc}")

    def download_and_load(self):
        zip_path = DATA_DIR / "parcel_data.zip"
        def do_dl():
            r = SESSION.get(OCPA_BULK_URL, timeout=120, stream=True)
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(65536): f.write(chunk)
            mb = zip_path.stat().st_size / 1_048_576
            if mb < 0.5: raise ValueError(f"ZIP too small ({mb:.1f}MB)")
            log.info(f"Parcel ZIP: {mb:.1f}MB")
        if zip_path.exists() and (time.time() - zip_path.stat().st_mtime) < 72_000:
            log.info("Using cached parcel ZIP")
        else:
            retry(do_dl, 3, 5)
            if not zip_path.exists(): return
        try:
            with zipfile.ZipFile(zip_path) as zf:
                dbf_files = [n for n in zf.namelist() if n.upper().endswith(".DBF")]
                if dbf_files:
                    dbf_path = DATA_DIR / Path(dbf_files[0]).name
                    with zf.open(dbf_files[0]) as src, open(dbf_path, "wb") as dst:
                        dst.write(src.read())
                    self._load_dbf(dbf_path)
        except Exception as exc:
            log.error(f"ZIP error: {exc}")

    def lookup(self, owner: str) -> Optional[dict]:
        for v in self._variants(owner):
            if v in self.by_owner: return self.by_owner[v]
        return None

# ── PLAYWRIGHT: SCREENSHOT DEBUG ──────────────────────────────────────────────

async def debug_screenshot(page, name: str):
    try:
        path = DATA_DIR / f"debug_{name}.png"
        await page.screenshot(path=str(path))
        log.info(f"Screenshot saved: {path}")
    except Exception:
        pass

# ── PLAYWRIGHT: ACCEPT DISCLAIMER ─────────────────────────────────────────────

async def accept_disclaimer(page) -> bool:
    try:
        await page.goto(OR_DISCLAIMER, timeout=30_000, wait_until="domcontentloaded")
        await asyncio.sleep(3)
        await debug_screenshot(page, "01_disclaimer")

        # Try clicking Accept button
        for sel in [
            "input[value='Accept']", "input[value*='Accept']",
            "button:has-text('Accept')", "a:has-text('Accept')",
            "input[type='submit']", "button[type='submit']",
            "#SelfService-accept", "[data-val='accept']"
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2_000):
                    await el.click(timeout=5_000)
                    await asyncio.sleep(2)
                    log.info(f"Disclaimer clicked: {sel}")
                    return True
            except Exception:
                pass

        # JS POST fallback
        result = await page.evaluate("""
            async () => {
                const r = await fetch('/ssweb/user/disclaimer', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'ajaxRequest': 'true'
                    },
                    body: 'accept=Accept'
                });
                return r.status;
            }
        """)
        log.info(f"Disclaimer JS POST status: {result}")
        await asyncio.sleep(1)
        return True
    except Exception as exc:
        log.warning(f"Disclaimer error: {exc}")
        return False

# ── PLAYWRIGHT: SEARCH ONE DOC TYPE ──────────────────────────────────────────

async def search_doc_type(page, code: str, date_from: str, date_to: str) -> list:
    records = []
    log.info(f"Searching {code} | {date_from} - {date_to}")
    try:
        # Navigate to search page
        await page.goto(OR_SEARCH, timeout=30_000, wait_until="domcontentloaded")
        await asyncio.sleep(4)

        # Wait for the page body to settle
        try:
            await page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass

        await debug_screenshot(page, f"02_search_{code}")

        # Log all visible input fields for debugging
        inputs = await page.evaluate("""
            () => {
                const els = document.querySelectorAll('input, select, a[data-val], li[data-val]');
                return Array.from(els).map(e => ({
                    tag: e.tagName,
                    id: e.id,
                    name: e.name,
                    type: e.type,
                    placeholder: e.placeholder,
                    value: e.value,
                    dataVal: e.getAttribute('data-val'),
                    visible: e.offsetParent !== null
                }));
            }
        """)
        log.info(f"Page inputs for {code}: {json.dumps(inputs[:20])}")

        # Try clicking Document Type / Instrument Type search tab
        for sel in [
            "a[data-val='dt']", "li[data-val='dt'] a",
            "a:has-text('Document Type')", "a:has-text('Instrument Type')",
            "label:has-text('Document Type')", "[href*='dt']",
            "a:has-text('Type')"
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2_000):
                    await el.click(timeout=3_000)
                    await asyncio.sleep(2)
                    log.info(f"  Tab clicked: {sel}")
                    break
            except Exception:
                pass

        # Fill doc type
        for sel in [
            "#field_DocType", "input[name='field_DocType']",
            "input[name*='DocType']", "input[id*='DocType']",
            "input[placeholder*='Type']", "input[placeholder*='Instrument']",
            "select[name*='DocType']", "input[data-field='DocType']"
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2_000):
                    tag = await el.evaluate("e => e.tagName")
                    if tag == "SELECT":
                        await page.select_option(sel, value=code, timeout=3_000)
                    else:
                        await el.triple_click(timeout=3_000)
                        await el.fill(code, timeout=3_000)
                    log.info(f"  Doc type filled via {sel}")
                    break
            except Exception:
                pass

        # Fill begin date
        for sel in [
            "#field_DocBeginDate", "input[name='field_DocBeginDate']",
            "input[name*='BeginDate']", "input[name*='beginDate']",
            "input[placeholder*='Begin']", "input[placeholder*='Start']",
            "input[data-field='DocBeginDate']"
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2_000):
                    await el.triple_click(timeout=3_000)
                    await el.fill(date_from, timeout=3_000)
                    log.info(f"  Begin date filled via {sel}")
                    break
            except Exception:
                pass

        # Fill end date
        for sel in [
            "#field_DocEndDate", "input[name='field_DocEndDate']",
            "input[name*='EndDate']", "input[name*='endDate']",
            "input[placeholder*='End']", "input[placeholder*='Thru']",
            "input[data-field='DocEndDate']"
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2_000):
                    await el.triple_click(timeout=3_000)
                    await el.fill(date_to, timeout=3_000)
                    log.info(f"  End date filled via {sel}")
                    break
            except Exception:
                pass

        await asyncio.sleep(1)
        await debug_screenshot(page, f"03_filled_{code}")

        # Click search
        for sel in [
            "#searchButton", "a#searchButton",
            "button:has-text('Search')", "a:has-text('Search')",
            "input[value='Search']", "[data-role='button']:has-text('Search')"
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=2_000):
                    await el.click(timeout=5_000)
                    log.info(f"  Search clicked via {sel}")
                    break
            except Exception:
                pass

        await asyncio.sleep(5)
        try:
            await page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass

        await debug_screenshot(page, f"04_results_{code}")

        # Paginate
        page_num = 0
        while True:
            page_num += 1
            html = await page.content()
            batch = _parse_results(html, code)
            records.extend(batch)
            log.info(f"  {code} p{page_num}: {len(batch)}")

            next_found = False
            for sel in ["a:has-text('Next')", "button:has-text('Next')",
                        "[aria-label='Next page']", ".next-page", "a[title='Next']"]:
                try:
                    btn = page.locator(sel).first
                    vis = await btn.is_visible(timeout=1_500)
                    cls = await btn.get_attribute("class") or ""
                    dis = await btn.get_attribute("disabled")
                    if vis and "disabled" not in cls.lower() and dis is None:
                        await btn.click(timeout=5_000)
                        await page.wait_for_load_state("networkidle", timeout=15_000)
                        await asyncio.sleep(1)
                        next_found = True
                        break
                except Exception:
                    pass

            if not next_found or page_num > 50:
                break

    except Exception as exc:
        log.error(f"Error {code}: {exc}\n{traceback.format_exc()}")
    return records


def _parse_results(html: str, doc_type: str) -> list:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for tbl in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if not headers: continue
        if not any(k in " ".join(headers) for k in
                   ["instrument","doc","book","date","filed","grantor","type"]): continue
        hmap = {}
        for i, h in enumerate(headers):
            if any(k in h for k in ["instrument","doc num","book","number","doc #"]):
                hmap.setdefault("doc_num", i)
            elif "type" in h: hmap.setdefault("doc_type", i)
            elif any(k in h for k in ["record","filed","date"]): hmap.setdefault("filed", i)
            elif "grantor" in h: hmap.setdefault("grantor", i)
            elif "grantee" in h: hmap.setdefault("grantee", i)
            elif any(k in h for k in ["amount","consider"]): hmap.setdefault("amount", i)
            elif "legal" in h: hmap.setdefault("legal", i)
        for tr in tbl.find_all("tr")[1:]:
            cells = tr.find_all(["td","th"])
            if len(cells) < 2: continue
            texts = [c.get_text(strip=True) for c in cells]
            def get(key):
                idx = hmap.get(key)
                return texts[idx] if idx is not None and idx < len(texts) else ""
            link = ""
            for a in tr.find_all("a", href=True):
                href = a["href"]
                if any(k in href.lower() for k in ["doc","instrument","detail","view"]):
                    link = href if href.startswith("http") else f"https://selfservice.or.occompt.com{href}"
                    break
            num = get("doc_num") or (texts[0] if texts else "")
            if not num or num.lower() in ("","instrument","doc #","#"): continue
            rows.append({
                "doc_num": num, "doc_type": get("doc_type") or doc_type,
                "filed": get("filed"), "grantor": get("grantor"),
                "grantee": get("grantee"), "amount": get("amount"),
                "legal": get("legal"), "clerk_url": link,
            })
    return rows

# ── SCORE ─────────────────────────────────────────────────────────────────────

def compute_score(rec: dict) -> tuple:
    flags, score = [], 30
    dt    = rec.get("doc_type","")
    owner = rec.get("owner","").upper()
    amt   = rec.get("amount_raw")
    filed = rec.get("filed","")
    if dt == "LP": flags.append("Lis pendens"); score += 30
    if dt in ("LP","NOFC"): flags.append("Pre-foreclosure"); score += 10
    if dt in ("JUD","CCJ","DRJUD"): flags.append("Judgment lien"); score += 10
    if dt in ("TAXDEED","LNCORPTX","LNIRS","LNFED"): flags.append("Tax lien"); score += 10
    if dt == "LNMECH": flags.append("Mechanic lien"); score += 10
    if dt == "PRO": flags.append("Probate / estate"); score += 10
    if any(k in owner for k in ["LLC","INC","CORP","LTD","TRUST"]): flags.append("LLC / corp owner"); score += 10
    if amt and amt > 100_000: score += 15; flags.append("High debt (>$100k)")
    elif amt and amt > 50_000: score += 10; flags.append("Significant debt (>$50k)")
    try:
        if (datetime.today() - datetime.strptime(filed[:10],"%Y-%m-%d")).days <= 7:
            score += 5; flags.append("New this week")
    except Exception: pass
    if rec.get("prop_address"): score += 5; flags.append("Has address")
    return list(dict.fromkeys(flags)), min(score, 100)

# ── ENRICH ────────────────────────────────────────────────────────────────────

def enrich(raw: dict, code: str, parcel: ParcelLookup) -> dict:
    _, cat_label = LEAD_TYPES.get(code, (code, "Other"))
    owner_raw = safe_strip(raw.get("grantor",""))
    amount_str = safe_strip(raw.get("amount",""))
    amount_raw = parse_amount(amount_str)
    filed_raw = safe_strip(raw.get("filed",""))
    filed_norm = ""
    for fmt in ("%m/%d/%Y","%Y-%m-%d","%m-%d-%Y"):
        try: filed_norm = datetime.strptime(filed_raw, fmt).strftime("%Y-%m-%d"); break
        except Exception: pass
    if not filed_norm: filed_norm = filed_raw
    ph = parcel.lookup(owner_raw) if owner_raw else None
    rec = {
        "doc_num": safe_strip(raw.get("doc_num","")),
        "doc_type": code, "filed": filed_norm,
        "cat": code, "cat_label": cat_label,
        "owner": owner_raw, "grantee": safe_strip(raw.get("grantee","")),
        "amount": amount_str, "amount_raw": amount_raw,
        "legal": safe_strip(raw.get("legal","")),
        "prop_address": ph.get("site_addr","") if ph else "",
        "prop_city": ph.get("site_city","") if ph else "Orange County",
        "prop_state": "FL",
        "prop_zip": ph.get("site_zip","") if ph else "",
        "mail_address": ph.get("mail_addr","") if ph else "",
        "mail_city": ph.get("mail_city","") if ph else "",
        "mail_state": ph.get("mail_state","") if ph else "",
        "mail_zip": ph.get("mail_zip","") if ph else "",
        "clerk_url": safe_strip(raw.get("clerk_url","")),
        "flags": [], "score": 0,
    }
    rec["flags"], rec["score"] = compute_score(rec)
    return rec

# ── CSV ───────────────────────────────────────────────────────────────────────

def export_csv(records: list, out_path: Path):
    cols = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State",
            "Mailing Zip","Property Address","Property City","Property State","Property Zip",
            "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
            "Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    with open(out_path,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in records:
            parts = (r.get("owner","")).split()
            fn = parts[0].title() if parts else ""
            ln = " ".join(parts[1:]).title() if len(parts)>1 else ""
            w.writerow({
                "First Name":fn,"Last Name":ln,
                "Mailing Address":r.get("mail_address",""),
                "Mailing City":r.get("mail_city",""),
                "Mailing State":r.get("mail_state",""),
                "Mailing Zip":r.get("mail_zip",""),
                "Property Address":r.get("prop_address",""),
                "Property City":r.get("prop_city",""),
                "Property State":r.get("prop_state","FL"),
                "Property Zip":r.get("prop_zip",""),
                "Lead Type":r.get("cat_label",""),
                "Document Type":r.get("doc_type",""),
                "Date Filed":r.get("filed",""),
                "Document Number":r.get("doc_num",""),
                "Amount/Debt Owed":r.get("amount",""),
                "Seller Score":r.get("score",0),
                "Motivated Seller Flags":" | ".join(r.get("flags",[])),
                "Source":"Orange County Comptroller OR – occompt.com",
                "Public Records URL":r.get("clerk_url",""),
            })
    log.info(f"CSV: {out_path} ({len(records)} rows)")

# ── MAIN ──────────────────────────────────────────────────────────────────────

async def main():
    now       = datetime.today()
    date_to   = now.strftime("%m/%d/%Y")
    date_from = (now - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Run: {date_from} → {date_to}")

    parcel = ParcelLookup()
    parcel.download_and_load()

    all_records = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"]
        )
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120 Safari/537.36",
            viewport={"width":1280,"height":800},
        )
        page = await ctx.new_page()
        await accept_disclaimer(page)

        for code in LEAD_TYPES:
            try:
                raws = await asyncio.wait_for(
                    search_doc_type(page, code, date_from, date_to),
                    timeout=120
                )
            except asyncio.TimeoutError:
                log.warning(f"Timeout: {code}")
                raws = []
            except Exception as exc:
                log.error(f"{code}: {exc}")
                raws = []
            log.info(f"{code}: {len(raws)} raw records")
            for raw in raws:
                try: all_records.append(enrich(raw, code, parcel))
                except Exception as exc: log.warning(f"Enrich {code}: {exc}")

        await browser.close()

    seen, deduped = set(), []
    for r in all_records:
        k = r["doc_num"]
        if k and k not in seen: seen.add(k); deduped.append(r)
        elif not k: deduped.append(r)
    deduped.sort(key=lambda x: x["score"], reverse=True)
    for r in deduped: r.pop("amount_raw", None)

    with_addr = sum(1 for r in deduped if r.get("prop_address"))
    payload = {
        "fetched_at": now.isoformat(),
        "source": "Orange County Comptroller Official Records – occompt.com",
        "date_range": {"from": date_from, "to": date_to},
        "total": len(deduped), "with_address": with_addr,
        "records": deduped,
    }
    for p in [DATA_DIR/"records.json", DASHBOARD_DIR/"records.json"]:
        with open(p,"w",encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        log.info(f"Wrote {p}")
    export_csv(deduped, DATA_DIR/"leads_export.csv")
    log.info(f"Done. {len(deduped)} records | {with_addr} with address")

if __name__ == "__main__":
    asyncio.run(main())
