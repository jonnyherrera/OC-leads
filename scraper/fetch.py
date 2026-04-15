"""
Orange County FL – Motivated Seller Lead Scraper
Source: Orange County Comptroller Official Records Self-Service
URL: https://selfservice.or.occompt.com/ssweb/
"""

import asyncio, json, os, re, csv, time, logging, zipfile, traceback
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

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", 7))
OR_BASE       = "https://selfservice.or.occompt.com/ssweb"
OR_DISCLAIMER = f"{OR_BASE}/user/disclaimer"
OR_SEARCH     = f"{OR_BASE}/document/search"
OCPA_BULK_URL = "https://www.ocpafl.org/downloads/Parcel_Data.zip"

LEAD_TYPES = {
    "LP":("Lis Pendens","Pre-Foreclosure"),
    "NOFC":("Notice of Foreclosure","Pre-Foreclosure"),
    "TAXDEED":("Tax Deed","Tax / Govt Lien"),
    "JUD":("Judgment","Judgment Lien"),
    "CCJ":("Certified Judgment","Judgment Lien"),
    "DRJUD":("Domestic Judgment","Judgment Lien"),
    "LNCORPTX":("Corp Tax Lien","Tax / Govt Lien"),
    "LNIRS":("IRS Lien","Tax / Govt Lien"),
    "LNFED":("Federal Lien","Tax / Govt Lien"),
    "LN":("Lien","General Lien"),
    "LNMECH":("Mechanic Lien","Mechanic Lien"),
    "LNHOA":("HOA Lien","HOA Lien"),
    "MEDLN":("Medicaid Lien","Govt / Medical Lien"),
    "PRO":("Probate Document","Probate"),
    "NOC":("Notice of Commencement","NOC"),
    "RELLP":("Release Lis Pendens","Release"),
}

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR.mkdir(exist_ok=True)
DASHBOARD_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120"})

def safe_strip(v) -> str: return str(v).strip() if v else ""
def parse_amount(raw):
    try: return float(re.sub(r"[^\d.]","",raw)) or None
    except: return None
def retry(fn, attempts=3, delay=4):
    for i in range(attempts):
        try: return fn()
        except Exception as e:
            log.warning(f"Attempt {i+1} failed: {e}")
            if i < attempts-1: time.sleep(delay*(i+1))

# ── PARCEL ────────────────────────────────────────────────────────────────────
class ParcelLookup:
    def __init__(self): self.by_owner = {}
    def _norm(self,n): return re.sub(r"\s+"," ",n.upper().strip())
    def _variants(self,raw):
        n = self._norm(raw); v = [n]
        if "," in n:
            p = [x.strip() for x in n.split(",",1)]
            if len(p)==2: v.append(f"{p[1]} {p[0]}")
        else:
            p = n.split()
            if len(p)>=2: v.append(f"{p[-1]}, {' '.join(p[:-1])}")
        return list(dict.fromkeys(v))
    def _load_dbf(self, path):
        if not HAS_DBF: return
        col_map = {"owner":["OWNER","OWN1"],"site_addr":["SITE_ADDR","SITEADDR"],
                   "site_city":["SITE_CITY","SITECITY"],"site_zip":["SITE_ZIP","SITEZIP"],
                   "mail_addr":["ADDR_1","MAILADR1"],"mail_city":["CITY","MAILCITY"],
                   "mail_state":["STATE","MAILSTATE"],"mail_zip":["ZIP","MAILZIP"]}
        try:
            tbl = DBF(str(path), ignore_missing_memofile=True)
            fields = [f.name.upper() for f in tbl.fields]
            def fc(keys):
                for k in keys:
                    if k in fields: return k
            cols = {k:fc(v) for k,v in col_map.items()}
            cnt = 0
            for rec in tbl:
                try:
                    o = safe_strip(rec.get(cols["owner"]) if cols["owner"] else "")
                    if not o: continue
                    p = {k:safe_strip(rec.get(cols[k]) if cols[k] else "") for k in col_map}
                    for v in self._variants(o): self.by_owner.setdefault(v,p)
                    cnt += 1
                except: pass
            log.info(f"Loaded {cnt:,} parcels")
        except Exception as e: log.error(f"DBF: {e}")
    def download_and_load(self):
        zp = DATA_DIR / "parcel_data.zip"
        def dl():
            r = SESSION.get(OCPA_BULK_URL, timeout=120, stream=True)
            r.raise_for_status()
            with open(zp,"wb") as f:
                for ch in r.iter_content(65536): f.write(ch)
            mb = zp.stat().st_size/1_048_576
            if mb < 0.5: raise ValueError(f"too small: {mb:.1f}MB")
            log.info(f"Parcel ZIP: {mb:.1f}MB")
        if zp.exists() and (time.time()-zp.stat().st_mtime)<72000:
            log.info("Cached parcel ZIP")
        else:
            retry(dl,3,5)
            if not zp.exists(): return
        try:
            with zipfile.ZipFile(zp) as z:
                dbfs = [n for n in z.namelist() if n.upper().endswith(".DBF")]
                if dbfs:
                    dp = DATA_DIR / Path(dbfs[0]).name
                    with z.open(dbfs[0]) as s, open(dp,"wb") as d: d.write(s.read())
                    self._load_dbf(dp)
        except Exception as e: log.error(f"ZIP: {e}")
    def lookup(self, owner):
        for v in self._variants(owner):
            if v in self.by_owner: return self.by_owner[v]

# ── BROWSER HELPERS ───────────────────────────────────────────────────────────

async def ss(page, name):
    try: await page.screenshot(path=str(DATA_DIR/f"debug_{name}.png"), full_page=True)
    except: pass

async def wait_for_any(page, selectors, timeout=10000):
    """Wait until any of the selectors becomes visible. Returns the first found."""
    deadline = asyncio.get_event_loop().time() + timeout/1000
    while asyncio.get_event_loop().time() < deadline:
        for sel in selectors:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=500):
                    return sel
            except: pass
        await asyncio.sleep(0.5)
    return None

async def try_fill(page, selectors, value):
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if await el.is_visible(timeout=2000):
                tag = await el.evaluate("e=>e.tagName")
                if tag == "SELECT":
                    await page.select_option(sel, value=value, timeout=3000)
                else:
                    await el.triple_click(timeout=2000)
                    await el.type(value, delay=50)
                log.info(f"  Filled '{value}' -> {sel}")
                return True
        except: pass
    return False

async def try_click(page, selectors):
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if await el.is_visible(timeout=2000):
                await el.click(timeout=3000)
                log.info(f"  Clicked: {sel}")
                return True
        except: pass
    return False

# ── ACCEPT DISCLAIMER ─────────────────────────────────────────────────────────

async def accept_disclaimer(page):
    await page.goto(OR_DISCLAIMER, timeout=30000, wait_until="domcontentloaded")
    await asyncio.sleep(3)
    await ss(page, "01_disclaimer")

    # Click any accept button
    clicked = await try_click(page, [
        "input[value='Accept']", "input[value*='ccept']",
        "button:has-text('Accept')", "a:has-text('Accept')",
        "input[type='submit']", "button[type='submit']",
    ])
    if clicked:
        await asyncio.sleep(2)
        log.info("Disclaimer accepted via click")
        return

    # JS POST fallback
    status = await page.evaluate("""
        async () => {
            const r = await fetch('/ssweb/user/disclaimer', {
                method:'POST',
                headers:{'Content-Type':'application/x-www-form-urlencoded','ajaxRequest':'true'},
                body:'accept=Accept'
            });
            return r.status;
        }
    """)
    log.info(f"Disclaimer JS POST: {status}")
    await asyncio.sleep(1)

# ── SEARCH ONE DOC TYPE ───────────────────────────────────────────────────────

async def search_doc_type(page, code, date_from, date_to):
    records = []
    log.info(f"Searching {code} | {date_from} - {date_to}")

    try:
        await page.goto(OR_SEARCH, timeout=30000, wait_until="domcontentloaded")

        # Wait for jQuery Mobile to finish rendering (key fix)
        # jQM adds data-role="page" and then initializes — wait for any input to appear
        found_sel = await wait_for_any(page, [
            "input", "select", "a[data-role='button']",
            "[data-role='listview']", ".ui-content",
            "#searchButton", "form"
        ], timeout=12000)
        log.info(f"  Page ready signal: {found_sel}")
        await asyncio.sleep(2)
        await ss(page, f"02_{code}_loaded")

        # Dump full page HTML for first run to see what we're working with
        if code == "LP":
            html = await page.content()
            with open(DATA_DIR/"debug_page_source.html","w") as f:
                f.write(html)
            log.info("  Saved full page source to debug_page_source.html")

        # Log ALL inputs including hidden ones
        all_inputs = await page.evaluate("""
            () => Array.from(document.querySelectorAll('input,select,textarea,a,button'))
                .map(e => ({
                    tag:e.tagName, id:e.id||'', name:e.name||'',
                    type:e.getAttribute('type')||'',
                    placeholder:e.placeholder||'',
                    text:(e.textContent||'').trim().substring(0,40),
                    href:e.href||'',
                    dataRole:e.getAttribute('data-role')||'',
                    dataVal:e.getAttribute('data-val')||'',
                    visible: e.offsetWidth > 0 && e.offsetHeight > 0,
                    class:(e.className||'').substring(0,60)
                }))
        """)
        log.info(f"  All elements ({len(all_inputs)}): {json.dumps(all_inputs[:30])}")

        # Try to click "Document Type" tab
        await try_click(page, [
            "a[data-val='dt']", "li[data-val='dt'] a",
            "a:has-text('Document Type')", "a:has-text('Instrument')",
            "[href*='searchType=dt']", "a:has-text('Type Search')",
        ])
        await asyncio.sleep(2)

        # Fill fields
        await try_fill(page, [
            "#field_DocType","input[name='field_DocType']",
            "input[name*='ocType']","input[id*='ocType']",
            "input[placeholder*='Type']","select[name*='ocType']"
        ], code)

        await try_fill(page, [
            "#field_DocBeginDate","input[name='field_DocBeginDate']",
            "input[name*='BeginDate']","input[placeholder*='Begin']",
            "input[placeholder*='Start']","input[placeholder*='From']"
        ], date_from)

        await try_fill(page, [
            "#field_DocEndDate","input[name='field_DocEndDate']",
            "input[name*='EndDate']","input[placeholder*='End']",
            "input[placeholder*='Thru']","input[placeholder*='To']"
        ], date_to)

        await asyncio.sleep(1)
        await ss(page, f"03_{code}_filled")

        # Click search
        await try_click(page, [
            "#searchButton","a#searchButton",
            "button:has-text('Search')","a:has-text('Search')",
            "input[value='Search']","[data-role='button']:has-text('Search')"
        ])

        await asyncio.sleep(5)
        try: await page.wait_for_load_state("networkidle", timeout=20000)
        except: pass
        await ss(page, f"04_{code}_results")

        # Collect pages
        pnum = 0
        while True:
            pnum += 1
            html = await page.content()
            batch = _parse_results(html, code)
            records.extend(batch)
            log.info(f"  {code} p{pnum}: {len(batch)}")
            nxt = await try_click(page, [
                "a:has-text('Next')","button:has-text('Next')",
                "[aria-label='Next page']",".next-page"
            ])
            if not nxt or pnum > 50: break
            await asyncio.sleep(2)
            try: await page.wait_for_load_state("networkidle", timeout=15000)
            except: pass

    except Exception as e:
        log.error(f"{code}: {e}\n{traceback.format_exc()}")
    return records

def _parse_results(html, doc_type):
    soup = BeautifulSoup(html,"lxml")
    rows = []
    for tbl in soup.find_all("table"):
        hdrs = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if not hdrs: continue
        joined = " ".join(hdrs)
        if not any(k in joined for k in ["instrument","doc","book","date","filed","grantor","type"]): continue
        hmap = {}
        for i,h in enumerate(hdrs):
            if any(k in h for k in ["instrument","doc num","book","number","doc #"]): hmap.setdefault("doc_num",i)
            elif "type" in h: hmap.setdefault("doc_type",i)
            elif any(k in h for k in ["record","filed","date"]): hmap.setdefault("filed",i)
            elif "grantor" in h: hmap.setdefault("grantor",i)
            elif "grantee" in h: hmap.setdefault("grantee",i)
            elif any(k in h for k in ["amount","consider"]): hmap.setdefault("amount",i)
            elif "legal" in h: hmap.setdefault("legal",i)
        for tr in tbl.find_all("tr")[1:]:
            cells = tr.find_all(["td","th"])
            if len(cells)<2: continue
            texts = [c.get_text(strip=True) for c in cells]
            def get(k): idx=hmap.get(k); return texts[idx] if idx is not None and idx<len(texts) else ""
            link = next((a["href"] if a["href"].startswith("http") else f"https://selfservice.or.occompt.com{a['href']}"
                        for a in tr.find_all("a",href=True) if any(k in a["href"].lower() for k in ["doc","instrument","detail","view"])), "")
            num = get("doc_num") or (texts[0] if texts else "")
            if not num or num.lower() in ("","instrument","doc #","#"): continue
            rows.append({"doc_num":num,"doc_type":get("doc_type") or doc_type,"filed":get("filed"),
                         "grantor":get("grantor"),"grantee":get("grantee"),"amount":get("amount"),
                         "legal":get("legal"),"clerk_url":link})
    return rows

# ── SCORE / ENRICH / CSV (unchanged) ─────────────────────────────────────────

def compute_score(rec):
    flags,score=[],30
    dt=rec.get("doc_type",""); owner=rec.get("owner","").upper()
    amt=rec.get("amount_raw"); filed=rec.get("filed","")
    if dt=="LP": flags.append("Lis pendens"); score+=30
    if dt in("LP","NOFC"): flags.append("Pre-foreclosure"); score+=10
    if dt in("JUD","CCJ","DRJUD"): flags.append("Judgment lien"); score+=10
    if dt in("TAXDEED","LNCORPTX","LNIRS","LNFED"): flags.append("Tax lien"); score+=10
    if dt=="LNMECH": flags.append("Mechanic lien"); score+=10
    if dt=="PRO": flags.append("Probate / estate"); score+=10
    if any(k in owner for k in ["LLC","INC","CORP","LTD","TRUST"]): flags.append("LLC / corp owner"); score+=10
    if amt and amt>100000: score+=15; flags.append("High debt (>$100k)")
    elif amt and amt>50000: score+=10; flags.append("Significant debt (>$50k)")
    try:
        if (datetime.today()-datetime.strptime(filed[:10],"%Y-%m-%d")).days<=7: score+=5; flags.append("New this week")
    except: pass
    if rec.get("prop_address"): score+=5; flags.append("Has address")
    return list(dict.fromkeys(flags)),min(score,100)

def enrich(raw, code, parcel):
    _,cat_label=LEAD_TYPES.get(code,(code,"Other"))
    owner_raw=safe_strip(raw.get("grantor",""))
    amount_str=safe_strip(raw.get("amount",""))
    amount_raw=parse_amount(amount_str)
    filed_raw=safe_strip(raw.get("filed",""))
    filed_norm=""
    for fmt in ("%m/%d/%Y","%Y-%m-%d","%m-%d-%Y"):
        try: filed_norm=datetime.strptime(filed_raw,fmt).strftime("%Y-%m-%d"); break
        except: pass
    if not filed_norm: filed_norm=filed_raw
    ph=parcel.lookup(owner_raw) if owner_raw else None
    rec={"doc_num":safe_strip(raw.get("doc_num","")),"doc_type":code,"filed":filed_norm,
         "cat":code,"cat_label":cat_label,"owner":owner_raw,"grantee":safe_strip(raw.get("grantee","")),
         "amount":amount_str,"amount_raw":amount_raw,"legal":safe_strip(raw.get("legal","")),
         "prop_address":ph.get("site_addr","") if ph else "","prop_city":ph.get("site_city","") if ph else "Orange County",
         "prop_state":"FL","prop_zip":ph.get("site_zip","") if ph else "",
         "mail_address":ph.get("mail_addr","") if ph else "","mail_city":ph.get("mail_city","") if ph else "",
         "mail_state":ph.get("mail_state","") if ph else "","mail_zip":ph.get("mail_zip","") if ph else "",
         "clerk_url":safe_strip(raw.get("clerk_url","")),"flags":[],"score":0}
    rec["flags"],rec["score"]=compute_score(rec)
    return rec

def export_csv(records, out_path):
    cols=["First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
          "Property Address","Property City","Property State","Property Zip","Lead Type","Document Type",
          "Date Filed","Document Number","Amount/Debt Owed","Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    with open(out_path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=cols); w.writeheader()
        for r in records:
            parts=(r.get("owner","")).split()
            w.writerow({"First Name":parts[0].title() if parts else "","Last Name":" ".join(parts[1:]).title() if len(parts)>1 else "",
                "Mailing Address":r.get("mail_address",""),"Mailing City":r.get("mail_city",""),
                "Mailing State":r.get("mail_state",""),"Mailing Zip":r.get("mail_zip",""),
                "Property Address":r.get("prop_address",""),"Property City":r.get("prop_city",""),
                "Property State":r.get("prop_state","FL"),"Property Zip":r.get("prop_zip",""),
                "Lead Type":r.get("cat_label",""),"Document Type":r.get("doc_type",""),
                "Date Filed":r.get("filed",""),"Document Number":r.get("doc_num",""),
                "Amount/Debt Owed":r.get("amount",""),"Seller Score":r.get("score",0),
                "Motivated Seller Flags":" | ".join(r.get("flags",[])),"Source":"Orange County Comptroller OR",
                "Public Records URL":r.get("clerk_url","")})
    log.info(f"CSV: {out_path} ({len(records)} rows)")

# ── MAIN ──────────────────────────────────────────────────────────────────────

async def main():
    now=datetime.today()
    date_to=now.strftime("%m/%d/%Y")
    date_from=(now-timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Run: {date_from} → {date_to}")

    parcel=ParcelLookup()
    parcel.download_and_load()

    all_records=[]
    async with async_playwright() as pw:
        browser=await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-blink-features=AutomationControlled"]
        )
        ctx=await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            viewport={"width":1280,"height":900}
        )
        page=await ctx.new_page()
        await accept_disclaimer(page)

        for code in LEAD_TYPES:
            try:
                raws=await asyncio.wait_for(search_doc_type(page,code,date_from,date_to),timeout=120)
            except asyncio.TimeoutError:
                log.warning(f"Timeout: {code}"); raws=[]
            except Exception as e:
                log.error(f"{code}: {e}"); raws=[]
            log.info(f"{code}: {len(raws)} raw")
            for raw in raws:
                try: all_records.append(enrich(raw,code,parcel))
                except Exception as e: log.warning(f"Enrich: {e}")

        await browser.close()

    seen,deduped=set(),[]
    for r in all_records:
        k=r["doc_num"]
        if k and k not in seen: seen.add(k); deduped.append(r)
        elif not k: deduped.append(r)
    deduped.sort(key=lambda x:x["score"],reverse=True)
    for r in deduped: r.pop("amount_raw",None)

    with_addr=sum(1 for r in deduped if r.get("prop_address"))
    payload={"fetched_at":now.isoformat(),"source":"Orange County Comptroller OR – occompt.com",
             "date_range":{"from":date_from,"to":date_to},"total":len(deduped),
             "with_address":with_addr,"records":deduped}

    for p in [DATA_DIR/"records.json",DASHBOARD_DIR/"records.json"]:
        with open(p,"w",encoding="utf-8") as f: json.dump(payload,f,indent=2,default=str)
        log.info(f"Wrote {p}")
    export_csv(deduped,DATA_DIR/"leads_export.csv")
    log.info(f"Done. {len(deduped)} records | {with_addr} with address")

if __name__=="__main__":
    asyncio.run(main())
