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
SESSION.headers.update({"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) Chrome/120"})

def safe_strip(v): return str(v).strip() if v else ""
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
        n=self._norm(raw); v=[n]
        if "," in n:
            p=[x.strip() for x in n.split(",",1)]
            if len(p)==2: v.append(f"{p[1]} {p[0]}")
        else:
            p=n.split()
            if len(p)>=2: v.append(f"{p[-1]}, {' '.join(p[:-1])}")
        return list(dict.fromkeys(v))
    def _load_dbf(self,path):
        if not HAS_DBF: return
        col_map={"owner":["OWNER","OWN1"],"site_addr":["SITE_ADDR","SITEADDR"],
                 "site_city":["SITE_CITY","SITECITY"],"site_zip":["SITE_ZIP","SITEZIP"],
                 "mail_addr":["ADDR_1","MAILADR1"],"mail_city":["CITY","MAILCITY"],
                 "mail_state":["STATE","MAILSTATE"],"mail_zip":["ZIP","MAILZIP"]}
        try:
            tbl=DBF(str(path),ignore_missing_memofile=True)
            fields=[f.name.upper() for f in tbl.fields]
            def fc(keys):
                for k in keys:
                    if k in fields: return k
            cols={k:fc(v) for k,v in col_map.items()}; cnt=0
            for rec in tbl:
                try:
                    o=safe_strip(rec.get(cols["owner"]) if cols["owner"] else "")
                    if not o: continue
                    p={k:safe_strip(rec.get(cols[k]) if cols[k] else "") for k in col_map}
                    for v in self._variants(o): self.by_owner.setdefault(v,p)
                    cnt+=1
                except: pass
            log.info(f"Loaded {cnt:,} parcels")
        except Exception as e: log.error(f"DBF: {e}")
    def download_and_load(self):
        zp=DATA_DIR/"parcel_data.zip"
        def dl():
            r=SESSION.get(OCPA_BULK_URL,timeout=120,stream=True); r.raise_for_status()
            with open(zp,"wb") as f:
                for ch in r.iter_content(65536): f.write(ch)
            mb=zp.stat().st_size/1_048_576
            if mb<0.5: raise ValueError(f"too small: {mb:.1f}MB")
            log.info(f"Parcel ZIP: {mb:.1f}MB")
        if zp.exists() and (time.time()-zp.stat().st_mtime)<72000: log.info("Cached parcel ZIP")
        else:
            retry(dl,3,5)
            if not zp.exists(): return
        try:
            with zipfile.ZipFile(zp) as z:
                dbfs=[n for n in z.namelist() if n.upper().endswith(".DBF")]
                if dbfs:
                    dp=DATA_DIR/Path(dbfs[0]).name
                    with z.open(dbfs[0]) as s, open(dp,"wb") as d: d.write(s.read())
                    self._load_dbf(dp)
        except Exception as e: log.error(f"ZIP: {e}")
    def lookup(self,owner):
        for v in self._variants(owner):
            if v in self.by_owner: return self.by_owner[v]

# ── BROWSER HELPERS ───────────────────────────────────────────────────────────

async def ss(page, name):
    try: await page.screenshot(path=str(DATA_DIR/f"debug_{name}.png"), full_page=True)
    except: pass

async def dismiss_popups(page):
    """Dismiss any session timeout or other popups blocking the form."""
    for sel in [
        "button:has-text('Yes - Continue')",
        "button:has-text('Continue')",
        "#session-continue",
        "button:has-text('OK')",
        "button:has-text('Close')",
    ]:
        try:
            el = page.locator(sel).first
            if await el.is_visible(timeout=1000):
                await el.click(timeout=2000)
                log.info(f"  Dismissed popup: {sel}")
                await asyncio.sleep(1)
        except: pass

async def ensure_fresh_session(page):
    """
    Accept disclaimer fresh to get a valid session.
    Called before EVERY doc type search to avoid session expiry.
    """
    try:
        # POST the disclaimer acceptance
        status = await page.evaluate("""
            async () => {
                try {
                    const r = await fetch('/ssweb/user/disclaimer', {
                        method:'POST',
                        headers:{
                            'Content-Type':'application/x-www-form-urlencoded',
                            'ajaxRequest':'true'
                        },
                        body:'accept=Accept'
                    });
                    return r.status;
                } catch(e) { return -1; }
            }
        """)
        log.info(f"  Session refresh: {status}")
        await asyncio.sleep(0.5)
    except Exception as e:
        log.warning(f"  Session refresh error: {e}")

async def wait_for_form(page, timeout=15000):
    """Wait until the search form inputs are visible on the page."""
    deadline = time.time() + timeout/1000
    while time.time() < deadline:
        count = await page.evaluate("""
            () => document.querySelectorAll('input[type="text"], input[type="search"], select').length
        """)
        if count > 0:
            log.info(f"  Form ready: {count} input fields found")
            return True
        await asyncio.sleep(0.8)
    log.warning("  Form never appeared")
    return False

# ── SEARCH ────────────────────────────────────────────────────────────────────

async def search_doc_type(page, code, date_from, date_to):
    records = []
    log.info(f"Searching {code} | {date_from} - {date_to}")
    try:
        # Refresh session cookie before each search
        await ensure_fresh_session(page)

        # Navigate to search
        await page.goto(OR_SEARCH, timeout=30000, wait_until="domcontentloaded")
        await asyncio.sleep(2)

        # Dismiss any blocking popups
        await dismiss_popups(page)

        # Wait for actual form fields
        form_ready = await wait_for_form(page, timeout=15000)

        # Save source for first doc type
        if code == "LP":
            html = await page.content()
            with open(DATA_DIR/"debug_page_source.html","w") as f: f.write(html)
            await ss(page, f"02_{code}_loaded")

        if not form_ready:
            log.warning(f"  No form for {code}, skipping")
            return records

        # Dump visible inputs
        inputs = await page.evaluate("""
            () => Array.from(document.querySelectorAll('input,select'))
                .filter(e => e.offsetWidth > 0 || e.type === 'hidden')
                .map(e => ({id:e.id, name:e.name, type:e.type,
                            placeholder:e.placeholder, value:e.value}))
        """)
        log.info(f"  Inputs for {code}: {json.dumps(inputs)}")

        # Click Document Type tab if present
        for sel in ["a[data-val='dt']","a:has-text('Document Type')",
                    "a:has-text('Instrument Type')","li:has-text('Document Type') a"]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=1500):
                    await el.click(timeout=2000)
                    await asyncio.sleep(1.5)
                    log.info(f"  Tab: {sel}")
                    break
            except: pass

        # Re-check inputs after tab click
        inputs = await page.evaluate("""
            () => Array.from(document.querySelectorAll('input,select'))
                .map(e => ({id:e.id, name:e.name, type:e.type, placeholder:e.placeholder}))
        """)
        log.info(f"  Inputs after tab: {json.dumps(inputs)}")

        # Fill by whatever fields exist
        filled = False
        for inp in inputs:
            iid = inp.get("id","")
            nm  = inp.get("name","")
            ph  = inp.get("placeholder","").lower()
            tp  = inp.get("type","")

            if tp in ("hidden","submit","button","checkbox","radio"): continue

            sel = f"#{iid}" if iid else f"[name='{nm}']" if nm else None
            if not sel: continue

            if any(k in iid.lower()+nm.lower()+ph for k in ["doctype","instrumenttype","instrtype","type"]):
                await page.fill(sel, code)
                log.info(f"  Filled type: {sel} = {code}")
                filled = True
            elif any(k in iid.lower()+nm.lower()+ph for k in ["begindate","startdate","fromdate","datefrom"]):
                await page.fill(sel, date_from)
                log.info(f"  Filled from: {sel} = {date_from}")
            elif any(k in iid.lower()+nm.lower()+ph for k in ["enddate","todate","dateto","thrudate"]):
                await page.fill(sel, date_to)
                log.info(f"  Filled to: {sel} = {date_to}")

        if not filled:
            log.warning(f"  Could not fill type field for {code}")

        await asyncio.sleep(1)
        if code == "LP": await ss(page, f"03_{code}_filled")

        # Click search
        for sel in ["#searchButton","a#searchButton","button:has-text('Search')",
                    "a:has-text('Search')","input[value='Search']",
                    "[data-role='button']:has-text('Search')"]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=1500):
                    await el.click(timeout=3000)
                    log.info(f"  Search: {sel}")
                    break
            except: pass

        await asyncio.sleep(5)
        try: await page.wait_for_load_state("networkidle", timeout=20000)
        except: pass

        if code == "LP": await ss(page, f"04_{code}_results")

        # Paginate
        pnum = 0
        while True:
            pnum += 1
            html = await page.content()
            batch = _parse_results(html, code)
            records.extend(batch)
            log.info(f"  {code} p{pnum}: {len(batch)}")

            nxt = False
            for sel in ["a:has-text('Next')","button:has-text('Next')",
                        "[aria-label='Next page']",".next-page"]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=1000):
                        cls = await btn.get_attribute("class") or ""
                        dis = await btn.get_attribute("disabled")
                        if "disabled" not in cls.lower() and dis is None:
                            await btn.click(timeout=3000)
                            await asyncio.sleep(2)
                            try: await page.wait_for_load_state("networkidle",timeout=12000)
                            except: pass
                            nxt = True; break
                except: pass
            if not nxt or pnum > 50: break

    except Exception as e:
        log.error(f"{code}: {e}\n{traceback.format_exc()}")
    return records

def _parse_results(html, doc_type):
    soup = BeautifulSoup(html,"lxml")
    rows = []
    for tbl in soup.find_all("table"):
        hdrs = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if not hdrs: continue
        if not any(k in " ".join(hdrs) for k in ["instrument","doc","book","date","filed","grantor"]): continue
        hmap = {}
        for i,h in enumerate(hdrs):
            if any(k in h for k in ["instrument","doc num","book","number"]): hmap.setdefault("doc_num",i)
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
            link = next((a["href"] if a["href"].startswith("http") else
                         f"https://selfservice.or.occompt.com{a['href']}"
                         for a in tr.find_all("a",href=True)
                         if any(k in a["href"].lower() for k in ["doc","instrument","detail","view"])),"")
            num = get("doc_num") or (texts[0] if texts else "")
            if not num or num.lower() in ("","instrument","doc #","#"): continue
            rows.append({"doc_num":num,"doc_type":get("doc_type") or doc_type,
                         "filed":get("filed"),"grantor":get("grantor"),
                         "grantee":get("grantee"),"amount":get("amount"),
                         "legal":get("legal"),"clerk_url":link})
    return rows

# ── SCORE / ENRICH / CSV ──────────────────────────────────────────────────────

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
        if (datetime.today()-datetime.strptime(filed[:10],"%Y-%m-%d")).days<=7:
            score+=5; flags.append("New this week")
    except: pass
    if rec.get("prop_address"): score+=5; flags.append("Has address")
    return list(dict.fromkeys(flags)),min(score,100)

def enrich(raw,code,parcel):
    _,cat_label=LEAD_TYPES.get(code,(code,"Other"))
    owner_raw=safe_strip(raw.get("grantor","")); amount_str=safe_strip(raw.get("amount",""))
    amount_raw=parse_amount(amount_str); filed_raw=safe_strip(raw.get("filed",""))
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

def export_csv(records,out_path):
    cols=["First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
          "Property Address","Property City","Property State","Property Zip","Lead Type","Document Type",
          "Date Filed","Document Number","Amount/Debt Owed","Seller Score","Motivated Seller Flags",
          "Source","Public Records URL"]
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
    now=datetime.today(); date_to=now.strftime("%m/%d/%Y")
    date_from=(now-timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")
    log.info(f"Run: {date_from} → {date_to}")
    parcel=ParcelLookup(); parcel.download_and_load()
    all_records=[]

    async with async_playwright() as pw:
        browser=await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"]
        )
        ctx=await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            viewport={"width":1280,"height":900}
        )
        page=await ctx.new_page()

        # Initial disclaimer accept
        await page.goto(OR_DISCLAIMER, timeout=30000, wait_until="domcontentloaded")
        await asyncio.sleep(2)
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
        log.info(f"Initial disclaimer: {status}")

        for code in LEAD_TYPES:
            try:
                raws=await asyncio.wait_for(
                    search_doc_type(page,code,date_from,date_to), timeout=90
                )
            except asyncio.TimeoutError: log.warning(f"Timeout: {code}"); raws=[]
            except Exception as e: log.error(f"{code}: {e}"); raws=[]
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
