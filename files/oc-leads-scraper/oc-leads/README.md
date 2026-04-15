# Swift Offers — Orange County FL Lead Intelligence

Automated motivated seller lead scraper for Orange County, FL.  
Runs daily via GitHub Actions · Publishes dashboard to GitHub Pages.

---

## File Structure

```
├── scraper/
│   ├── fetch.py            ← Main scraper (Playwright + requests)
│   └── requirements.txt
├── dashboard/
│   ├── index.html          ← Lead intelligence dashboard (GitHub Pages)
│   └── records.json        ← Latest scraped records
├── data/
│   ├── records.json        ← Duplicate of dashboard records
│   └── leads_export.csv    ← GHL-ready CSV export
└── .github/workflows/
    └── scrape.yml          ← Daily cron + manual trigger
```

---

## Setup

### 1. Fork / clone this repo

```bash
git clone https://github.com/YOUR_ORG/oc-leads.git
cd oc-leads
```

### 2. Enable GitHub Pages

- Go to **Settings → Pages**
- Source: **GitHub Actions**

### 3. (Optional) Set env variables

In **Settings → Secrets and variables → Actions**, add:

| Variable | Default | Description |
|---|---|---|
| `LOOKBACK_DAYS` | `7` | How many days back to search |

### 4. Run manually

Go to **Actions → Scrape Orange County Leads → Run workflow**

---

## Lead Types Collected

| Code | Name | Category |
|---|---|---|
| LP | Lis Pendens | Pre-Foreclosure |
| NOFC | Notice of Foreclosure | Pre-Foreclosure |
| TAXDEED | Tax Deed | Tax / Govt Lien |
| JUD / CCJ / DRJUD | Judgment types | Judgment Lien |
| LNCORPTX / LNIRS / LNFED | Tax & federal liens | Tax / Govt Lien |
| LN / LNMECH / LNHOA | Misc liens | Various |
| MEDLN | Medicaid Lien | Govt / Medical Lien |
| PRO | Probate | Probate |
| NOC | Notice of Commencement | NOC |
| RELLP | Release Lis Pendens | Release |

---

## Seller Score (0–100)

| Condition | Points |
|---|---|
| Base | +30 |
| Each motivated flag | +10 |
| LP + Foreclosure combo | +20 |
| Amount > $100k | +15 |
| Amount > $50k | +10 |
| Filed within 7 days | +5 |
| Has property address | +5 |

---

## Data Sources

- **Orange County Clerk:** [myorangeclerk.com](https://www.myorangeclerk.com)
- **OCPA Parcel Data:** [ocpafl.org](https://www.ocpafl.org) — bulk DBF download for owner/address enrichment

---

## Local Development

```bash
pip install -r scraper/requirements.txt
python -m playwright install chromium
python scraper/fetch.py
```

Open `dashboard/index.html` in your browser (or run a local server).

---

## GHL Export

After each run, `data/leads_export.csv` is produced with columns ready to import directly into GoHighLevel:

`First Name, Last Name, Mailing Address, Mailing City, Mailing State, Mailing Zip, Property Address, Property City, Property State, Property Zip, Lead Type, Document Type, Date Filed, Document Number, Amount/Debt Owed, Seller Score, Motivated Seller Flags, Source, Public Records URL`
