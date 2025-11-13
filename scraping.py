
from __future__ import annotations

import csv, os, re, time, random
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- DATES TO CONFIGURE MANUALLY ---
# Enter the desired start date here (Year, Month, Day)
START_DATE = datetime(2011, 11, 1) 
# Enter the desired end date here
END_DATE = datetime(2025, 11, 1)
# --- END OF CONFIGURATION ---


BASE = "https://www.federalreserve.gov"
YEAR_URL = BASE + "/newsevents/speech/{year}-speeches.htm"

OUT_DIR = Path.cwd()
FULL_CSV  = OUT_DIR / "fed_speeches.csv"

# If a speech has fewer than 150 words, it will be reprocessed on the next run
REPROCESS_IF_WC_LT = 150

SPEECH_URL_RE = re.compile(r"/newsevents/speech/[a-z0-9-]*\d{8}[a-z]?\.htm$", re.I)
DATE_MMDDYYYY = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")

def make_driver():
    opts = Options()
    opts.add_argument("--headless=new")     
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

def write_rows(path: Path, rows, header):
    if not rows: return
    file_exists = path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            w.writeheader()
        w.writerows(rows)

def parse_date_from_text(txt: str):
    m = DATE_MMDDYYYY.search(txt or "")
    if not m: return ""
    try:
        dt = datetime.strptime(m.group(0), "%m/%d/%Y")
        return dt.strftime("%Y-%m-%d")
    except:
        return ""

def date_from_url(href: str):
    m = re.search(r'/(\d{8})[a-z]?\.htm$', href or "")
    if not m: return ""
    s = m.group(1)
    try:
        dt = datetime(int(s[:4]), int(s[4:6]), int(s[6:8]))
        return dt.strftime("%Y-%m-%d")
    except:
        return ""

def extract_speaker_from_block(text_block: str):
    if not text_block: return ""
    lines = [ln.strip() for ln in text_block.splitlines() if ln.strip()]
    for ln in lines:
        lo = ln.lower()
        if lo.startswith("by "): return ln[3:].strip()
        if "speaker:" in lo:     return ln.split(":",1)[1].strip()
        if any(k in ln for k in ["Chair", "Governor", "President", "Vice Chair"]):
            return ln.strip()
    return ""

def nearest_container(a_el):
    XPATHS = [
        "./ancestor::li[1]",
        "./ancestor::div[contains(@class,'row')][1]",
        "./ancestor::article[1]",
        "./parent::div"
    ]
    for xp in XPATHS:
        try:
            c = a_el.find_element(By.XPATH, xp)
            if c and c.text.strip():
                return c
        except:
            pass
    return a_el

def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/126.0 Safari/537.36")
    })
    retry = Retry(total=5, backoff_factor=0.5,
                  status_forcelist=(429,500,502,503,504),
                  allowed_methods=("GET",))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

WS_RE   = re.compile(r"\s+")
WORD_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ0-9’']+")
FOOTNOTE_ANCHOR = re.compile(r"^fn\d+$", re.I)

def clean_text(t:str) -> str:
    return WS_RE.sub(" ", t).strip()

def count_words(t:str) -> int:
    return len(WORD_RE.findall(t))

def _good_para_text(t:str) -> bool:
    if not t: return False
    low = t.lower()
    if "watch live" in low or low.startswith("share"): return False
    if t == "PDF": return False
    return count_words(t) >= 25

def _score_candidate(div: Tag) -> int:
    score = 0
    for p in div.find_all("p"):
        if p.find("a", attrs={"name": FOOTNOTE_ANCHOR}): 
            continue
        txt = p.get_text(" ", strip=True)
        if _good_para_text(txt):
            score += 1
    return score

def _pick_main_body(root: Tag) -> Tag:
    candidates = []
    selectors = [
        "#content div.col-xs-12.col-sm-8.col-md-8",
        "#content div.col-sm-8.col-md-8",
        "#content div.col-sm-8",
        "#content article",
        "div.col-xs-12.col-sm-8.col-md-8",
        "div.col-sm-8.col-md-8",
        "div.col-sm-8",
        "article",
    ]
    seen = set()
    for sel in selectors:
        for div in root.select(sel):
            if id(div) in seen: 
                continue
            seen.add(id(div))
            candidates.append(div)
    return max(candidates, key=_score_candidate) if candidates else root

def extract_transcript(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("#content") or soup
    body = _pick_main_body(root)
    stop = root.select_one("#lastUpdate")

    parts = []
    for p in body.find_all("p", recursive=True):
        if stop and (p is stop or stop in p.parents):
            break
        if p.find("a", attrs={"name": FOOTNOTE_ANCHOR}):
            continue
        txt = p.get_text(" ", strip=True)
        if _good_para_text(txt):
            parts.append(txt)

    if not parts:  
        for p in body.find_all("p", recursive=True):
            if stop and (p is stop or stop in p.parents):
                break
            if p.find("a", attrs={"name": FOOTNOTE_ANCHOR}):
                continue
            txt = p.get_text(" ", strip=True)
            if count_words(txt) >= 5:
                parts.append(txt)

    text = " ".join(parts)
    text = re.sub(r"\s*\[\d+\]\s*", " ", text)
    text = re.sub(r"\s*\(\d+\)\s*", " ", text)
    return clean_text(text)

def scrape_and_process_speeches():
    """
    Scrapes Federal Reserve speeches for a given period and saves all data
    directly into a single CSV file.
    """
    print(f"→ Starting scraper for the period from {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    print(f"→ Output file will be: {FULL_CSV}")

    # Resume logic: read already scraped URLs to avoid reprocessing them
    done_urls = set()
    if FULL_CSV.exists():
        with open(FULL_CSV, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                url = (r.get("url") or "").strip()
                if not url: continue
                try:
                    wc = int(r.get("word_count") or 0)
                    if wc >= REPROCESS_IF_WC_LT:
                        done_urls.add(url)
                except ValueError:
                    continue
    print(f"Found {len(done_urls)} already processed and valid speeches.")

    driver = make_driver()
    session = make_session()

    header = ["date", "title", "speaker", "url", "word_count", "text"]
    batch, total_new_speeches = [], 0

    # Iterate through the years, from most recent to oldest
    for year in range(END_DATE.year, START_DATE.year - 1, -1):
        year_url = YEAR_URL.format(year=year)
        print(f"\n[Indexing Year {year}] {year_url}")

        try:
            driver.get(year_url)
            WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a")))
            time.sleep(0.5)
        except Exception as e:
            print(f"  [WARNING] Could not load the page for year {year}: {e}")
            continue

        anchors = driver.find_elements(By.CSS_SELECTOR, "a[href^='/newsevents/speech/'][href$='.htm']")

        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                if not SPEECH_URL_RE.search(href):
                    continue

                full_url = urljoin(BASE, href)
                if full_url in done_urls:
                    continue

                # --- Date validation ---
                cont_for_date = nearest_container(a)
                date_str = parse_date_from_text(cont_for_date.text) or date_from_url(href)
                if not date_str:
                    continue

                speech_date = datetime.strptime(date_str, "%Y-%m-%d")
                if not (START_DATE <= speech_date <= END_DATE):
                    continue

                # --- If the URL is new and within the date range, process it ---
                print(f"  > Processing: {full_url}")

                resp = session.get(full_url, timeout=30)
                resp.raise_for_status()

                text = extract_transcript(resp.text)
                wc = count_words(text)

                ctx_text = cont_for_date.text if cont_for_date else ""

                row = {
                    "date": date_str,
                    "title": (a.text or a.get_attribute("title") or "").strip(),
                    "speaker": extract_speaker_from_block(ctx_text),
                    "url": full_url,
                    "word_count": wc,
                    "text": text
                }

                batch.append(row)
                total_new_speeches += 1
                done_urls.add(full_url) # Add to set to avoid duplicates within the same session

                # Write in batches to avoid keeping everything in memory
                if len(batch) >= 20:
                    write_rows(FULL_CSV, batch, header=header)
                    print(f"    ...batch of {len(batch)} speeches written to CSV (new total: {total_new_speeches})")
                    batch = []

                time.sleep(random.uniform(0.1, 0.2))

            except Exception as e:
                # Ignore errors on a single speech to avoid halting the entire process
                # print(f"  [WARNING] Error on speech {href}: {e}")
                continue

    # Write the last batch if any remains
    if batch:
        write_rows(FULL_CSV, batch, header=header)
        print(f"    ...final batch of {len(batch)} speeches written to CSV (new total: {total_new_speeches})")

    driver.quit()
    print(f"\nScraping complete. {total_new_speeches} new speeches were added.")


def main():
    # Optional: to ensure you start from scratch, uncomment the lines below to delete the old file
    # if FULL_CSV.exists():
    #     print(f"Deleting old file: {FULL_CSV}")
    #     os.remove(FULL_CSV)

    scrape_and_process_speeches()

    print(f"\nFinal CSV file created/updated at: {FULL_CSV}")

if __name__ == "__main__":
    main()
