# fed_speeches_all_in_one.py
# 1) Scrape index des pages annuelles (Selenium)
# 2) Télécharge chaque discours, extrait le texte complet et compte les mots (Requests + BS4)
# 3) Écrit: fed_index.csv et fed_speeches_full.csv (avec reprise)

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

BASE = "https://www.federalreserve.gov"
YEAR_URL = BASE + "/newsevents/speech/{year}-speeches.htm"

CURRENT_YEAR = datetime.today().year
MIN_YEAR = CURRENT_YEAR - 8         
YEAR_START, YEAR_END = CURRENT_YEAR, max(CURRENT_YEAR-30, 1996)  

OUT_DIR = Path.home() / "fed_project" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)
INDEX_CSV = OUT_DIR / "fed_index.csv"
FULL_CSV  = OUT_DIR / "fed_speeches_full.csv"

REPROCESS_IF_WC_LT = 150

SPEECH_URL_RE = re.compile(r"/newsevents/speech/[a-z0-9-]*\d{8}[a-z]?\.htm$", re.I)
DATE_MMDDYYYY = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")

def make_driver():
    opts = Options()
    # opts.add_argument("--headless=new")     
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

def extract_year_index(driver, year: int, seen_urls: set):
    url = YEAR_URL.format(year=year)
    print(f"[index {year}] {url}")
    driver.get(url)

    wait = WebDriverWait(driver, 20)
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a")))
    time.sleep(0.4)

    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href^='/newsevents/speech/'][href$='.htm']")
    rows, new_count = [], 0

    for a in anchors:
        try:
            href = a.get_attribute("href") or ""
            if not SPEECH_URL_RE.search(href):
                continue
            if href in seen_urls:
                continue

            title = (a.text or a.get_attribute("title") or "").strip()
            cont = nearest_container(a)
            ctx = cont.text if cont else ""

            date_str = parse_date_from_text(ctx) or date_from_url(href)
            speaker  = extract_speaker_from_block(ctx)

            rows.append({
                "date": date_str,
                "title": title,
                "speaker": speaker,
                "url": href if href.startswith("http") else urljoin(BASE, href)
            })
            seen_urls.add(href)
            new_count += 1
        except:
            continue

    print(f"  +{new_count} new links")
    return rows

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

def build_index() -> list[dict]:
    """Scrape les pages annuelles et écrit fed_index.csv (avec reprise)."""
    print(f"→ Building index into {INDEX_CSV}")
    driver = make_driver()

    seen_urls = set()
    if INDEX_CSV.exists():
        with open(INDEX_CSV, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("url"):
                    seen_urls.add(row["url"])

    total = 0
    for year in range(YEAR_START, YEAR_END - 1, -1):
        if year < MIN_YEAR:
            break
        rows = extract_year_index(driver, year, seen_urls)

        keep = []
        for r in rows:
            if r["date"]:
                try:
                    if datetime.strptime(r["date"], "%Y-%m-%d").year >= MIN_YEAR:
                        keep.append(r)
                except:
                    keep.append(r)
            else:
                keep.append(r)

        write_rows(INDEX_CSV, keep, header=["date","title","speaker","url"])
        total += len(keep)
        time.sleep(random.uniform(0.6, 1.2))

    driver.quit()
    print(f"Index done. +{total} rows written.")
    with open(INDEX_CSV, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def build_full(index_rows: list[dict]):
    """Télécharge les discours & écrit fed_speeches_full.csv (reprise intelligente)."""
    print(f"→ Building full into {FULL_CSV}")
    done_ok, redo = set(), set()
    if FULL_CSV.exists():
        with open(FULL_CSV, "r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                url = (r.get("url") or "").strip()
                if not url: 
                    continue
                try:
                    wc = int(r.get("word_count") or 0)
                except:
                    wc = 0
                if wc < REPROCESS_IF_WC_LT:
                    redo.add(url)
                else:
                    done_ok.add(url)

    if not FULL_CSV.exists():
        with open(FULL_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "date","title","speaker","url","word_count","text"
            ])
            w.writeheader()

    s = make_session()
    batch, total = [], 0
    for r in index_rows:
        url = (r.get("url") or "").strip()
        if not url: 
            continue
        if url in done_ok:      
            continue

        try:
            resp = s.get(url, timeout=30)
            resp.raise_for_status()
            text = extract_transcript(resp.text)
            wc = count_words(text)
            row = {
                "date": r.get("date",""),
                "title": r.get("title",""),
                "speaker": r.get("speaker",""),
                "url": url,
                "word_count": wc,
                "text": text
            }
            batch.append(row); total += 1

            if len(batch) >= 20:
                write_rows(FULL_CSV, batch, header=[
                    "date","title","speaker","url","word_count","text"
                ])
                print(f"  wrote {len(batch)} rows (progress {total})")
                batch = []

            time.sleep(0.15)
        except Exception as e:
            print(f"[warn] {url} -> {e}")

    if batch:
        write_rows(FULL_CSV, batch, header=[
            "date","title","speaker","url","word_count","text"
        ])
        print(f"  wrote {len(batch)} rows (final)")

    print(f"Full done. New/updated rows: {total}")

def main():
    # Étape 1 — index (Selenium)
    index_rows = build_index()

    # Étape 2 — textes (Requests + BS4)
    build_full(index_rows)

    print(f"\nCSV index   → {INDEX_CSV}")
    print(f"CSV full    → {FULL_CSV}")

if __name__ == "__main__":
    main()