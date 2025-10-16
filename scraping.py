# fed_speeches_all_in_one.py (no-Selenium)
# 1) Scrape index des pages annuelles (Requests + BS4)
# 2) Télécharge chaque discours, extrait le texte complet et compte les mots (Requests + BS4)
# 3) Écrit: fed_index.csv et fed_speeches_full.csv (avec reprise)

from __future__ import annotations

import csv, re, time, random
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------- Config -----------
BASE = "https://www.federalreserve.gov"
YEAR_URL = BASE + "/newsevents/speech/{year}-speeches.htm"

CURRENT_YEAR = datetime.today().year
MIN_YEAR = CURRENT_YEAR - 8                 # 5–8 ans -> on prend 8
YEAR_START, YEAR_END = CURRENT_YEAR, max(CURRENT_YEAR-30, 1996)

OUT_DIR = Path.home() / "fed_project" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)
INDEX_CSV = OUT_DIR / "fed_index.csv"
FULL_CSV  = OUT_DIR / "fed_speeches_full.csv"

REPROCESS_IF_WC_LT = 150

# ----------- HTTP robuste -----------
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "KHTML, like Gecko) Chrome/126 Safari/537.36")
    })
    retry = Retry(total=5, backoff_factor=0.5,
                  status_forcelist=(429,500,502,503,504),
                  allowed_methods=("GET",))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://",  HTTPAdapter(max_retries=retry))
    return s

# ----------- Utils communs -----------
def write_rows(path: Path, rows, header):
    if not rows: return
    file_exists = path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            w.writeheader()
        w.writerows(rows)

SPEECH_URL_RE = re.compile(r"/newsevents/speech/[a-z0-9-]*\d{8}[a-z]?\.htm$", re.I)
DATE_MMDDYYYY = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")
FOOTNOTE_ANCHOR = re.compile(r"^fn\d+$", re.I)
WS_RE   = re.compile(r"\s+")
WORD_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ0-9’']+")

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
    candidates, seen = [], set()
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

    if not parts:  # fallback pour pages atypiques
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

# ----------- Étape 1 : INDEX (requests+bs4, pas de Selenium) -----------
def build_index() -> list[dict]:
    print(f"→ Building index into {INDEX_CSV}")
    s = make_session()

    # reprise: éviter doublons si relance
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

        url = YEAR_URL.format(year=year)
        print(f"[index {year}] {url}")
        r = s.get(url, timeout=20); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        anchors = soup.select("a[href^='/newsevents/speech/'][href$='.htm']")
        rows, new_count = [], 0

        for a in anchors:
            href = a.get("href","")
            if not SPEECH_URL_RE.search(href):
                continue
            abs_url = href if href.startswith("http") else urljoin(BASE, href)
            if abs_url in seen_urls:
                continue

            title = a.get_text(strip=True) or (a.get("title") or "")
            # remonter à un conteneur pertinent pour date/speaker
            container = a
            for _ in range(5):
                if not container: break
                if container.name in ("li","article") or ("row" in (container.get("class") or [])): break
                container = container.parent
            ctx = container.get_text("\n", strip=True) if container else ""

            date_str = parse_date_from_text(ctx) or date_from_url(href)
            speaker  = extract_speaker_from_block(ctx)

            row = {"date": date_str, "title": title, "speaker": speaker, "url": abs_url}
            # filtre cutoff année si possible
            if date_str:
                try:
                    if datetime.strptime(date_str, "%Y-%m-%d").year < MIN_YEAR:
                        continue
                except:
                    pass

            rows.append(row); seen_urls.add(abs_url); new_count += 1

        write_rows(INDEX_CSV, rows, header=["date","title","speaker","url"])
        total += len(rows)
        print(f"  +{new_count} new links (total index {total})")
        time.sleep(random.uniform(0.3, 0.7))

    print("Index done.")
    with open(INDEX_CSV, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

# ----------- Étape 2 : FULL (texte + word_count) -----------
def build_full(index_rows: list[dict]):
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

# ----------- Main -----------
def main():
    index_rows = build_index()
    build_full(index_rows)
    print(f"\nCSV index   → {INDEX_CSV}")
    print(f"CSV full    → {FULL_CSV}")

if __name__ == "__main__":
    main()