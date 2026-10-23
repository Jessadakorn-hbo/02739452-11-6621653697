"""Scraping + transform helpers (pure Python, no Airflow imports -> easy to test).

Sources (Thai marketing / advertising news):
  1. Marketing Oops  -> WordPress REST API (public, not blocked by robots.txt)
  2. Positioning Mag -> HTML scraping (requests + BeautifulSoup, no Selenium)
  3. Brand Inside    -> HTML scraping (requests + BeautifulSoup, no Selenium)

Only public article metadata is stored (title, url, date, category).
No author names / personal data are collected.
"""
import html as htmlmod
import os
import re
import time
import unicodedata
from datetime import date
from difflib import SequenceMatcher
from itertools import islice
from urllib import robotparser
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

UA = os.getenv("SCRAPER_UA", "KU-BigData-StudentProject/1.0 (educational; polite crawler)")
HEADERS = {"User-Agent": UA, "Accept-Language": "th,en;q=0.8"}
DELAY = float(os.getenv("SCRAPER_DELAY", "1.0"))
TIMEOUT = 25
_ROBOTS = {}


# --------------------------------------------------------------------------
# HTTP + robots.txt
# --------------------------------------------------------------------------
def _robots_for(url):
    p = urlparse(url)
    root = f"{p.scheme}://{p.netloc}"
    if root in _ROBOTS:
        return _ROBOTS[root]
    rp = robotparser.RobotFileParser()
    try:
        r = requests.get(root + "/robots.txt", headers=HEADERS, timeout=TIMEOUT)
    except requests.RequestException as e:
        raise RuntimeError(f"cannot read robots.txt of {root}: {e}")
    if r.status_code == 200:
        rp.parse(r.text.splitlines())
    elif 400 <= r.status_code < 500:
        rp.allow_all = True          # no robots.txt -> everything allowed (RFC 9309)
    else:
        rp.disallow_all = True       # server error -> be conservative
    rp.modified()
    _ROBOTS[root] = rp
    return rp


def require_allowed(url):
    if "/page/" in url:              # Marketing Oops disallows /page/ (wildcards not parsed by urllib)
        raise RuntimeError(f"skip pagination url (robots): {url}")
    if not _robots_for(url).can_fetch(UA, url):
        raise RuntimeError(f"blocked by robots.txt: {url}")


def fetch(url, params=None, tries=3):
    require_allowed(url)
    last = None
    for i in range(tries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return r
            last = RuntimeError(f"HTTP {r.status_code}")
            if r.status_code in (403, 404, 410):
                break
        except requests.RequestException as e:
            last = e
        time.sleep(2 * (i + 1))
    raise RuntimeError(f"fetch failed: {url}: {last}")


def soup_of(url):
    return BeautifulSoup(fetch(url).content, "lxml")   # bytes -> bs4 detects Thai charset itself


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------
def normalize_text(s):
    s = htmlmod.unescape(s or "")
    s = BeautifulSoup(s, "lxml").get_text(" ") if "<" in s else s
    s = unicodedata.normalize("NFC", s)       # NFC (not NFKC) keeps Thai sara-am intact
    return re.sub(r"\s+", " ", s).strip()


def title_key(t):
    return re.sub(r"[\W_]+", "", unicodedata.normalize("NFC", t or "").lower())


def parse_date(s):
    if not s:
        return None
    s = str(s)
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        y, mo, d = map(int, m.groups())
    else:
        m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)      # dd/mm/yyyy
        if not m:
            return None
        d, mo, y = map(int, m.groups())
    if y > 2400:                                              # Buddhist era
        y -= 543
    try:
        return date(y, mo, d).isoformat()
    except ValueError:
        return None


def category_from_url(url):
    parts = [p for p in urlparse(url).path.split("/") if p]
    return parts[0] if parts else "unknown"


def make_row(source, url, title, published, category):
    return {
        "source": source,
        "url": url,
        "title": normalize_text(title),
        "published_date": parse_date(published),
        "category": category,
    }


def _clean_url(u):
    u = u.split("#")[0].split("?")[0]
    return u.replace("://www.brandinside.asia", "://brandinside.asia")


def _find_date_near(a, base, is_article):
    """Look for a date inside the 'card' that contains this link (stop if the card holds >1 article)."""
    for anc in [a] + list(islice(a.parents, 5)):
        if anc.name in ("body", "html", "[document]"):
            break
        urls = {_clean_url(urljoin(base, x["href"])) for x in anc.find_all("a", href=True)}
        if len([u for u in urls if is_article(u)]) > 1:
            break
        t = anc.find("time")
        if t is not None:
            d = parse_date(t.get("datetime") or t.get_text(" "))
            if d:
                return d
        d = parse_date(anc.get_text(" ", strip=True)) if anc.name != "a" or anc.find("time") else None
        if d:
            return d
    return None


def parse_article_links(soup, base, is_article):
    found = {}
    for a in soup.find_all("a", href=True):
        url = _clean_url(urljoin(base, a["href"]))
        if not is_article(url):
            continue
        title = a.get_text(" ", strip=True)
        if not title:
            img = a.find("img")
            title = (img.get("alt") or "").strip() if img else ""
        rec = found.setdefault(url, {"title": "", "date": None})
        if len(title) > len(rec["title"]):
            rec["title"] = title
        if not rec["date"]:
            rec["date"] = _find_date_near(a, base, is_article)
    return found


def page_title_date(soup):
    title = None
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        title = re.sub(r"\s*[\|\-–—]\s*(Positioning|Brand Inside|Marketing Oops).*$", "", og["content"], flags=re.I)
    elif soup.title:
        title = soup.title.get_text()
    d = None
    for attrs in ({"property": "article:published_time"}, {"itemprop": "datePublished"}, {"name": "pubdate"}):
        m = soup.find("meta", attrs=attrs)
        if m and m.get("content"):
            d = parse_date(m["content"])
            if d:
                break
    if not d:
        t = soup.find("time")
        if t:
            d = parse_date(t.get("datetime") or t.get_text(" "))
    if not d:
        m = re.search(r'"datePublished"\s*:\s*"([^"]+)"', str(soup))
        d = parse_date(m.group(1)) if m else None
    return title, d


# --------------------------------------------------------------------------
# Extractors
# --------------------------------------------------------------------------
def extract_marketingoops(pages=6, per_page=50):
    """WordPress REST API (/wp-json). Not pagination under /page/, so robots.txt is respected."""
    rows = []
    for p in range(1, pages + 1):
        r = fetch("https://www.marketingoops.com/wp-json/wp/v2/posts",
                  params={"per_page": per_page, "page": p, "_fields": "date,link,title"})
        data = r.json()
        if not data:
            break
        for it in data:
            rows.append(make_row("marketingoops", it["link"], it["title"]["rendered"],
                                 it["date"], category_from_url(it["link"])))
        time.sleep(DELAY)
    return rows


POS_SECTIONS = ["insight", "strategic-move", "people", "digital", "global-trend", "pr-news"]
POS_ARTICLE = re.compile(r"^https://positioningmag\.com/(insight|strategic-move|people|digital|global-trend|pr-news|columnist|infographic)/(\d+)/?$")


def extract_positioning(max_articles=None):
    max_articles = max_articles or int(os.getenv("POSITIONING_MAX", "60"))
    is_art = lambda u: bool(POS_ARTICLE.match(u))
    pages = ["https://positioningmag.com/"] + [f"https://positioningmag.com/{s}" for s in POS_SECTIONS]
    urls = {}
    for i, page in enumerate(pages):
        try:
            soup = soup_of(page)
        except Exception:
            if i == 0:
                raise
            continue
        for u, rec in parse_article_links(soup, page, is_art).items():
            urls.setdefault(u, rec)
        time.sleep(DELAY)
    if not urls:
        raise RuntimeError("positioning: no article links found (layout changed or JS-rendered?)")
    ordered = sorted(urls, key=lambda u: -int(POS_ARTICLE.match(u).group(2)))[:max_articles]
    rows = []
    for u in ordered:
        title, d = urls[u]["title"], urls[u]["date"]
        try:                                   # article page gives full title + exact date
            t2, d2 = page_title_date(soup_of(u))
            title, d = t2 or title, d2 or d
        except Exception:
            pass
        rows.append(make_row("positioning", u, title, d, category_from_url(u)))
        time.sleep(DELAY)
    return rows


BI_CATEGORY = re.compile(r"^https://brandinside\.asia/category/([^/]+)/?$")
BI_BLOCK = {"category", "author", "tag", "page", "feed", "about", "about-us", "contact", "contact-us",
            "privacy-policy", "search", "wp-content", "wp-json", "wp-admin", "advertise", "ads"}


def _is_bi_article(u):
    p = urlparse(u)
    if p.netloc != "brandinside.asia":
        return False
    seg = [s for s in p.path.split("/") if s]
    return len(seg) == 1 and seg[0].lower() not in BI_BLOCK


def extract_brandinside(max_categories=8):
    home = "https://brandinside.asia/"
    soup = soup_of(home)
    cats = []
    for a in soup.find_all("a", href=True):
        m = BI_CATEGORY.match(_clean_url(urljoin(home, a["href"])))
        if m and m.group(1) not in cats:
            cats.append(m.group(1))
    found = {u: dict(rec, category="home") for u, rec in parse_article_links(soup, home, _is_bi_article).items()}
    for c in cats[:max_categories]:
        time.sleep(DELAY)
        try:
            page = f"https://brandinside.asia/category/{c}/"
            for u, rec in parse_article_links(soup_of(page), page, _is_bi_article).items():
                old = found.get(u)
                if old is None or old["category"] == "home":
                    rec["category"] = c
                    if old and not rec["date"]:
                        rec["date"] = old["date"]
                    found[u] = rec
        except Exception:
            continue
    rows = [make_row("brandinside", u, r["title"], r["date"], r["category"])
            for u, r in found.items() if len(r["title"]) >= 12]
    if not rows:
        raise RuntimeError("brandinside: no articles parsed (layout changed or JS-rendered?)")
    return rows


EXTRACTORS = {
    "marketingoops": extract_marketingoops,
    "positioning": extract_positioning,
    "brandinside": extract_brandinside,
}

# --------------------------------------------------------------------------
# Transform: brand detection + cross-source dedup
# --------------------------------------------------------------------------
BRANDS = {
    "7-Eleven": ["7-eleven", "7eleven", "เซเว่น"], "CP": ["ซีพี", "cp all", "cpall", "เจริญโภคภัณฑ์"],
    "Lotus's": ["lotus", "โลตัส"], "Big C": ["big c", "บิ๊กซี"], "Makro": ["makro", "แม็คโคร"],
    "Central": ["central", "เซ็นทรัล"], "Siam Piwat": ["siam piwat", "สยามพิวรรธน์"], "The Mall": ["the mall", "เดอะมอลล์"],
    "Grab": ["grab", "แกร็บ"], "LINE": ["line man", "lineman", "line ", "ไลน์"], "Shopee": ["shopee", "ช้อปปี้"],
    "Lazada": ["lazada", "ลาซาด้า"], "TikTok": ["tiktok", "ติ๊กต็อก"], "Google": ["google", "กูเกิล", "gemini"],
    "Meta": ["facebook", "instagram", "meta ", "เฟซบุ๊ก"], "YouTube": ["youtube", "ยูทูบ"], "Netflix": ["netflix", "เน็ตฟลิกซ์"],
    "AIS": ["ais ", "เอไอเอส"], "True": ["true ", "ทรู"], "dtac": ["dtac", "ดีแทค"],
    "KBank": ["kbank", "กสิกร"], "SCB": ["scb", "ไทยพาณิชย์"], "Krungthai": ["krungthai", "กรุงไทย"],
    "Bangkok Bank": ["bangkok bank", "ธนาคารกรุงเทพ"], "กรุงเทพประกันภัย": ["กรุงเทพประกันภัย"],
    "Toyota": ["toyota", "โตโยต้า"], "Honda": ["honda", "ฮอนด้า"], "Nissan": ["nissan", "นิสสัน"], "BYD": ["byd"],
    "Mitsubishi": ["mitsubishi", "มิตซูบิชิ"], "Nestlé": ["nestl", "เนสท์เล่", "nescafe", "เนสกาแฟ"],
    "Singha": ["singha", "สิงห์"], "Chang": ["chang", "ช้าง", "thaibev", "ไทยเบฟ"], "Coca-Cola": ["coca-cola", "โค้ก"],
    "Pepsi": ["pepsi", "เป๊ปซี่"], "Oishi": ["oishi", "โออิชิ"], "Saltz": ["saltz", "ซอลท์"], "Unilever": ["unilever", "ยูนิลีเวอร์"],
    "P&G": ["p&g", "procter"], "Samsung": ["samsung", "ซัมซุง"], "Apple": ["apple", "iphone", "แอปเปิล"],
    "Starbucks": ["starbucks", "สตาร์บัคส์"], "McDonald's": ["mcdonald", "แมคโดนัลด์"], "KFC": ["kfc"],
    "Mixue": ["mixue", "มี่เสวี่ย"], "Amazon": ["amazon", "คาเฟ่ อเมซอน", "cafe amazon"], "Uniqlo": ["uniqlo", "ยูนิโคล่"],
    "Nike": ["nike", "ไนกี้"], "Adidas": ["adidas", "อาดิดาส"], "Thai Airways": ["thai airways", "การบินไทย"],
    "AirAsia": ["airasia", "แอร์เอเชีย"], "SCG": ["scg", "เอสซีจี"], "PTT": ["ptt", "ปตท"], "Minor": ["minor", "ไมเนอร์"],
    "AP Thailand": ["ap thailand", "เอพี ไทยแลนด์"], "Lazada/Alibaba": ["alibaba", "อาลีบาบา"],
}


def detect_brand(title):
    low = (title or "").lower() + " "
    best, pos = None, 10 ** 9
    for brand, keys in BRANDS.items():
        for k in keys:
            i = low.find(k)
            if 0 <= i < pos:
                best, pos = brand, i
    return best


def transform(rows):
    """Normalise, detect brand, de-duplicate across sources (same URL / same title / near-identical title)."""
    out, seen_urls, seen_keys = [], set(), []
    for r in rows:
        url = (r.get("url") or "").strip()
        title = normalize_text(r.get("title"))
        key = title_key(title)
        if url in seen_urls:
            continue
        dup = False
        if len(key) >= 12:
            for k in seen_keys:
                if k == key:
                    dup = True
                    break
                if abs(len(k) - len(key)) <= 0.15 * max(len(k), len(key)):
                    sm = SequenceMatcher(None, k, key)
                    if sm.real_quick_ratio() >= 0.9 and sm.quick_ratio() >= 0.9 and sm.ratio() >= 0.9:
                        dup = True
                        break
        if dup:
            continue
        seen_urls.add(url)
        seen_keys.append(key)
        out.append({"source": r.get("source"), "url": url, "title": title, "title_key": key,
                    "published_date": r.get("published_date"), "category": r.get("category") or "unknown",
                    "brand": detect_brand(title)})
    return out


def assess(rows, min_rows=30, max_bad_ratio=0.2):
    """Data-quality gate. Returns dict(ok, reasons, stats)."""
    n = len(rows)
    today = date.today().isoformat()
    bad = [r for r in rows if not r["url"] or len(r["title"]) < 8
           or (r["published_date"] and r["published_date"] > today)]
    dated = sum(1 for r in rows if r["published_date"])
    reasons = []
    if n < min_rows:
        reasons.append(f"rows {n} < {min_rows}")
    if n and len(bad) / n > max_bad_ratio:
        reasons.append(f"bad ratio {len(bad)}/{n} > {max_bad_ratio}")
    return {"ok": not reasons, "reasons": reasons,
            "stats": {"rows": n, "bad": len(bad), "dated": dated, "sources": len({r['source'] for r in rows})}}
