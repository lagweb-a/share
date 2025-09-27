# -*- coding: utf-8 -*-
"""
Scrape allowed URLs and extract (LLM-first):

--mode facility:
  - url
  - title
  - full address (if possible)
  - student discount (text, yen, percent)

--mode targets:
  - source_url         (まとめ記事URL)
  - item_name          (学割対象の名称: あれば)
  - item_url           (学割対象のURL: あれば)
  - extraction_method  (llm|mixed|rule)
  - notes              (補足: sns_skip など)

Input : ./final_checked/{basename}_final.csv   (must contain url, scraping_allowed)
Output: ./scraped/{basename}_scraped.csv  (facility)
        ./scraped/{basename}_targets.csv  (targets)
"""

import csv
import json
import re
import time
from pathlib import Path
from urllib.parse import urlparse, urljoin, urlunparse, urldefrag

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from string import Template

# ========== 設定 ==========
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SpotAppRobot/1.0; +https://example.com/robot)"
}
# 推奨: Qwen2.5 14B or 32B *Q4_K_M*（Ollama）
LLM_DEFAULT_MODEL = "qwen2.5:32b-instruct-q4_K_M"
LLM_ENDPOINT = "http://localhost:11434/api/generate"

AGGREGATOR_HINT_DOMAINS = {
    "prtimes.jp", "news.yahoo.co.jp", "twitter.com", "x.com", "navitime.co.jp",
    "tripadvisor.com", "pinterest.com", "note.com", "instagram.com", "facebook.com"
}

EXCLUDE_DOMAINS = {
    "instagram.com", "www.instagram.com",
    "tiktok.com", "www.tiktok.com",
    "facebook.com", "m.facebook.com", "www.facebook.com",
    "x.com", "twitter.com", "mobile.twitter.com", "t.co",
    "line.me",
    "asoview.com", "www.asoview.com",
    "hotpepper.jp", "www.hotpepper.jp",
    "tabelog.com", "www.tabelog.com",
    "gnavi.co.jp", "www.gnavi.co.jp",
    "jalan.net", "www.jalan.net",
    "prtimes.jp", "news.yahoo.co.jp",
    "pinterest.com", "www.pinterest.com",
    "tripadvisor.com", "www.tripadvisor.com",
    "chiebukuro.yahoo.co.jp", "detail.chiebukuro.yahoo.co.jp",
}

PRICE_KEYWORDS = [
    "料金","ご利用料金","利用料金","価格","プライス","チケット","入場料",
    "Fee","Fees","Price","Prices","Ticket","Admission"
]

PREFS = "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
POSTAL_RE = r"(〒\s*\d{3}[-‐–－]?\d{4})"
ADDRESS_RE = re.compile(rf"({POSTAL_RE}\s*)?({PREFS}).+?[0-9０-９\-－丁目番地号\.、,\s]+", re.S)

YEN_RE = re.compile(r"([0-9０-９,]+)\s*円")
PCT_RE = re.compile(r"([1-9][0-9]?)\s*%")

YEN_MIN, YEN_MAX = 100, 100000
PCT_MIN, PCT_MAX = 1, 95

# ========== ユーティリティ ==========
def ensure_parent(path_str: str) -> str:
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)
    return path_str

def make_session(timeout=12):
    sess = requests.Session()
    retries = Retry(
        total=3, connect=3, read=3, backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update(DEFAULT_HEADERS)
    sess._timeout = timeout
    return sess

SESSION = make_session()

def _zen2han_num(s: str) -> str:
    return s.translate(str.maketrans("０１２３４５６７８９", "0123456789")).replace(",", "")

def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _looks_garbled(s: str) -> bool:
    return "�" in (s or "")

def normalize_url(u: str) -> str:
    if not u:
        return ""
    u, _ = urldefrag(u)
    parsed = urlparse(u)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    q = parsed.query
    q = "&".join([kv for kv in q.split("&") if not kv.startswith(("utm_", "msclkid", "fbclid")) and kv != ""])
    return urlunparse((scheme, netloc, path, "", q, ""))

def is_excluded_domain(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return True
    return host in EXCLUDE_DOMAINS

# ========== タイトル可読化 ==========
GENERIC_TITLE_WORDS = {
    "公式サイト","公式","ホームページ","TOP","トップ","トップページ","HOME","Home",
    "お知らせ","ニュース","最新情報","サイト","インフォメーション","案内","予約","アクセス"
}

def clean_title(raw: str) -> str:
    if not raw:
        return ""
    t = re.sub(r"\s+", " ", raw.strip())
    parts = re.split(r"\s*[｜\|\-\—–·・:：»›]+\s*", t)
    if parts:
        for p in parts:
            p2 = p.strip()
            if p2 and not any(w.lower() == p2.lower() for w in GENERIC_TITLE_WORDS):
                t = p2
                break
        else:
            t = parts[0].strip()
    if any(w.lower() == t.lower() for w in GENERIC_TITLE_WORDS):
        t = ""
    t = re.sub(r"[（(].{0,12}?(公式|ホームページ|サイト|TOP|トップ).{0,12}?[）)]", "", t).strip()
    if len(t) > 48:
        t = t[:48].rstrip()
    return t

def is_generic_title(t: str) -> bool:
    if not t:
        return True
    s = t.strip()
    if len(s) < 3:
        return True
    if s in GENERIC_TITLE_WORDS:
        return True
    return False

# ========== HTML取得/本文抽出 ==========
def fetch(url: str):
    try:
        resp = SESSION.get(url, timeout=SESSION._timeout, allow_redirects=True)
        if resp and not resp.encoding:
            resp.encoding = resp.apparent_encoding
        ctype = (resp.headers.get("Content-Type") or "").lower()
        return resp, ctype
    except requests.RequestException:
        return None, None

MAIN_SELECTORS = [
    "main", "article", "#content", "#main", ".entry", ".post", ".page-content", ".l-main", ".c-contents"
]

def get_visible_text_preferring_main(soup: BeautifulSoup, fallback_limit=30000) -> str:
    for sel in MAIN_SELECTORS:
        node = soup.select_one(sel)
        if node:
            t = node.get_text(" ", strip=True)
            if len(t) > 40:
                return t[:fallback_limit]
    return soup.get_text(" ", strip=True)[:fallback_limit]

# ========== JSON-LD / microdata ==========
def parse_jsonld_blocks(soup: BeautifulSoup):
    blocks = []
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = (s.string or s.text or "").strip()
        if not text:
            continue
        try:
            obj = json.loads(text)
            if isinstance(obj, list):
                blocks.extend(obj)
            else:
                blocks.append(obj)
        except Exception:
            for m in re.findall(r"\{.*?\}", text, flags=re.S):
                try:
                    blocks.append(json.loads(m))
                except Exception:
                    pass
    return blocks

def extract_address_from_microdata(soup: BeautifulSoup) -> str:
    cand = soup.select('[itemprop="address"]')
    for c in cand:
        t = c.get_text(" ", strip=True)
        if t and len(t) >= 6:
            return t
    pa = soup.select('[itemtype*="PostalAddress"]')
    for b in pa:
        t = b.get_text(" ", strip=True)
        if t and len(t) >= 6:
            return t
    return ""

# ========== ルール系サブ ==========
def extract_address_candidates_from_text(text: str):
    cands = []
    for m in re.finditer(r"(住所|所在地)[:：]\s*([^\n\r<]+)", text):
        cands.append(m.group(2).strip())
    for m in re.finditer(ADDRESS_RE, text):
        start = max(0, m.start() - 10)
        end = min(len(text), m.end() + 10)
        cands.append(re.sub(r"\s+", " ", text[start:end]).strip(" ・,、。|>/"))
    uniq, seen = [], set()
    for c in cands:
        k = re.sub(r"\s+", "", c)
        if k not in seen:
            uniq.append(c)
            seen.add(k)
    return uniq

def score_jp_address(addr: str) -> int:
    sc = 0
    if re.search(POSTAL_RE, addr): sc += 3
    if re.search(PREFS, addr): sc += 3
    if re.search(r"[0-9０-９]+(丁目|番地|−|-)", addr): sc += 2
    if len(addr) >= 10: sc += 1
    addr = re.split(r"(TEL|電話|営業時間|Open|OPEN)[:：]?", addr)[0].strip()
    return sc

def extract_best_address(soup: BeautifulSoup) -> str:
    blocks = parse_jsonld_blocks(soup)
    addr = extract_address_from_jsonld_strict(blocks)
    if addr and score_jp_address(addr) >= 4:
        return addr
    md = extract_address_from_microdata(soup)
    if md and score_jp_address(md) >= 4:
        return md
    def near_access(txts):
        chunk = " ".join(txts)
        cands = extract_address_candidates_from_text(chunk)
        if not cands: return ""
        cands = sorted(cands, key=lambda x: score_jp_address(x), reverse=True)
        return cands[0]
    for sel in ["#access", ".access", "section.access", "div.access"]:
        sec = soup.select_one(sel)
        if sec:
            t = sec.get_text(" ", strip=True)
            a = near_access([t])
            if a: return a
    vis_main = get_visible_text_preferring_main(soup)
    a = near_access([vis_main, soup.get_text(" ", strip=True)])
    return a or ""

def extract_address_from_jsonld_strict(blocks) -> str:
    def flatten_addr(addr):
        if isinstance(addr, dict):
            postal = str(addr.get("postalCode","") or "")
            region = str(addr.get("addressRegion","") or "")
            city   = str(addr.get("addressLocality","") or "")
            street = str(addr.get("streetAddress","") or "")
            parts = [p for p in [postal, region, city, street] if p]
            return " ".join(parts).strip()
        if isinstance(addr, str):
            return addr.strip()
        return ""
    def pick(node):
        if not isinstance(node, dict):
            return ""
        if "address" in node:
            s = flatten_addr(node["address"])
            if s: return s
        if "location" in node and isinstance(node["location"], dict):
            s = flatten_addr(node["location"].get("address"))
            if s: return s
        for v in node.values():
            if isinstance(v, dict):
                s = pick(v)
                if s: return s
            elif isinstance(v, list):
                for it in v:
                    s = pick(it)
                    if s: return s
        return ""
    for obj in blocks:
        typ = obj.get("@type")
        if isinstance(typ, list):
            typ = next((x for x in typ if isinstance(x, str)), None)
        if typ in ("Place","LocalBusiness","Organization","Event"):
            s = pick(obj)
            if s:
                return s
    for obj in blocks:
        s = pick(obj)
        if s: return s
    return ""

def extract_discount_from_table_like_block(block: BeautifulSoup):
    best_text, yen_val, pct_val = None, None, None
    rows = []
    if block.name == "table":
        rows = block.find_all("tr")
    elif block.name in ("dl",):
        pairs = []
        dts = block.find_all("dt")
        for dt in dts:
            dd = dt.find_next_sibling("dd")
            if dd:
                pairs.append(dt.get_text(" ", strip=True) + " " + dd.get_text(" ", strip=True))
        rows = [{"_text": t} for t in pairs]
    elif block.name in ("ul","ol"):
        rows = block.find_all("li")
    else:
        return None, None, None
    def row_text(r):
        if isinstance(r, dict) and "_text" in r:
            return r["_text"]
        return r.get_text(" ", strip=True)
    header_text = block.get_text(" ", strip=True)
    if not any(k in header_text for k in ["学割","学生","大学生","高校生","専門学生","Student","student"]):
        return None, None, None
    for r in rows:
        t = row_text(r)
        if not t: continue
        if any(k in t for k in ["学割","学生","大学生","高校生","専門学生","Student","student"]):
            if not best_text: best_text = t[:160]
            m_y = YEN_RE.search(t); m_p = PCT_RE.search(t)
            if m_y and yen_val is None:
                try: yen_val = int(_zen2han_num(m_y.group(1)))
                except: pass
            if m_p and pct_val is None:
                try: pct_val = int(m_p.group(1))
                except: pass
            if yen_val or pct_val: break
    return best_text, yen_val, pct_val

def extract_discount(text: str):
    kw_hits = [m.span() for m in re.finditer(r"(学割|学生|学生証|Student|student)", text)]
    windows = []
    for s, e in kw_hits:
        start = max(0, s - 120); end = min(len(text), e + 200)
        windows.append(text[start:end])
    if not windows: windows = [text[:1600]]
    yen_val = None; pct_val = None; best_text = ""
    for w in windows:
        m_line = re.search(r"(大学生|高校生|専門学生|学生|学割)[^。\n\r]{0,20}?([0-9０-９,]+)\s*円", w)
        m_y = YEN_RE.search(w); m_p = PCT_RE.search(w)
        y_match = m_line or m_y
        if (y_match or m_p) and not best_text: best_text = _clean_text(w)[:160]
        if y_match and yen_val is None:
            try:
                raw = y_match.group(2) if y_match.re.groups >= 2 else y_match.group(1)
                yen_val = int(_zen2han_num(raw))
            except: pass
        if m_p and pct_val is None:
            try: pct_val = int(m_p.group(1))
            except: pass
        if yen_val is not None or pct_val is not None: break
    return best_text, yen_val, pct_val

def discover_price_like_links(base_url: str, soup: BeautifulSoup, max_links=3):
    base_parsed = urlparse(base_url)
    base_origin = f"{base_parsed.scheme}://{base_parsed.netloc}"
    links = []
    for a in soup.select("a[href]"):
        text = (a.get_text() or "").strip()
        if not text: continue
        if not any(k.lower() in text.lower() for k in PRICE_KEYWORDS): continue
        href = a.get("href") or ""
        absu = urljoin(base_url, href)
        if not absu.startswith(base_origin): continue
        links.append(absu)
        if len(links) >= max_links: break
    return links

# ========== サニティチェック ==========
def is_reasonable_yen(v): return isinstance(v, int) and (YEN_MIN <= v <= YEN_MAX)
def is_reasonable_pct(v): return isinstance(v, int) and (PCT_MIN <= v <= PCT_MAX)
def looks_like_jp_address(s): return bool(re.search(PREFS, s or ""))

def title_quality_flags(title: str):
    flags = []
    if not title or len(title) < 3: flags.append("title_short")
    if _looks_garbled(title): flags.append("title_garbled")
    if is_generic_title(title): flags.append("title_generic")
    return flags

def address_quality_flags(addr: str):
    flags = []
    if not addr: flags.append("addr_empty")
    if addr and not looks_like_jp_address(addr): flags.append("addr_not_jp_like")
    if _looks_garbled(addr): flags.append("addr_garbled")
    return flags

def discount_quality_flags(text, yen, pct):
    flags = []
    if not (yen or pct or (text and "学" in text)): flags.append("disc_missing")
    if yen is not None and not is_reasonable_yen(yen): flags.append("yen_unreasonable")
    if pct is not None and not is_reasonable_pct(pct): flags.append("pct_unreasonable")
    return flags

# ========== LLMコア ==========
def llm_call(prompt, model=LLM_DEFAULT_MODEL, endpoint=LLM_ENDPOINT, timeout=120, debug=False, options=None):
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": options or {"temperature": 0.0, "num_ctx": 8192, "top_p": 0.8}
    }
    res = requests.post(endpoint, json=payload, timeout=timeout)
    res.raise_for_status()
    raw = (res.json() or {}).get("response", "").strip()
    if debug:
        print("[LLM raw]", raw[:300].replace("\n"," ") + ("..." if len(raw)>300 else ""))
    m = re.search(r"\{.*\}", raw, flags=re.S)
    s = m.group(0) if m else raw
    return json.loads(s)

def chunk_text(text, size=3800, overlap=300):
    pieces = []; i = 0; n = len(text)
    while i < n:
        pieces.append(text[i:i+size]); i += max(1, size - overlap)
    return pieces

# ======== LLM（facility: 先に抽出） ========
def llm_extract_fields_chunkwise(visible_text: str, url: str, model: str, debug: bool):
    tpl = Template("""
あなたは日本語の抽出器。下の本文から厳密なJSONだけを返すこと。前後の文章は禁止。

出力:
{
  "title": "string",
  "address": "string",
  "discount_text": "string",
  "discount_value_yen": int|null,
  "discount_percent": int|null
}

制約:
- 推測しない。本文に根拠がない値は空/NULL。
- titleは施設名に正規化。区切りやサイト名は除去し短く。
- JSON以外は一切書かない。

URL: $url

本文:
$text
""".strip())

    partials = []
    for piece in chunk_text(visible_text, size=3800, overlap=300):
        prompt = tpl.substitute(url=url, text=piece)
        try:
            obj = llm_call(prompt, model=model, debug=debug, options={"temperature":0.0,"num_ctx":8192,"top_p":0.8})
            partials.append({
                "title": obj.get("title") or "",
                "address": obj.get("address") or "",
                "discount_text": obj.get("discount_text") or "",
                "discount_value_yen": obj.get("discount_value_yen"),
                "discount_percent": obj.get("discount_percent"),
            })
        except Exception as e:
            if debug: print("[LLM chunk err]", e)

    if not partials:
        return None

    def pick_title(parts):
        counts = {}
        for p in parts:
            t = clean_title(p.get("title",""))
            if not t: continue
            counts[t] = counts.get(t, 0) + 1
        if not counts: return ""
        best = sorted(counts.items(), key=lambda kv: (kv[1], 1 if 8<=len(kv[0])<=42 else 0), reverse=True)[0][0]
        return best

    def pick_address(parts):
        cands = [p.get("address","") for p in parts if p.get("address")]
        if not cands: return ""
        cands = sorted(cands, key=lambda x: score_jp_address(x), reverse=True)
        return cands[0]

    def pick_discount(parts):
        best_y, best_p, best_t = None, None, ""
        for p in parts:
            y = p.get("discount_value_yen"); pct = p.get("discount_percent")
            t = p.get("discount_text") or ""
            if isinstance(y, int) and is_reasonable_yen(y): best_y = y
            if isinstance(pct, int) and is_reasonable_pct(pct): best_p = pct
            if t and len(t) > len(best_t): best_t = t[:160]
        return best_t, best_y, best_p

    title = pick_title(partials)
    address = pick_address(partials)
    disc_text, disc_yen, disc_pct = pick_discount(partials)

    return {
        "title": title,
        "address": address,
        "discount_text": disc_text,
        "discount_value_yen": disc_yen,
        "discount_percent": disc_pct,
    }

# ======== facility: ルール抽出（LLMの補完用に残す） ========
def extract_core_fields_rule(url: str, html: str, soup: BeautifulSoup):
    # タイトルは軽く（保険）
    title = ""
    # JSON-LDやh1, title等から最善を拾う
    # 既存の実装（extract_best_title）を軽く使う
    title = extract_best_title(soup)

    # 住所
    addr = extract_best_address(soup)

    # 割引（表/本文）
    disc_text, disc_yen, disc_pct = "", None, None
    for block in soup.find_all(["table","dl","ul","ol"]):
        t, y, p = extract_discount_from_table_like_block(block)
        if (y or p) or (t and not disc_text):
            disc_text, disc_yen, disc_pct = t or disc_text, y or disc_yen, p or disc_pct
            if y or p: break
    if not (disc_yen or disc_pct or disc_text):
        vis_main = get_visible_text_preferring_main(soup)
        disc_text, disc_yen, disc_pct = extract_discount(vis_main)

    return title, addr, disc_text, disc_yen, disc_pct

def fetch_and_make_soup(url: str):
    resp, ctype = fetch(url)
    if not resp or not resp.text or "text/html" not in (ctype or ""):
        return None, ctype, resp
    soup = BeautifulSoup(resp.text, "html.parser")
    return soup, ctype, resp

# ========== facility: 1URL処理（LLM優先） ==========
def scrape_one_facility(url: str, domain_sleep=0.3, hop=True, use_llm=True,
                        llm_model=LLM_DEFAULT_MODEL, llm_debug=False, strict=False):
    time.sleep(domain_sleep)
    soup, ctype, resp = fetch_and_make_soup(url)
    if not soup:
        return {
            "url": url, "final_url": url, "source_page": url,
            "title": "", "address": "", "discount_text": "",
            "discount_value_yen": "", "discount_percent": "",
            "used_llm": "NO", "hop_used": "NO",
            "quality_flags": "fetch_failed",
            "error": f"fetch_failed:{'no_resp' if not resp else (ctype or '')}"
        }

    parsed = urlparse(resp.url)
    netloc = parsed.netloc
    quality_flags = []

    # 1) **LLMベース**で抽出
    used_llm = "NO"
    title = addr = disc_text = ""
    disc_yen = disc_pct = None

    if use_llm:
        visible = get_visible_text_preferring_main(soup, fallback_limit=30000)
        llm = llm_extract_fields_chunkwise(visible, resp.url, model=llm_model, debug=llm_debug)
        if llm:
            used_llm = "YES"
            title = clean_title(llm.get("title") or "")
            addr = llm.get("address") or ""
            disc_text = llm.get("discount_text") or ""
            ly = llm.get("discount_value_yen"); lp = llm.get("discount_percent")
            disc_yen = ly if isinstance(ly, int) else None
            disc_pct = lp if isinstance(lp, int) else None

    # 2) LLM結果に不足/サニティNGがあれば、**ルール**で補完
    need_rule = False
    quality_flags.extend(title_quality_flags(title))
    quality_flags.extend(address_quality_flags(addr))
    quality_flags.extend(discount_quality_flags(disc_text, disc_yen, disc_pct))
    if any(f in quality_flags for f in [
        "title_short","title_generic","title_garbled",
        "addr_empty","addr_not_jp_like","addr_garbled",
        "disc_missing","yen_unreasonable","pct_unreasonable"
    ]):
        need_rule = True

    if (not use_llm) or need_rule:
        t2, a2, dt2, dy2, dp2 = extract_core_fields_rule(resp.url, resp.text, soup)
        if (not title) and t2: title = clean_title(t2)
        if (not addr) and a2: addr = a2
        if (not disc_text) and dt2: disc_text = dt2
        if (disc_yen is None or not is_reasonable_yen(disc_yen)) and is_reasonable_yen(dy2 or -1): disc_yen = dy2
        if (disc_pct is None or not is_reasonable_pct(disc_pct)) and is_reasonable_pct(dp2 or -1): disc_pct = dp2

        # フラグ更新
        quality_flags = []
        quality_flags.extend(title_quality_flags(title))
        quality_flags.extend(address_quality_flags(addr))
        quality_flags.extend(discount_quality_flags(disc_text, disc_yen, disc_pct))

    # 3) まだ割引情報が薄い場合に限り、料金ページへ軽ホップ
    hop_used = "NO"
    if hop and not (disc_yen or disc_pct or disc_text):
        for link in discover_price_like_links(resp.url, soup, max_links=2):
            hop_used = "YES"
            time.sleep(max(domain_sleep, 0.5))
            s2, ct2, r2 = fetch_and_make_soup(link)
            if not s2: continue
            t2, a2, dt2, dy2, dp2 = extract_core_fields_rule(r2.url, r2.text, s2)
            if (not disc_text) and dt2: disc_text = dt2
            if disc_yen is None and dy2 is not None and is_reasonable_yen(dy2): disc_yen = dy2
            if disc_pct is None and dp2 is not None and is_reasonable_pct(dp2): disc_pct = dp2
            if (not addr) and a2: addr = a2
            if (not title) and t2: title = clean_title(t2)
            if disc_yen or disc_pct or disc_text: break

        quality_flags = []
        quality_flags.extend(title_quality_flags(title))
        quality_flags.extend(address_quality_flags(addr))
        quality_flags.extend(discount_quality_flags(disc_text, disc_yen, disc_pct))

    if any(h in netloc for h in AGGREGATOR_HINT_DOMAINS):
        quality_flags.append("looks_like_aggregator")

    result = {
        "url": url,
        "final_url": resp.url,
        "source_page": resp.url,
        "title": title or "",
        "address": addr or "",
        "discount_text": disc_text or "",
        "discount_value_yen": disc_yen if disc_yen is not None else "",
        "discount_percent": disc_pct if disc_pct is not None else "",
        "used_llm": used_llm,
        "hop_used": hop_used,
        "quality_flags": "|".join(sorted(set(quality_flags))),
        "error": ""
    }
    return result

# ========== targets: まとめ記事（LLM優先） ==========
TARGETS_HEADINGS = ("学割","学生","Student","student","学生割引","学割情報","大学生","高校生","専門学生")

def anchors_in_content(soup: BeautifulSoup):
    for sel in ["main", "article", "#content", ".entry", ".post", ".page-content", ".l-main", ".c-contents", "body"]:
        root = soup.select_one(sel)
        if root:
            return root.select("a[href]")
    return soup.select("a[href]")

def collect_targets_rule(base_url: str, soup: BeautifulSoup, max_items=80):
    items = []; seen = set()
    def push(name, href, note=""):
        if not href:
            key = f"__no_url__::{name}"
        else:
            absu = urljoin(base_url, href)
            norm = normalize_url(absu)
            if is_excluded_domain(norm): return
            key = norm + "::" + (name or "")
        if key in seen: return
        seen.add(key)
        items.append({"name": (name or "").strip(), "url": "" if not href else normalize_url(urljoin(base_url, href)), "note": note})
    for hd in soup.select("h2, h3, h4"):
        htxt = hd.get_text(" ", strip=True)
        if any(k in htxt for k in TARGETS_HEADINGS):
            sib = hd.find_next_sibling()
            block_text = (sib.get_text(" ", strip=True) if sib else "")
            anchors = (sib.select("a[href]") if sib else []) or hd.find_all_next("a", limit=40)
            for a in anchors:
                txt = (a.get_text(" ", strip=True) or "")
                href = a.get("href") or ""
                if not href: continue
                if (any(k in txt for k in TARGETS_HEADINGS) or
                    any(k in txt for k in ["料金","チケット","入場","Price","Ticket"]) or
                    any(k in block_text for k in TARGETS_HEADINGS)):
                    push(clean_title(txt) or txt, href, note="hd-near")
                if len(items) >= max_items: return items
    for a in anchors_in_content(soup):
        parent_chain = " ".join([p.name for p in a.parents if hasattr(p, "name")][:5])
        if any(pat in parent_chain for pat in ["nav", "breadcrumb", "footer", "header", "aside"]): continue
        txt = (a.get_text(" ", strip=True) or "")
        href = a.get("href") or ""
        if not href: continue
        if any(k in txt for k in TARGETS_HEADINGS) or any(k in txt for k in ["料金","チケット","入場","Price","Ticket"]):
            push(clean_title(txt) or txt, href, note="body-anchor")
        if len(items) >= max_items: break
    return items

def llm_extract_targets_chunkwise(visible_text: str, base_url: str, model: str, debug: bool):
    tpl = Template("""
あなたは日本語の抽出器。本文から「学割対象の施設名とURL」を抽出し、厳密なJSONだけを返すこと。

出力スキーマ:
{"items":[{"name":"施設名","url":"https://..."}, ...]}

ルール:
- URLは可能なら絶対URL。相対URLしか無い場合は空文字でもよい。
- SNS/シェア/広告/ナビゲーション的なリンクは含めない。
- JSON以外は絶対に書かない。

ページURL: $url

本文:
$text
""".strip())
    results = []
    for piece in chunk_text(visible_text, size=3800, overlap=300):
        prompt = tpl.substitute(url=base_url, text=piece)
        try:
            obj = llm_call(prompt, model=model, debug=debug, options={"temperature":0.0,"num_ctx":8192,"top_p":0.8})
            arr = obj.get("items") or []
            for it in arr:
                nm = (it.get("name") or "").strip()
                href = (it.get("url") or "").strip()
                results.append({"name": nm, "url": href})
        except Exception as e:
            if debug: print("[LLM targets err]", e)
    if not results:
        return []
    merged, seen = [], set()
    for it in results:
        nm = it["name"]; href = it["url"]
        if href:
            absu = urljoin(base_url, href); nu = normalize_url(absu)
            if is_excluded_domain(nu): continue
        else:
            nu = ""
        key = nu + "::" + (nm or "")
        if key in seen: continue
        seen.add(key)
        merged.append({"name": nm, "url": nu})
    return merged

def scrape_one_targets(url: str, domain_sleep=0.3, use_llm=True, llm_model=LLM_DEFAULT_MODEL, llm_debug=False,
                       min_rule_items=3, max_return=120):
    time.sleep(domain_sleep)
    soup, ctype, resp = fetch_and_make_soup(url)
    if not soup:
        return {"source": url, "items": [], "method": "none", "notes": "fetch_failed"}

    method = "rule"
    merged = []

    # **LLMベース**で抽出
    llm_items = []
    if use_llm:
        visible = get_visible_text_preferring_main(soup, fallback_limit=30000)
        llm_items = llm_extract_targets_chunkwise(visible, resp.url, model=llm_model, debug=llm_debug)
        method = "llm" if llm_items else "rule"
        merged.extend(llm_items)

    # LLMが少ない/ゼロならルールで補完
    if (not use_llm) or len(merged) < min_rule_items:
        rule_items = collect_targets_rule(resp.url, soup, max_items=max_return)
        # マージ・重複排除
        seen = set(nu.get("url","") + "::" + (nu.get("name","") or "") for nu in merged)
        for it in rule_items:
            key = (it.get("url","") + "::" + (it.get("name","") or ""))
            if key in seen: continue
            seen.add(key); merged.append({"name": it.get("name",""), "url": it.get("url","")})
        if method == "llm" and rule_items: method = "mixed"
        elif method == "rule" and rule_items: method = "rule"

    notes = []
    if not merged: notes.append("no_items")
    return {"source": resp.url, "items": merged[:max_return], "method": method, "notes": ",".join(notes)}

# ========== 収集オーケストレーション ==========
def scrape_from_final(final_csv: str, output_csv: str, limit=None, domain_sleep=0.3, hop=True,
                      llm=True, llm_model=LLM_DEFAULT_MODEL, llm_debug=False, strict=False):
    ensure_parent(output_csv)
    targets = []
    with open(final_csv, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("scraping_allowed") or "").upper() == "YES":
                u = (row.get("url") or "").strip()
                if u: targets.append(u)
    if limit: targets = targets[:limit]

    fieldnames = [
        "url", "final_url", "source_page",
        "title", "address",
        "discount_text", "discount_value_yen", "discount_percent",
        "used_llm", "hop_used", "quality_flags",
        "error"
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f_out:
        w = csv.DictWriter(f_out, fieldnames=fieldnames); w.writeheader()
        last_netloc = None
        for url in targets:
            netloc = urlparse(url).netloc
            if last_netloc and last_netloc != netloc:
                time.sleep(max(domain_sleep, 0.6))
            res = scrape_one_facility(url, domain_sleep=domain_sleep, hop=hop,
                                      use_llm=llm, llm_model=llm_model, llm_debug=llm_debug, strict=strict)
            w.writerow(res); last_netloc = netloc

    print(f"✅ Scraped (facility/LLM-first): {len(targets)} URLs → {output_csv}")
    return output_csv

def scrape_targets_from_final(final_csv: str, output_csv: str, limit=None, domain_sleep=0.3,
                              llm=True, llm_model=LLM_DEFAULT_MODEL, llm_debug=False):
    ensure_parent(output_csv)
    sources = []
    with open(final_csv, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("scraping_allowed") or "").upper() == "YES":
                u = (row.get("url") or "").strip()
                if u: sources.append(u)
    if limit: sources = sources[:limit]

    fieldnames = ["source_url", "item_name", "item_url", "extraction_method", "notes"]

    with open(output_csv, "w", newline="", encoding="utf-8") as f_out:
        w = csv.DictWriter(f_out, fieldnames=fieldnames); w.writeheader()
        last_netloc = None
        for src in sources:
            netloc = urlparse(src).netloc
            if last_netloc and last_netloc != netloc:
                time.sleep(max(domain_sleep, 0.6))
            bundle = scrape_one_targets(src, domain_sleep=domain_sleep, use_llm=llm, llm_model=llm_model, llm_debug=llm_debug)
            for it in bundle["items"]:
                w.writerow({
                    "source_url": bundle["source"],
                    "item_name": it.get("name",""),
                    "item_url": it.get("url",""),
                    "extraction_method": bundle["method"],
                    "notes": bundle["notes"]
                })
            last_netloc = netloc

    print(f"✅ Scraped (targets/LLM-first): {len(sources)} pages → {output_csv}")
    return output_csv

# ========== CLI ==========
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape allowed URLs. --mode facility or --mode targets (LLM-first)")
    parser.add_argument("final_csv", help="final_checked/{basename}_final.csv (from pipeline)")
    parser.add_argument("--mode", choices=["facility","targets"], default="facility", help="scraping mode")
    parser.add_argument("--out", default="", help="output CSV path (auto default by mode if empty)")
    parser.add_argument("--limit", type=int, default=None, help="limit number of pages (debug)")
    parser.add_argument("--sleep", type=float, default=0.3, help="per-request sleep seconds")
    parser.add_argument("--no-hop", action="store_true", help="(facility) disable in-domain price page hop")
    parser.add_argument("--llm-off", action="store_true", help="disable ollama LLM usage (fallback to rules only)")
    parser.add_argument("--llm-model", default=LLM_DEFAULT_MODEL, help="ollama model tag")
    parser.add_argument("--llm-debug", action="store_true", help="print raw LLM outputs/errors")
    parser.add_argument("--strict", action="store_true", help="(facility) unused in LLM-first, kept for compatibility")

    args = parser.parse_args()

    if not args.out:
        args.out = "./scraped/tickets_scraped.csv" if args.mode == "facility" else "./scraped/tickets_targets.csv"

    if args.mode == "facility":
        scrape_from_final(
            args.final_csv, args.out,
            limit=args.limit, domain_sleep=args.sleep, hop=not args.no_hop,
            llm=not args.llm_off, llm_model=args.llm_model, llm_debug=args.llm_debug, strict=args.strict
        )
    else:
        scrape_targets_from_final(
            args.final_csv, args.out,
            limit=args.limit, domain_sleep=args.sleep,
            llm=not args.llm_off, llm_model=args.llm_model, llm_debug=args.llm_debug
        )
