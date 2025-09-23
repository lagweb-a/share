# -*- coding: utf-8 -*-
"""
Scrape allowed URLs and extract:
- url
- title
- full address (if possible)
- student discount (text, yen, percent)

Input : ./final_checked/{basename}_final.csv   (must contain url, scraping_allowed)
Output: ./scraped/{basename}_scraped.csv

精度向上ポイント（改良版）:
- エンコーディング補正 (apparent_encoding)
- 本文優先抽出 (<main>/<article>/#content/.entry/.post 等)
- DL/UL/OL を用いた料金表パターンを強化
- 値のサニティチェック（円/％の範囲、タイトル/住所の質）
- LLMは欠損時 + 品質低い時に再抽出（空欄優先上書き、または不適時に置換）
- PDF/非HTMLの早期スキップ、アグリゲータ系の軽除外フラグ
- 出力列に final_url, used_llm, quality_flags, hop_used, source_page を追加
- 追加: タイトルの可読化（ルール + LLM リライト）
"""

import csv
import json
import re
import time
from pathlib import Path
from urllib.parse import urlparse, urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ========== 設定 ==========
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SpotAppRobot/1.0; +https://example.com/robot)"
}
LLM_DEFAULT_MODEL = "qwen2.5:7b-instruct-q5_1"
LLM_ENDPOINT = "http://localhost:11434/api/generate"

# アグリゲータ/ニュース告知系（住所や価格が本質でないことが多い）
AGGREGATOR_HINT_DOMAINS = {
    "prtimes.jp", "news.yahoo.co.jp", "twitter.com", "x.com", "navitime.co.jp",
    "tripadvisor.com", "pinterest.com", "note.com", "instagram.com", "facebook.com"
}

# 料金/チケット系リンク探索
PRICE_KEYWORDS = [
    "料金","ご利用料金","利用料金","価格","プライス","チケット","入場料",
    "Fee","Fees","Price","Prices","Ticket","Admission"
]

# 日本の都道府県と住所パターン
PREFS = "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
POSTAL_RE = r"(〒\s*\d{3}[-‐–－]?\d{4})"
ADDRESS_RE = re.compile(rf"({POSTAL_RE}\s*)?({PREFS}).+?[0-9０-９\-－丁目番地号\.、,\s]+", re.S)

YEN_RE = re.compile(r"([0-9０-９,]+)\s*円")
PCT_RE = re.compile(r"([1-9][0-9]?)\s*%")

# 円/％のサニティレンジ（チケット価格の現実的範囲）
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
    # 文字化けっぽいパターンをゆるく検出（�, 文字コード化け）
    return "�" in (s or "")

# ========== タイトル可読化（ルールベース） ==========
GENERIC_TITLE_WORDS = {
    "公式サイト","公式","ホームページ","TOP","トップ","トップページ","HOME","Home",
    "お知らせ","ニュース","最新情報","サイト","インフォメーション","案内","予約","アクセス"
}

def clean_title(raw: str) -> str:
    """
    サイト実運用向けに可読化:
    - 区切り（｜, |, -, —, ・, ：, :）があれば左側を優先
    - 「公式サイト/ホームページ/TOP/お知らせ」などの汎用語を除去
    - 先頭末尾のノイズをトリム
    - 長すぎる場合は適度に丸める
    """
    if not raw:
        return ""
    t = raw.strip()

    # HTML 系の余計な空白を正規化
    t = re.sub(r"\s+", " ", t)

    # 左側優先で分割（日本語サイトに多い区切り）
    parts = re.split(r"\s*[｜\|\-\—–·・:：»›]+\s*", t)
    if parts:
        # 左から意味のあるチャンクを採用
        for p in parts:
            p2 = p.strip()
            if p2 and not any(w.lower() == p2.lower() for w in GENERIC_TITLE_WORDS):
                t = p2
                break
        else:
            t = parts[0].strip()

    # 汎用語だけのタイトルを空にする（LLMフォールバック狙い）
    if any(w.lower() == t.lower() for w in GENERIC_TITLE_WORDS):
        t = ""

    # 括弧でサイト名などが残る場合は軽く削る
    t = re.sub(r"[（(].{0,12}?(公式|ホームページ|サイト|TOP|トップ).{0,12}?[）)]", "", t).strip()

    # 極端に長いときは丸め（多言語を考慮し緩めに）
    if len(t) > 48:
        t = t[:48].rstrip()

    return t

def is_generic_title(t: str) -> bool:
    if not t:
        return True
    s = t.strip()
    if len(s) < 3:
        return True
    # 「料金」「アクセス」等、単語1つだけの汎用もNG
    if s in GENERIC_TITLE_WORDS:
        return True
    return False

# ========== LLM ==========
def llm_extract_fields_via_ollama(
    text: str, url: str,
    model: str = LLM_DEFAULT_MODEL,
    endpoint: str = LLM_ENDPOINT,
    timeout: int = 150,
    debug: bool = False,
    num_ctx: int = 8192
):
    prompt = f"""
あなたは日本語の情報抽出アシスタントです。以下のWebページ本文から項目を抽出して、JSONで返してください。
URL: {url}

抽出ルール:
- title: 人が読んでわかる「施設/店舗/イベント名」などの実用的タイトルに正規化。
         「公式サイト」「ホームページ」「TOP/トップ」等の語は削除し、余計なサイト名の連結や区切りは除去して短く要点だけにする。
- address: できるだけ完全な住所（郵便番号〜番地まで）。見つからなければ空文字
- discount_text: 学割/学生に関する記述の短い抜粋（最大160文字）
- discount_value_yen: 学割価格や学生料金など具体的な金額（数値, 円）。無ければ null
- discount_percent: 〇%割引等の数値（整数）。無ければ null
- 出力は **厳密な JSON**。余計な文字や説明は出さない。
- 住所や割引は、本文に根拠がない推測はしない。

本文:
{text[:4000]}
"""
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.2, "num_ctx": num_ctx}
    }
    try:
        res = requests.post(endpoint, json=payload, timeout=timeout)
        res.raise_for_status()
        raw = (res.json() or {}).get("response", "").strip()
        if debug:
            print("[LLM raw]", raw[:300].replace("\n"," ") + ("..." if len(raw)>300 else ""))
        m = re.search(r"\{.*\}", raw, flags=re.S)
        s = m.group(0) if m else raw
        obj = json.loads(s)
        return {
            "title": obj.get("title") or "",
            "address": obj.get("address") or "",
            "discount_text": obj.get("discount_text") or "",
            "discount_value_yen": obj.get("discount_value_yen"),
            "discount_percent": obj.get("discount_percent"),
        }
    except Exception as e:
        if debug:
            print(f"[LLM error] {e.__class__.__name__}: {e}")
        return None

def llm_retry(text, url, model, endpoint, timeout, debug, num_ctx, tries=2, wait=0.8):
    for i in range(tries):
        out = llm_extract_fields_via_ollama(text, url, model, endpoint, timeout, debug, num_ctx)
        if out:
            return out
        time.sleep(wait)
    return None

# ========== HTML取得/本文抽出 ==========
def fetch(url: str):
    try:
        resp = SESSION.get(url, timeout=SESSION._timeout, allow_redirects=True)
        # 文字化け対策
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
    # main-like を優先してテキスト取得
    for sel in MAIN_SELECTORS:
        node = soup.select_one(sel)
        if node:
            t = node.get_text(" ", strip=True)
            if len(t) > 40:  # 最低限の文字数
                return t[:fallback_limit]
    # fallback 全文
    return soup.get_text(" ", strip=True)[:fallback_limit]

# ========== タイトル/住所/割引抽出 ==========
def extract_title(soup: BeautifulSoup) -> str:
    for attr in [
        ("meta", {"property": "og:title"}),
        ("meta", {"name": "twitter:title"}),
        ("meta", {"name": "og:title"}),
    ]:
        tag = soup.find(*attr)
        if tag and tag.get("content"):
            return tag["content"].strip()
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(" ", strip=True)[:120]
    return ""

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

def _get_text(d, key):
    v = d.get(key)
    if isinstance(v, dict):
        return v.get("name") or v.get("@value") or ""
    if isinstance(v, list):
        for item in v:
            if isinstance(item, (str, int, float)):
                return str(item)
            if isinstance(item, dict):
                t = item.get("name") or item.get("@value")
                if t:
                    return t
        return ""
    return str(v) if v is not None else ""

def extract_address_from_jsonld(blocks) -> str:
    def pick_addr(addr):
        if not isinstance(addr, dict):
            return ""
        parts = []
        for k in ("postalCode","addressRegion","addressLocality","streetAddress"):
            v = _get_text(addr, k)
            if v:
                parts.append(v)
        if not parts:
            free = addr.get("address") or addr.get("name")
            if isinstance(free, str):
                return free.strip()
        return " ".join(parts).strip()

    def scan(node):
        if not isinstance(node, dict):
            return ""
        if node.get("@type") == "PostalAddress":
            s = pick_addr(node)
            if s:
                return s
        if "address" in node:
            s = pick_addr(node["address"])
            if s:
                return s
        for _, v in node.items():
            if isinstance(v, dict):
                s = scan(v)
                if s: return s
            elif isinstance(v, list):
                for it in v:
                    s = scan(it)
                    if s: return s
        return ""

    for obj in blocks:
        s = scan(obj)
        if s:
            return s
    return ""

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

def extract_address_from_text(text: str) -> str:
    m = re.search(r"(住所|所在地)[:：]\s*([^\n\r<]+)", text)
    if m:
        return m.group(2).strip()
    m2 = ADDRESS_RE.search(text)
    if m2:
        start = max(0, m2.start() - 10)
        end = min(len(text), m2.end() + 10)
        cand = text[start:end]
        cand = re.sub(r"\s+", " ", cand)
        return cand.strip(" ・,、。|>/")
    return ""

def extract_discount_from_table_like_block(block: BeautifulSoup):
    """
    <table> に限らず、<dl>, <ul>, <ol> の「行」に相当する塊から抽出
    """
    best_text, yen_val, pct_val = None, None, None
    # 各「行」テキスト
    rows = []
    if block.name == "table":
        rows = block.find_all("tr")
    elif block.name in ("dl",):
        # dt/dd をまとめて1行扱い
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

    # 全体に学生/学割の含有が無ければスキップ
    header_text = block.get_text(" ", strip=True)
    if not any(k in header_text for k in ["学割","学生","大学生","高校生","専門学生","Student","student"]):
        return None, None, None

    for r in rows:
        t = row_text(r)
        if not t:
            continue
        if any(k in t for k in ["学割","学生","大学生","高校生","専門学生","Student","student"]):
            if not best_text:
                best_text = t[:160]
            m_y = YEN_RE.search(t)
            m_p = PCT_RE.search(t)
            if m_y and yen_val is None:
                try:
                    yen_val = int(_zen2han_num(m_y.group(1)))
                except Exception:
                    pass
            if m_p and pct_val is None:
                try:
                    pct_val = int(m_p.group(1))
                except Exception:
                    pass
            if yen_val or pct_val:
                break
    return best_text, yen_val, pct_val

def extract_discount(text: str):
    """
    安全版：金額抽出の正規表現が2グループ（m_line）/1グループ（YEN_RE）のどちらでも
    例外を投げずに動くように分岐。IndexError: no such group を防止。
    """
    kw_hits = [m.span() for m in re.finditer(r"(学割|学生|学生証|Student|student)", text)]
    windows = []
    for s, e in kw_hits:
        start = max(0, s - 120)
        end = min(len(text), e + 200)
        windows.append(text[start:end])
    if not windows:
        windows = [text[:1600]]

    yen_val = None
    pct_val = None
    best_text = ""

    for w in windows:
        # m_line: 2グループ（2つ目が金額）
        m_line = re.search(r"(大学生|高校生|専門学生|学生|学割)[^。\n\r]{0,20}?([0-9０-９,]+)\s*円", w)
        # m_y   : 1グループ（1つ目が金額）
        m_y    = YEN_RE.search(w)
        m_p    = PCT_RE.search(w)

        # どちらかヒットした方を採用
        y_match = m_line or m_y

        # 抜粋テキストは最初に金額or%が見つかった窓
        if (y_match or m_p) and not best_text:
            best_text = _clean_text(w)[:160]

        # 金額（グループ数で分岐）
        if y_match and yen_val is None:
            try:
                raw = y_match.group(2) if y_match.re.groups >= 2 else y_match.group(1)
                yen_val = int(_zen2han_num(raw))
            except Exception:
                pass

        # 割合
        if m_p and pct_val is None:
            try:
                pct_val = int(m_p.group(1))
            except Exception:
                pass

        if yen_val is not None or pct_val is not None:
            break

    return best_text, yen_val, pct_val

# ========== 料金リンク探索 ==========
def discover_price_like_links(base_url: str, soup: BeautifulSoup, max_links=3):
    base_parsed = urlparse(base_url)
    base_origin = f"{base_parsed.scheme}://{base_parsed.netloc}"
    links = []
    for a in soup.select("a[href]"):
        text = (a.get_text() or "").strip()
        if not text:
            continue
        if not any(k.lower() in text.lower() for k in PRICE_KEYWORDS):
            continue
        href = a.get("href") or ""
        absu = urljoin(base_url, href)
        if not absu.startswith(base_origin):
            continue
        links.append(absu)
        if len(links) >= max_links:
            break
    return links

# ========== サニティチェック ==========
def is_reasonable_yen(v):
    return isinstance(v, int) and (YEN_MIN <= v <= YEN_MAX)

def is_reasonable_pct(v):
    return isinstance(v, int) and (PCT_MIN <= v <= PCT_MAX)

def looks_like_jp_address(s):
    return bool(re.search(PREFS, s or ""))

def title_quality_flags(title: str):
    flags = []
    if not title or len(title) < 3:
        flags.append("title_short")
    if _looks_garbled(title):
        flags.append("title_garbled")
    if is_generic_title(title):
        flags.append("title_generic")
    return flags

def address_quality_flags(addr: str):
    flags = []
    if not addr:
        flags.append("addr_empty")
    if addr and not looks_like_jp_address(addr):
        flags.append("addr_not_jp_like")
    if _looks_garbled(addr):
        flags.append("addr_garbled")
    return flags

def discount_quality_flags(text, yen, pct):
    flags = []
    if not (yen or pct or (text and "学" in text)):
        flags.append("disc_missing")
    if yen is not None and not is_reasonable_yen(yen):
        flags.append("yen_unreasonable")
    if pct is not None and not is_reasonable_pct(pct):
        flags.append("pct_unreasonable")
    return flags

# ========== コア抽出 ==========
def extract_core_fields(url: str, html: str, soup: BeautifulSoup):
    # タイトル抽出 → 可読化
    raw_title = extract_title(soup)
    title = clean_title(raw_title)

    blocks = parse_jsonld_blocks(soup)
    addr = extract_address_from_jsonld(blocks) or extract_address_from_microdata(soup)
    if not addr:
        vis_main = get_visible_text_preferring_main(soup)
        addr = extract_address_from_text(vis_main)

    disc_text, disc_yen, disc_pct = "", None, None
    for block in soup.find_all(["table","dl","ul","ol"]):
        t, y, p = extract_discount_from_table_like_block(block)
        if (y or p) or (t and not disc_text):
            disc_text, disc_yen, disc_pct = t or disc_text, y or disc_yen, p or disc_pct
            if y or p:
                break

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

def scrape_one(url: str, domain_sleep=0.3, hop=True, use_llm=True, llm_model=LLM_DEFAULT_MODEL, llm_debug=False, strict=False):
    """
    strict=True のとき、サニティが悪い場合は LLM 再抽出を積極化
    """
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

    # まず現在ページで抽出
    title, addr, disc_text, disc_yen, disc_pct = extract_core_fields(resp.url, resp.text, soup)

    # 料金ページへ軽くホップ（見つからなかった時のみ）
    hop_used = "NO"
    if hop and not (disc_yen or disc_pct or disc_text):
        for link in discover_price_like_links(resp.url, soup, max_links=2):
            hop_used = "YES"
            time.sleep(max(domain_sleep, 0.5))
            s2, ct2, r2 = fetch_and_make_soup(link)
            if not s2:
                continue
            t2, a2, dt2, dy2, dp2 = extract_core_fields(r2.url, r2.text, s2)
            if dt2 or dy2 or dp2:
                disc_text = disc_text or dt2
                disc_yen = disc_yen if disc_yen is not None else dy2
                disc_pct = disc_pct if disc_pct is not None else dp2
            if (not addr) and a2:
                addr = a2
            if (not title) and t2:
                title = t2
            if disc_yen or disc_pct or disc_text:
                break

    # 品質判定
    quality_flags.extend(title_quality_flags(title))
    quality_flags.extend(address_quality_flags(addr))
    quality_flags.extend(discount_quality_flags(disc_text, disc_yen, disc_pct))

    # アグリゲータの気配（参考フラグ）
    if any(h in netloc for h in AGGREGATOR_HINT_DOMAINS):
        quality_flags.append("looks_like_aggregator")

    used_llm = "NO"

    # LLM で補完/再抽出（1) 欠損時, 2) strict またはクオリティ低い時）
    need_llm = False
    if use_llm:
        # 欠損
        if not title or not addr or ("disc_missing" in quality_flags):
            need_llm = True
        # 不合理値・文字化け・短すぎ・汎用タイトル 等
        if strict and any(f in quality_flags for f in
                          ["title_short","title_generic","title_garbled","addr_not_jp_like","addr_garbled","yen_unreasonable","pct_unreasonable"]):
            need_llm = True

    if need_llm:
        visible = get_visible_text_preferring_main(soup, fallback_limit=12000)
        llm = llm_retry(visible, resp.url, llm_model, LLM_ENDPOINT, timeout=120, debug=llm_debug, num_ctx=4096)
        if llm:
            used_llm = "YES"
            # タイトル補完/置換（LLM→clean_title で最終整形）
            if (not title or "title_generic" in quality_flags or "title_short" in quality_flags) and llm.get("title"):
                title = clean_title(llm["title"])
                # 再評価
                quality_flags = [f for f in quality_flags if not f.startswith("title_")]
                quality_flags.extend(title_quality_flags(title))

            # 住所補完/置換
            if (not addr or "addr_not_jp_like" in quality_flags) and llm.get("address"):
                addr = llm["address"]
                quality_flags = [f for f in quality_flags if not f.startswith("addr_")]
                quality_flags.extend(address_quality_flags(addr))

            # 割引は (1) 欠損時, (2) 不合理値 のとき置き換え可
            ly = llm.get("discount_value_yen")
            lp = llm.get("discount_percent")
            lt = llm.get("discount_text")

            if (disc_yen is None or not is_reasonable_yen(disc_yen)) and isinstance(ly, int) and is_reasonable_yen(ly):
                disc_yen = ly
            if (disc_pct is None or not is_reasonable_pct(disc_pct)) and isinstance(lp, int) and is_reasonable_pct(lp):
                disc_pct = lp
            if (not disc_text) and lt:
                disc_text = lt

            # フラグ再評価
            quality_flags = [f for f in quality_flags if f not in ("yen_unreasonable","pct_unreasonable","disc_missing")]
            quality_flags.extend(discount_quality_flags(disc_text, disc_yen, disc_pct))

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

# ========== 収集オーケストレーション ==========
def scrape_from_final(final_csv: str, output_csv: str, limit=None, domain_sleep=0.3, hop=True,
                      llm=True, llm_model=LLM_DEFAULT_MODEL, llm_debug=False, strict=False):
    """
    Read final CSV (must include url, scraping_allowed),
    scrape only rows with scraping_allowed == 'YES',
    write output_csv with extracted fields.
    """
    ensure_parent(output_csv)
    targets = []
    with open(final_csv, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if (row.get("scraping_allowed") or "").upper() == "YES":
                u = (row.get("url") or "").strip()
                if u:
                    targets.append(u)
    if limit:
        targets = targets[:limit]

    fieldnames = [
        "url", "final_url", "source_page",
        "title", "address",
        "discount_text", "discount_value_yen", "discount_percent",
        "used_llm", "hop_used", "quality_flags",
        "error"
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f_out:
        w = csv.DictWriter(f_out, fieldnames=fieldnames)
        w.writeheader()

        last_netloc = None
        for url in targets:
            netloc = urlparse(url).netloc
            if last_netloc and last_netloc != netloc:
                time.sleep(max(domain_sleep, 0.6))
            res = scrape_one(url, domain_sleep=domain_sleep, hop=hop,
                             use_llm=llm, llm_model=llm_model, llm_debug=llm_debug, strict=strict)
            w.writerow(res)
            last_netloc = netloc

    print(f"✅ Scraped: {len(targets)} URLs → {output_csv}")
    return output_csv

# ========== CLI ==========
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scrape allowed URLs and extract title/address/student discount.")
    parser.add_argument("final_csv", help="final_checked/{basename}_final.csv (from pipeline)")
    parser.add_argument("--out", default="./scraped/tickets_scraped.csv", help="output CSV path")
    parser.add_argument("--limit", type=int, default=None, help="limit number of pages (debug)")
    parser.add_argument("--sleep", type=float, default=0.3, help="per-request sleep seconds")
    parser.add_argument("--no-hop", action="store_true", help="disable in-domain price page hop")
    parser.add_argument("--llm-off", action="store_true", help="disable ollama LLM usage")
    parser.add_argument("--llm-model", default=LLM_DEFAULT_MODEL, help="ollama model tag")
    parser.add_argument("--llm-debug", action="store_true", help="print raw LLM outputs/errors")
    parser.add_argument("--strict", action="store_true", help="use strict sanity checks to trigger LLM re-extraction")

    args = parser.parse_args()
    scrape_from_final(
        args.final_csv, args.out,
        limit=args.limit, domain_sleep=args.sleep, hop=not args.no_hop,
        llm=not args.llm_off, llm_model=args.llm_model, llm_debug=args.llm_debug, strict=args.strict
    )
