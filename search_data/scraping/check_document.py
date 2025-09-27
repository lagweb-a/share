# -*- coding: utf-8 -*-
"""
既存CSV(robots判定付き)に、利用規約(=ToS)スクレイピングの可否を追記する関数版（高速化）
- サブドメイン→親ドメイン(eTLD+1)フォールバック
- 既知サイトマップ（asoview等）
- 規約ページは見つかったが禁止/許可文言なし → tos_reason='tos_found_no_signal'
- 時短ポイント:
  * 候補URLを優先度順に少数だけチェック（早期打ち切り）
  * 代表ページのリンク探索は "/" と "/about/" "/company/" に限定・上限つき
  * サイトマップは最大5件、ヒットURLも短いものを各キー最大2件まで
  * HEADで存在確認→必要時のみGET（HTML読み込みを減らす）
  * サブドメイン群で同一apex結果を共有する広域キャッシュ
  * 正規表現の事前コンパイル
"""

import csv
import time
import re
from pathlib import Path
from urllib.parse import urlparse, urljoin
import xml.etree.ElementTree as ET
from typing import Dict, Any, Tuple, Set, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import tldextract  # pip install tldextract

__all__ = ["append_tos_info"]

# ====== 既定設定（append_tos_info() の引数で上書き可能）======
DEFAULT_TIMEOUT = 12
DEFAULT_SLEEP_NEW_NETLOC = 0.6
DEFAULT_SLEEP_LIGHT = 0.15
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SpotAppRobot/1.0; +https://example.com/robot)",
    "Accept-Encoding": "gzip, deflate, br",
}

# ---- 時短用上限（必要に応じて調整可能。精度担保のため保守的に設定）----
MAX_DISCOVER_LINKS_PER_PAGE = 6   # 代表ページ1枚あたり拾うリンク上限
MAX_DISCOVER_PAGES = 3            # "/", "/about/", "/company/" のみに限定
MAX_SITEMAPS = 5                  # robots.txt内のSitemap: 最大5件
MAX_SITEMAP_HITS_PER_KEY = 2      # terms/kiyaku等のキーごとに短いURL上位2件まで
MAX_CANDIDATES_TOTAL = 18         # 最終的に検査する候補URLの総数上限（早期打ち切り）

def _ensure_parent(path_str: str) -> str:
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)
    return path_str

def _make_session(headers: Dict[str, str], timeout: int) -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=3, connect=3, read=3, backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=32, pool_maxsize=32)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update(headers)
    # timeout は各リクエスト時に使用
    return sess

# 既知サイトの規約URL（優先）
KNOWN_TOS_MAP = {
    "www.asoview.com": "/terms/",
    "asoview.com": "/terms/",
    "ja.wikipedia.org": "https://foundation.wikimedia.org/wiki/Special:MyLanguage/Policy:Terms_of_Use",
    "wikipedia.org":   "https://foundation.wikimedia.org/wiki/Special:MyLanguage/Policy:Terms_of_Use",
}

# 候補パス（優先度順：短く一般的なものを先頭に）
CANDIDATE_PATHS = [
    "/terms", "/terms/", "/tos", "/tos/",
    "/terms-of-service", "/terms-of-service/",
    "/terms-and-conditions", "/terms-and-conditions/",
    "/legal/terms", "/legal/terms/",
    "/agreement", "/agreement/", "/user-agreement", "/user-agreement/",
    "/rules", "/rules/",
    "/policy", "/policy/", "/policies", "/policies/", "/sitepolicy", "/sitepolicy/",
    "/kiyaku", "/kiyaku/", "/riyokiyaku", "/riyokiyaku/",
    "/利用規約", "/利用規約/",
    "/ご利用条件", "/ご利用条件/", "/会員規約", "/会員規約/",
    "/guide/terms", "/guide/terms/", "/help/terms", "/help/terms/",
    "/company/terms", "/company/terms/", "/about/terms", "/about/terms/",
    "/terms.html", "/rule.html", "/rules.html", "/kiyaku.html", "/policy.html", "/agreement.html",
]

ANCHOR_KEYWORDS = [
    "利用規約","規約","会員規約","サイトポリシー","ご利用にあたって","ご利用条件","約款",
    "Terms","Terms of Service","Terms & Conditions","Policies","Policy","Legal","Rules","Agreement","User Agreement"
]

# ---- 判定パターン（事前コンパイル）----
_FORBID_RES = [
    re.compile(p, re.I) for p in [
        r"スクレイピング(を)?(禁止|禁ずる|しないで)",
        r"クローリング(を)?(禁止|禁ずる)",
        r"自動(化|的)手段(での)?(アクセス|取得|収集)を?禁止",
        r"(ボット|bot|ロボット|robot|クローラ|crawler|spider).*(禁止|不可|許可しない)",
        r"データ(の)?(収集|抽出|マイニング|収拾).*(禁止|不可)",
        r"\b(scrap(e|ing)|crawl(ing)?|spider(ing)?|harvest(ing)?|automated\s+means)\b.*(prohibit|forbid|not\s+allow|disallow|禁止)",
    ]
]
_ALLOW_RES = [
    re.compile(p, re.I) for p in [
        r"公式API(の)?利用(を)?認め(る|ています)",
        r"API(の)?利用(が)?可能",
        r"データ(の)?(引用|転載)は(出典明記|条件付き)で可",
        r"\bAPI\b.*(allowed|permit|利用可|ご利用いただけます)",
        r"Creative\s*Commons|CC[- ]BY|オープンデータ|Open\s*Data",
    ]
]
_CONDITIONAL_RES = [
    re.compile(p, re.I) for p in [
        r"(事前|書面)の(許可|承諾)が必要",
        r"当社の(許諾|承認)なく.*(禁止|できません)",
        r"商用(目的|利用)は(禁止|不可)",
        r"非商用(に限り|のみ)許可",
        r"(合理的|一定)の範囲(内)?での(引用|転載).*(可|認める)",
        r"\bwith\s+prior\s+(written\s+)?consent\b",
    ]
]

def _normalize_text(html_or_text: str) -> str:
    t = re.sub(r"<[^>]+>", " ", html_or_text or "")
    t = re.sub(r"\s+", " ", t)
    return t.strip()

def _make_snippet(text: str, span: Tuple[int, int], width=140) -> str:
    start, end = span
    s = max(0, start - width//2); e = min(len(text), end + width//2)
    return re.sub(r"\s+", " ", text[s:e].strip())[:width]

def _head_exists(session: requests.Session, url: str, timeout: int) -> Tuple[bool, int, str, str]:
    """HEADで存在/種別を素早く確認してから、必要ならGETに進む"""
    try:
        r = session.head(url, timeout=timeout, allow_redirects=True)
        ctype = (r.headers.get("Content-Type") or "").lower()
        return (200 <= r.status_code < 400), r.status_code, ctype, r.url
    except requests.RequestException:
        return False, 0, "", ""

def _get(session: requests.Session, url: str, timeout: int):
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        ctype = (resp.headers.get("Content-Type") or "").lower()
        return resp, ctype
    except requests.RequestException:
        return None, None

def _discover_links(session: requests.Session, page_url: str, timeout: int) -> List[str]:
    """代表ページから規約らしいアンカーを少数だけ拾う（テキスト/URLともにキーワード判定）"""
    out: List[str] = []
    resp, ctype = _get(session, page_url, timeout)
    if not resp or not resp.text or "text/html" not in (ctype or ""):
        return out
    soup = BeautifulSoup(resp.text, "lxml") if BeautifulSoup else BeautifulSoup(resp.text, "html.parser")
    hits = 0
    for a in soup.select("a[href]"):
        if hits >= MAX_DISCOVER_LINKS_PER_PAGE:
            break
        txt = (a.get_text() or "").strip()
        href = a.get("href") or ""
        if not href:
            continue
        test = f"{txt} {href}"
        if any(k.lower() in test.lower() for k in ANCHOR_KEYWORDS):
            out.append(urljoin(resp.url, href))
            hits += 1
    return out

def _enumerate_tos_candidates(session: requests.Session, base: str, timeout: int) -> List[str]:
    # base は "https://netloc"
    cand: List[str] = []

    # 1) 代表的な固定パス（優先度順）をまず詰める
    for p in CANDIDATE_PATHS:
        cand.append(base + p)

    # 2) 代表ページからのリンク探索（上限＆ページ限定）
    for p in ["/", "/about/", "/company/"][:MAX_DISCOVER_PAGES]:
        cand.extend(_discover_links(session, urljoin(base, p), timeout))

    # 3) robots.txt → sitemap
    robots_url = urljoin(base, "/robots.txt")
    sm_resp, _ = _get(session, robots_url, timeout)
    sitemap_urls: List[str] = []
    if sm_resp and sm_resp.ok and sm_resp.text:
        for line in sm_resp.text.splitlines():
            if line.lower().startswith("sitemap:"):
                sm = line.split(":", 1)[1].strip()
                sitemap_urls.append(sm)
    sitemap_urls = sitemap_urls[:MAX_SITEMAPS]

    # 4) 軽量サイトマップ走査（各キーごとに短いURL上位のみ）
    key_subs = ("terms", "kiyaku", "policy", "policies", "rules", "agreement", "riyokiyaku", "sitepolicy")
    for sm in sitemap_urls:
        r, _ctype = _get(session, sm, timeout)
        if not r or not r.ok:
            continue
        text = r.text or ""
        urls: List[str] = []
        try:
            root = ET.fromstring(text)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for loc in root.findall(".//sm:url/sm:loc", ns):
                if loc.text:
                    urls.append(loc.text.strip())
            if not urls:
                for loc in root.findall(".//sm:sitemap/sm:loc", ns):
                    if loc.text:
                        urls.append(loc.text.strip())
        except ET.ParseError:
            urls = re.findall(r"<loc>\s*([^<]+)\s*</loc>", text, flags=re.I)
        if not urls:
            continue
        # キーごとに短いURL順で2件まで
        lower_map = {}
        for u in urls:
            lu = (u or "").lower()
            for k in key_subs:
                if k in lu:
                    lower_map.setdefault(k, []).append(u)
        for k, items in lower_map.items():
            items_sorted = sorted(items, key=lambda x: len(x or ""))
            cand.extend(items_sorted[:MAX_SITEMAP_HITS_PER_KEY])

    # 5) 重複整理＆上限カット（短いURL優先）
    uniq = []
    seen = set()
    for u in cand:
        u2 = (u or "").rstrip("/")
        if u2 and u2 not in seen:
            seen.add(u2); uniq.append(u2)
    uniq_sorted = sorted(uniq, key=lambda x: len(x))
    return uniq_sorted[:MAX_CANDIDATES_TOTAL]

def _judge_from_text(text: str):
    for cre in _FORBID_RES:
        m = cre.search(text)
        if m:
            return "forbidden", "matched_forbid", _make_snippet(text, m.span())
    for cre in _ALLOW_RES:
        m = cre.search(text)
        if m:
            return "allowed", "matched_allow", _make_snippet(text, m.span())
    for cre in _CONDITIONAL_RES:
        m = cre.search(text)
        if m:
            return "conditional", "matched_conditional", _make_snippet(text, m.span())
    return "unknown", "no_signal", ""

def _evaluate_candidate(session: requests.Session, url: str, timeout: int, prefer_reason_prefix: str = "") -> Dict[str, Any]:
    """個別候補の評価（HEAD→必要ならGET）。HTML以外はPDFのみ特例扱い。"""
    ok, status, ctype, final_url = _head_exists(session, url, timeout)
    if not ok and status not in (405, 403):  # HEAD非対応や権限系はGETにフォールバック
        return {}
    # PDFは即unknown（PDF terms検知）
    if "pdf" in (ctype or ""):
        return {
            "tos_url": final_url or url, "tos_http_status": status or "",
            "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "pdf_terms_detected").strip(),
            "tos_evidence": ""
        }
    # HTML以外はスキップ
    if "text/html" not in (ctype or ""):
        # Content-Typeが取れない/HEADで不明ならGETして判定
        resp, ctype2 = _get(session, url, timeout)
        if not resp:
            return {}
        if "pdf" in (ctype2 or ""):
            return {
                "tos_url": resp.url, "tos_http_status": resp.status_code,
                "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "pdf_terms_detected").strip(),
                "tos_evidence": ""
            }
        if "text/html" not in (ctype2 or ""):
            return {}
        html = resp.text or ""
        if not html:
            return {
                "tos_url": resp.url, "tos_http_status": resp.status_code,
                "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "empty_html").strip(),
                "tos_evidence": ""
            }
        soup = BeautifulSoup(html, "lxml") if BeautifulSoup else BeautifulSoup(html, "html.parser")
        text = _normalize_text(soup.get_text(" "))
        if not text:
            return {
                "tos_url": resp.url, "tos_http_status": resp.status_code,
                "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "empty_html").strip(),
                "tos_evidence": ""
            }
        verdict, reason, evidence = _judge_from_text(text)
        if verdict == "unknown":
            title = (soup.title.string.strip() if soup.title and soup.title.string else "")
            if any(k.lower() in title.lower() for k in ANCHOR_KEYWORDS):
                return {
                    "tos_url": resp.url, "tos_http_status": resp.status_code,
                    "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "tos_found_no_signal").strip(),
                    "tos_evidence": ""
                }
            return {}
        return {
            "tos_url": resp.url, "tos_http_status": resp.status_code,
            "tos_can_scrape": verdict, "tos_reason": (prefer_reason_prefix + reason).strip(),
            "tos_evidence": evidence
        }

    # HTML見込み：最初からGETして判定
    resp, ctype = _get(session, url, timeout)
    if not resp:
        return {}
    if "pdf" in (ctype or ""):
        return {
            "tos_url": resp.url, "tos_http_status": resp.status_code,
            "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "pdf_terms_detected").strip(),
            "tos_evidence": ""
        }
    if "text/html" not in (ctype or ""):
        return {}
    html = resp.text or ""
    if not html:
        return {
            "tos_url": resp.url, "tos_http_status": resp.status_code,
            "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "empty_html").strip(),
            "tos_evidence": ""
        }
    soup = BeautifulSoup(html, "lxml") if BeautifulSoup else BeautifulSoup(html, "html.parser")
    text = _normalize_text(soup.get_text(" "))
    if not text:
        return {
            "tos_url": resp.url, "tos_http_status": resp.status_code,
            "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "empty_html").strip(),
            "tos_evidence": ""
        }
    verdict, reason, evidence = _judge_from_text(text)
    if verdict == "unknown":
        title = (soup.title.string.strip() if soup.title and soup.title.string else "")
        if any(k.lower() in title.lower() for k in ANCHOR_KEYWORDS):
            return {
                "tos_url": resp.url, "tos_http_status": resp.status_code,
                "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "tos_found_no_signal").strip(),
                "tos_evidence": ""
            }
        return {}
    return {
        "tos_url": resp.url, "tos_http_status": resp.status_code,
        "tos_can_scrape": verdict, "tos_reason": (prefer_reason_prefix + reason).strip(),
        "tos_evidence": evidence
    }

def _evaluate_on_base(session: requests.Session, base: str, timeout: int, prefer_reason_prefix: str = "") -> Dict[str, Any]:
    """base='https://netloc' を対象に探索・判定（優先度順・早期打ち切り）"""
    result: Dict[str, Any] = {
        "tos_url": "",
        "tos_http_status": "",
        "tos_can_scrape": "unknown",
        "tos_reason": "not_found",
        "tos_evidence": ""
    }

    # 既知URL最優先
    netloc = urlparse(base).netloc.lower()
    known = KNOWN_TOS_MAP.get(netloc)
    if known:
        known_url = known if known.startswith("http") else urljoin(base, known)
        ev = _evaluate_candidate(session, known_url, timeout, prefer_reason_prefix)
        if ev:
            return ev

    # 候補列挙（優先度順・上限あり）
    candidates = _enumerate_tos_candidates(session, base, timeout)
    for url in candidates:
        ev = _evaluate_candidate(session, url, timeout, prefer_reason_prefix)
        if ev:
            return ev
    return result

# キャッシュ（モジュール内で共有）— apexとサブドメインの両方で使い回し
_TOS_CACHE: Dict[str, Dict[str, Any]] = {}
_TOS_CACHE_APEX: Dict[str, Dict[str, Any]] = {}

def _evaluate_tos_for_url(session: requests.Session, url: str, timeout: int) -> Dict[str, Any]:
    """
    1) サブドメインのまま探索
    2) 取れなければ 親ドメイン(eTLD+1) でもう一度
    ※ 両者の結果はキャッシュを共有（多サブドメインで高速化）
    """
    parsed = urlparse(url)
    scheme, netloc = parsed.scheme, parsed.netloc
    key = netloc.lower()
    if key in _TOS_CACHE:
        return _TOS_CACHE[key]

    # 先にapexキャッシュを参照（同じapexの別サブドメインを高速化）
    ext = tldextract.extract(netloc)
    apex = ".".join([p for p in [ext.domain, ext.suffix] if p])  # eTLD+1
    if apex and apex.lower() in _TOS_CACHE_APEX:
        res_apex = _TOS_CACHE_APEX[apex.lower()]
        _TOS_CACHE[key] = res_apex
        return res_apex

    base = f"{scheme}://{netloc}"
    res = _evaluate_on_base(session, base, timeout, prefer_reason_prefix="")
    if res.get("tos_url") or res.get("tos_reason") != "not_found":
        _TOS_CACHE[key] = res
        # サブドメインの結果でも「許可/禁止/条件あり/No Signal取得済み」ならapex側にも共有しておく
        _TOS_CACHE_APEX.setdefault(apex.lower(), res)
        return res

    # eTLD+1 フォールバック
    if apex and apex.lower() != netloc.lower():
        base2 = f"{scheme}://{apex}"
        res2 = _evaluate_on_base(session, base2, timeout, prefer_reason_prefix="apex:")
        _TOS_CACHE[key] = res2
        _TOS_CACHE_APEX[apex.lower()] = res2
        return res2

    _TOS_CACHE[key] = res
    return res


def append_tos_info(
    input_with_robots: str,
    output_with_tos: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    headers: Dict[str, str] = DEFAULT_HEADERS,
    sleep_new_netloc: float = DEFAULT_SLEEP_NEW_NETLOC,
    sleep_same_netloc: float = DEFAULT_SLEEP_LIGHT,
) -> str:
    """
    robots 判定付き CSV に対し、ToS 判定（tos_*列）を追記して保存する。
    戻り値は出力CSVのパス。
    """
    _ensure_parent(output_with_tos)
    session = _make_session(headers, timeout)

    with open(input_with_robots, newline="", encoding="utf-8") as f_in, \
         open(output_with_tos,   "w", newline="", encoding="utf-8") as f_out:

        reader = csv.DictReader(f_in)
        out_fields = list(reader.fieldnames or []) + [
            "tos_url", "tos_http_status", "tos_can_scrape", "tos_reason", "tos_evidence"
        ]
        writer = csv.DictWriter(f_out, fieldnames=out_fields)
        writer.writeheader()

        last_netloc = None
        for row in reader:
            url = (row.get("url") or "").strip()
            if not url:
                writer.writerow(row); continue

            parsed = urlparse(url)
            netloc = parsed.netloc
            if not parsed.scheme or not netloc:
                out = {**row, "tos_url":"", "tos_http_status":"", "tos_can_scrape":"unknown",
                       "tos_reason":"invalid_url", "tos_evidence":""}
                writer.writerow(out); continue

            # レート制御（同一netlocは軽スリープ）
            if last_netloc != netloc:
                time.sleep(sleep_new_netloc)
                last_netloc = netloc
            else:
                time.sleep(sleep_same_netloc)

            tos = _evaluate_tos_for_url(session, url, timeout)

            out = {**row}
            out["tos_url"] = tos.get("tos_url","")
            out["tos_http_status"] = tos.get("tos_http_status","")
            out["tos_can_scrape"] = tos.get("tos_can_scrape","unknown")
            out["tos_reason"] = tos.get("tos_reason","")
            out["tos_evidence"] = tos.get("tos_evidence","")
            writer.writerow(out)

    return output_with_tos


# ---- 単体実行用（従来の動作を残す）----
if __name__ == "__main__":
    INPUT_WITH_ROBOTS = "./robot_checked/student_discount_tickets.csv"
    OUTPUT_WITH_TOS   = "./document_checked/student_discount_tickets_with_tos.csv"
    out_path = append_tos_info(
        INPUT_WITH_ROBOTS,
        OUTPUT_WITH_TOS,
        timeout=DEFAULT_TIMEOUT,
        headers=DEFAULT_HEADERS,
        sleep_new_netloc=DEFAULT_SLEEP_NEW_NETLOC,
        sleep_same_netloc=DEFAULT_SLEEP_LIGHT,
    )
    print(f"✅ 出力: {out_path}")
