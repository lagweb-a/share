# -*- coding: utf-8 -*-
"""
既存CSV(robots判定付き)に、利用規約(=ToS)スクレイピングの可否を追記する関数版
- サブドメイン→親ドメイン(eTLD+1)フォールバック
- 既知サイトマップ（asoview等）
- 規約ページは見つかったが禁止/許可文言なし → tos_reason='tos_found_no_signal'
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
    "User-Agent": "Mozilla/5.0 (compatible; SpotAppRobot/1.0; +https://example.com/robot)"
}

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
    adapter = HTTPAdapter(max_retries=retries)
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

# 候補パスとアンカー語
CANDIDATE_PATHS = [
    "/terms", "/terms/", "/terms-of-service", "/terms-of-service/",
    "/terms-and-conditions", "/terms-and-conditions/",
    "/tos", "/tos/", "/legal/terms", "/legal/terms/",
    "/agreement", "/agreement/", "/user-agreement", "/user-agreement/",
    "/rules", "/rules/", "/guidelines", "/guidelines/",
    "/policy", "/policy/", "/policies", "/policies/", "/sitepolicy", "/sitepolicy/",
    "/kiyaku", "/kiyaku/", "/riyokiyaku", "/riyokiyaku/",
    "/利用規約", "/利用規約/", "/ご利用にあたって", "/ご利用にあたって/",
    "/ご利用条件", "/ご利用条件/", "/会員規約", "/会員規約/",
    "/guide/terms", "/guide/terms/", "/help/terms", "/help/terms/",
    "/company/terms", "/company/terms/", "/about/terms", "/about/terms/",
    "/terms.html", "/rule.html", "/rules.html", "/kiyaku.html", "/policy.html", "/agreement.html",
]

ANCHOR_KEYWORDS = [
    "利用規約","規約","会員規約","サイトポリシー","ご利用にあたって","ご利用条件","約款",
    "Terms","Terms of Service","Terms & Conditions","Policies","Policy","Legal","Rules","Agreement","User Agreement"
]

# 判定パターン
FORBID_PATTERNS = [
    r"スクレイピング(を)?(禁止|禁ずる|しないで)",
    r"クローリング(を)?(禁止|禁ずる)",
    r"自動(化|的)手段(での)?(アクセス|取得|収集)を?禁止",
    r"(ボット|bot|ロボット|robot|クローラ|crawler|spider).*(禁止|不可|許可しない)",
    r"データ(の)?(収集|抽出|マイニング|収拾).*(禁止|不可)",
    r"無断(転載|複製|複写|再配布).*(禁止|不可)",
    r"\b(scrap(e|ing)|crawl(ing)?|spider(ing)?|harvest(ing)?|automated\s+means)\b.*(prohibit|forbid|not\s+allow|disallow|禁止)",
]
ALLOW_PATTERNS = [
    r"公式API(の)?利用(を)?認め(る|ています)",
    r"API(の)?利用(が)?可能",
    r"データ(の)?(引用|転載)は(出典明記|条件付き)で可",
    r"\bAPI\b.*(allowed|permit|利用可|ご利用いただけます)",
    r"Creative\s*Commons|CC[- ]BY|オープンデータ|Open\s*Data",
]
CONDITIONAL_PATTERNS = [
    r"(事前|書面)の(許可|承諾)が必要",
    r"当社の(許諾|承認)なく.*(禁止|できません)",
    r"商用(目的|利用)は(禁止|不可)",
    r"非商用(に限り|のみ)許可",
    r"(合理的|一定)の範囲(内)?での(引用|転載).*(可|認める)",
    r"\bwith\s+prior\s+(written\s+)?consent\b",
]

def _normalize_text(html_or_text: str) -> str:
    t = re.sub(r"<[^>]+>", " ", html_or_text or "")
    t = re.sub(r"\s+", " ", t)
    return t.strip()

def _make_snippet(text: str, span: Tuple[int, int], width=140) -> str:
    start, end = span
    s = max(0, start - width//2); e = min(len(text), end + width//2)
    return re.sub(r"\s+", " ", text[s:e].strip())[:width]

def _fetch(session: requests.Session, url: str, timeout: int):
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        ctype = (resp.headers.get("Content-Type") or "").lower()
        return resp, ctype
    except requests.RequestException:
        return None, None

def _discover_links(session: requests.Session, page_url: str, timeout: int) -> Set[str]:
    out: Set[str] = set()
    resp, ctype = _fetch(session, page_url, timeout)
    if not resp or not resp.text or "text/html" not in (ctype or ""):
        return out
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.select("a[href]"):
        txt = (a.get_text() or "").strip()
        href = a.get("href") or ""
        if not txt or not href:
            continue
        if any(k.lower() in txt.lower() for k in ANCHOR_KEYWORDS):
            out.add(urljoin(resp.url, href))
    return out

def _enumerate_tos_candidates(session: requests.Session, base: str, timeout: int) -> List[str]:
    # base は "https://netloc"
    cand: List[str] = [base + p for p in CANDIDATE_PATHS]
    # 代表ページ群からのリンク探索
    for p in ["/", "/about/", "/company/", "/guide/", "/help/"]:
        cand.extend(list(_discover_links(session, urljoin(base, p), timeout)))
    # robots.txt → sitemap
    robots_url = urljoin(base, "/robots.txt")
    resp, _ = _fetch(session, robots_url, timeout)
    sitemap_urls = []
    if resp and resp.ok and resp.text:
        for line in resp.text.splitlines():
            if line.lower().startswith("sitemap:"):
                sm = line.split(":", 1)[1].strip()
                sitemap_urls.append(sm)
    # 軽量サイトマップ走査
    key_subs = ("terms", "kiyaku", "policy", "policies", "rules", "agreement", "riyokiyaku", "sitepolicy")
    for sm in sitemap_urls[:5]:
        sm_resp, _ = _fetch(session, sm, timeout)
        if not sm_resp or not sm_resp.ok:
            continue
        text = sm_resp.text or ""
        urls = []
        try:
            root = ET.fromstring(text)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for loc in root.findall(".//sm:url/sm:loc", ns):
                urls.append((loc.text or "").strip())
            if not urls:
                for loc in root.findall(".//sm:sitemap/sm:loc", ns):
                    urls.append((loc.text or "").strip())
        except ET.ParseError:
            urls = re.findall(r"<loc>\s*([^<]+)\s*</loc>", text, flags=re.I)
        for u in urls:
            if any(k in (u or "").lower() for k in key_subs):
                cand.append(u)
    # 重複整理
    uniq, seen = [], set()
    for u in cand:
        u2 = (u or "").rstrip("/")
        if u2 and u2 not in seen:
            seen.add(u2); uniq.append(u)
    return uniq

def _judge_from_text(text: str):
    for pat in FORBID_PATTERNS:
        m = re.search(pat, text, flags=re.I)
        if m:
            return "forbidden", "matched_forbid", _make_snippet(text, m.span())
    for pat in ALLOW_PATTERNS:
        m = re.search(pat, text, flags=re.I)
        if m:
            return "allowed", "matched_allow", _make_snippet(text, m.span())
    for pat in CONDITIONAL_PATTERNS:
        m = re.search(pat, text, flags=re.I)
        if m:
            return "conditional", "matched_conditional", _make_snippet(text, m.span())
    return "unknown", "no_signal", ""

def _evaluate_on_base(session: requests.Session, base: str, timeout: int, prefer_reason_prefix: str = "") -> Dict[str, Any]:
    """base='https://netloc' を対象に探索・判定"""
    result: Dict[str, Any] = {
        "tos_url": "",
        "tos_http_status": "",
        "tos_can_scrape": "unknown",
        "tos_reason": "not_found",
        "tos_evidence": ""
    }

    # 既知URL
    netloc = urlparse(base).netloc.lower()
    known = KNOWN_TOS_MAP.get(netloc)
    if known:
        known_url = known if known.startswith("http") else urljoin(base, known)
        resp, ctype = _fetch(session, known_url, timeout)
        if resp and resp.status_code != 404:
            if "pdf" in (ctype or ""):
                result.update({
                    "tos_url": resp.url, "tos_http_status": resp.status_code,
                    "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "pdf_terms_detected").strip(),
                    "tos_evidence": ""
                })
                return result
            if "text/html" in (ctype or "") and (resp.text or ""):
                soup = BeautifulSoup(resp.text, "html.parser")
                text = _normalize_text(soup.get_text(" "))
                verdict, reason, evidence = _judge_from_text(text)
                if verdict == "unknown":
                    result.update({
                        "tos_url": resp.url, "tos_http_status": resp.status_code,
                        "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "tos_found_no_signal").strip(),
                        "tos_evidence": ""
                    })
                    return result
                result.update({
                    "tos_url": resp.url, "tos_http_status": resp.status_code,
                    "tos_can_scrape": verdict, "tos_reason": (prefer_reason_prefix + reason).strip(),
                    "tos_evidence": evidence
                })
                return result

    # 候補列挙
    candidates = _enumerate_tos_candidates(session, base, timeout)
    for url in candidates:
        resp, ctype = _fetch(session, url, timeout)
        if not resp or resp.status_code == 404:
            continue

        # PDF
        if "pdf" in (ctype or ""):
            result.update({
                "tos_url": resp.url, "tos_http_status": resp.status_code,
                "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "pdf_terms_detected").strip(),
                "tos_evidence": ""
            })
            return result

        if "text/html" not in (ctype or ""):
            continue

        soup = BeautifulSoup(resp.text or "", "html.parser")
        text = _normalize_text(soup.get_text(" "))
        if not text:
            result.update({
                "tos_url": resp.url, "tos_http_status": resp.status_code,
                "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "empty_html").strip(),
                "tos_evidence": ""
            })
            return result

        verdict, reason, evidence = _judge_from_text(text)

        # アンカーテキスト/タイトルに「規約」等があり、シグナル無い場合は tos_found_no_signal
        title = (soup.title.string.strip() if soup.title and soup.title.string else "")
        if verdict == "unknown" and any(k.lower() in title.lower() for k in ANCHOR_KEYWORDS):
            result.update({
                "tos_url": resp.url, "tos_http_status": resp.status_code,
                "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "tos_found_no_signal").strip(),
                "tos_evidence": ""
            })
            return result

        if verdict in ("forbidden","allowed","conditional"):
            result.update({
                "tos_url": resp.url, "tos_http_status": resp.status_code,
                "tos_can_scrape": verdict, "tos_reason": (prefer_reason_prefix + reason).strip(),
                "tos_evidence": evidence
            })
            return result

        # まだ不明なら次候補へ
        last = {
            "tos_url": resp.url, "tos_http_status": resp.status_code,
            "tos_can_scrape": "unknown", "tos_reason": (prefer_reason_prefix + "no_signal").strip(),
            "tos_evidence": ""
        }
        result = last  # 最後の結果を保持

    return result

# キャッシュ（モジュール内で共有）
_TOS_CACHE: Dict[str, Dict[str, Any]] = {}

def _evaluate_tos_for_url(session: requests.Session, url: str, timeout: int) -> Dict[str, Any]:
    """
    1) サブドメインのまま探索
    2) 取れなければ 親ドメイン(eTLD+1) でもう一度
    """
    parsed = urlparse(url)
    scheme, netloc = parsed.scheme, parsed.netloc
    key = netloc.lower()
    if key in _TOS_CACHE:
        return _TOS_CACHE[key]

    base = f"{scheme}://{netloc}"
    res = _evaluate_on_base(session, base, timeout, prefer_reason_prefix="")
    if res.get("tos_url") or res.get("tos_reason") != "not_found":
        _TOS_CACHE[key] = res
        return res

    # eTLD+1 フォールバック
    ext = tldextract.extract(netloc)
    registrable = ".".join([p for p in [ext.domain, ext.suffix] if p])
    if registrable and registrable.lower() != netloc.lower():
        base2 = f"{scheme}://{registrable}"
        res2 = _evaluate_on_base(session, base2, timeout, prefer_reason_prefix="apex:")
        _TOS_CACHE[key] = res2
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

            # レート制御
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
