# -*- coding: utf-8 -*-
"""
Geocode CSV (Japan)
- 入力: id,name,url,address,lat,lon,tags,description,image_url,price
- 処理:
  1) address から日本住所を抽出（郵便番号/都道府県〜番地）
  2) "オンライン" / "多数" を含む場合はスキップ
  3) Google Geocoding API（language=ja, region=jp）で geocode
  4) lat,lon を埋めて CSV 出力（既存lat/lonがある行はデフォルトで保持）
- APIキー: --api-key または環境変数 GOOGLE_MAPS_API_KEY

使い方:
  python geocode_csv.py input.csv --out output.csv --api-key YOUR_KEY
  # 既存 lat/lon を上書きしたい場合:
  python geocode_csv.py input.csv --out output.csv --overwrite
"""

import csv
import json
import os
import re
import time
import argparse
from pathlib import Path
from typing import Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# 先頭の import 群の直後あたりに追加（try/exceptで安全）
try:
    from dotenv import load_dotenv
    load_dotenv()  # .env をカレントや親階層から自動ロード
except Exception:
    pass

# ===== 日本住所抽出用パターン =====
PREFS = (
    "北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|"
    "新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|"
    "奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|"
    "長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県"
)
POSTAL_RE = r"(〒?\s*\d{3}[-‐–－]?\d{4})"
ADDRESS_RE = re.compile(
    rf"({POSTAL_RE}\s*)?({PREFS}).+?[0-9０-９\-−ー－丁目番地号\.、,\sF階階地目]+",
    re.S
)

# ===== ユーティリティ =====
def ensure_parent(path_str: str) -> str:
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)
    return path_str

def make_session(timeout=12) -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=3, connect=3, read=3, backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess._timeout = timeout  # type: ignore[attr-defined]
    return sess

SESSION = make_session()

def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _normalize_hyphen(s: str) -> str:
    # 似たハイフンを ASCII ハイフンへ統一
    return (s or "").translate(str.maketrans("‐–－ー", "----"))

def _to_hankaku_digits(s: str) -> str:
    return (s or "").translate(str.maketrans("０１２３４５６７８９", "0123456789"))

def _strip_postal(s: str) -> str:
    return re.sub(POSTAL_RE, "", s).strip()

def extract_jp_address(addr: str) -> str:
    """
    入力の address 文字列から日本の住所（郵便番号〜番地）っぽい部分を抽出。
    見つからなければ空文字。
    """
    if not addr:
        return ""
    text = _normalize_hyphen(_clean_spaces(addr))
    m = ADDRESS_RE.search(text)
    if m:
        # 末尾の明らかなノイズを軽く除去
        cand = m.group(0)
        cand = re.split(r"(TEL|電話|営業時間|Open|OPEN|Google\s*map|Google\s*Maps)[:：]?", cand)[0]
        return _clean_spaces(cand)
    # 郵便番号のみ + 後続が弱いケースにも対応（郵便番号があるなら先頭固定で返す）
    m2 = re.search(POSTAL_RE, text)
    if m2:
        start = m2.start()
        tail = text[start:start+64]
        return _clean_spaces(tail)
    return ""

def looks_online_or_many(addr: str) -> bool:
    s = addr or ""
    return ("オンライン" in s) or ("多数" in s) or ("住所多数" in s)

# ===== キャッシュ =====
def load_cache(cache_path: Optional[str]) -> dict:
    if not cache_path:
        return {}
    p = Path(cache_path)
    if p.is_file():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(cache_path: Optional[str], data: dict) -> None:
    if not cache_path:
        return
    ensure_parent(cache_path)
    Path(cache_path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# ===== Geocoding =====
def geocode_google(address: str, api_key: str, sleep_sec: float = 0.15, debug: bool = False) -> Optional[Tuple[float, float]]:
    """
    Google Geocoding API で住所をジオコーディングして (lat, lon) を返す。
    - 1回目: 与えた住所そのまま
    - 2回目: 郵便番号を外す + 数字を半角化 + 余分な空白を圧縮
    失敗したら None。
    """
    if not api_key:
        if debug:
            print("[GC] no api_key")
        return None

    def _hit(q: str) -> Tuple[Optional[Tuple[float,float]], str, str]:
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {"address": q, "key": api_key, "language": "ja", "region": "jp"}
        time.sleep(max(0.0, sleep_sec))
        try:
            r = SESSION.get(url, params=params, timeout=getattr(SESSION, "_timeout", 12))
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            return None, "HTTP_ERR", str(e)

        status = (data.get("status") or "").upper()
        if status == "OK":
            results = data.get("results") or []
            if not results:
                return None, "EMPTY_RESULTS", ""
            loc = ((((results[0] or {}).get("geometry") or {}).get("location")) or {})
            lat = loc.get("lat"); lng = loc.get("lng")
            if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
                return (float(lat), float(lng)), status, ""
            return None, "NO_GEOM", ""
        else:
            # よくある: ZERO_RESULTS, OVER_QUERY_LIMIT, REQUEST_DENIED, INVALID_REQUEST
            return None, status, (data.get("error_message") or "")

    # 1回目（そのまま）
    addr1 = _clean_spaces(_normalize_hyphen(address))
    if debug: print(f"[GC] try1: '{addr1}'")
    pair, st, em = _hit(addr1)
    if debug and (pair is None): print(f"[GC] status={st} msg={em}")

    if pair is not None:
        return pair

    # 2回目（郵便番号削除 + 半角化 + 再圧縮）
    addr2 = _clean_spaces(_to_hankaku_digits(_normalize_hyphen(_strip_postal(addr1))))
    if addr2 and addr2 != addr1:
        if debug: print(f"[GC] try2: '{addr2}'")
        pair2, st2, em2 = _hit(addr2)
        if debug and (pair2 is None): print(f"[GC] status={st2} msg={em2}")
        if pair2 is not None:
            return pair2

    return None

# ===== 本体 =====
def process_csv(
    input_csv: str,
    output_csv: str,
    api_key: str,
    cache_path: Optional[str] = "./cache/geocode_cache.json",
    only_missing: bool = True,
    overwrite: bool = False,
    sleep_sec: float = 0.15,
    debug: bool = False,
) -> str:
    ensure_parent(output_csv)
    cache = load_cache(cache_path)

    with open(input_csv, newline="", encoding="utf-8-sig") as f_in:
        reader = csv.DictReader(f_in)
        # 出力ヘッダは入力の順序を保ちつつ lat/lon を確実に含める
        fieldnames = reader.fieldnames or [
            "id","name","url","address","lat","lon","tags","description","image_url","price"
        ]
        # lat/lon が無ければ追加
        if "lat" not in fieldnames: fieldnames.append("lat")
        if "lon" not in fieldnames: fieldnames.append("lon")

        with open(output_csv, "w", newline="", encoding="utf-8-sig") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                addr_raw = (row.get("address") or "").strip()
                lat_val  = (row.get("lat") or "").strip()
                lon_val  = (row.get("lon") or "").strip()

                # 既に lat/lon があり、overwrite=False なら触らない
                if (lat_val and lon_val) and not overwrite:
                    writer.writerow(row)
                    continue

                # 「オンライン」「多数」はジオコーディングしない
                if looks_online_or_many(addr_raw):
                    row["lat"] = lat_val
                    row["lon"] = lon_val
                    writer.writerow(row)
                    continue

                # 住所抽出
                addr = extract_jp_address(addr_raw)
                if not addr:
                    row["lat"] = lat_val
                    row["lon"] = lon_val
                    writer.writerow(row)
                    continue

                # キャッシュ確認
                cache_key = addr
                if cache_key in cache:
                    res = cache[cache_key]
                    row["lat"] = str(res.get("lat",""))
                    row["lon"] = str(res.get("lon",""))
                    writer.writerow(row)
                    continue

                # Geocode 実行
                pair = geocode_google(addr, api_key=api_key, sleep_sec=sleep_sec, debug=debug) if api_key else None

                if pair is not None:
                    lat, lon = pair
                    row["lat"] = f"{lat:.8f}"
                    row["lon"] = f"{lon:.8f}"
                    cache[cache_key] = {"lat": lat, "lon": lon}
                    save_cache(cache_path, cache)
                else:
                    # 失敗時は空のまま
                    row["lat"] = lat_val
                    row["lon"] = lon_val

                writer.writerow(row)

    return output_csv

def main():
    ap = argparse.ArgumentParser(description="Geocode Japanese addresses in CSV (Google Geocoding API).")
    ap.add_argument("input", help="input CSV path (utf-8-sig). Must include columns: id,name,url,address,lat,lon,...")
    ap.add_argument("--out", default="", help="output CSV path (default: ./scraped/<basename>_geocoded.csv)")
    ap.add_argument("--api-key", default=os.environ.get("GOOGLE_MAPS_API_KEY", ""), help="Google Geocoding API key or set env GOOGLE_MAPS_API_KEY")
    ap.add_argument("--cache", default="./cache/geocode_cache.json", help="cache file path (json)")
    ap.add_argument("--sleep", type=float, default=0.15, help="sleep seconds per API request (rate limiting)")
    ap.add_argument("--overwrite", action="store_true", help="overwrite lat/lon even if already present")
    ap.add_argument("--all", dest="only_missing", action="store_false", help="geocode all rows (kept for compatibility)")
    ap.add_argument("--debug", action="store_true", help="print debug logs")  # ← 追加
    args = ap.parse_args()

    input_csv = args.input
    if not args.out:
        base = Path(input_csv).stem
        out_csv = ensure_parent(f"./scraped/{base}_geocoded.csv")
    else:
        out_csv = args.out

    out = process_csv(
        input_csv=input_csv,
        output_csv=out_csv,
        api_key=args.api_key or "",
        cache_path=args.cache,
        only_missing=True,
        overwrite=args.overwrite,
        sleep_sec=args.sleep,
        debug=args.debug,   # ← 追加
    )
    print(f"✅ Geocoded → {out}")

if __name__ == "__main__":
    main()
