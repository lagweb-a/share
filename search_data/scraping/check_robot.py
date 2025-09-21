"""
入力:  title,url[,snippet] のCSV（例: ./csv/student_discount_ticket.csv）
出力:  title,url[,snippet],robots_url,robots_http_status,robots_can_fetch,notes のCSV
判定:  robots.txt の User-agent: * に対して、その url のパスが許可/不許可/不明(unknown)か
"""

import csv
import time
import requests
from urllib.parse import urlparse
from urllib import robotparser
from typing import Dict, Any, Tuple, Optional

# デフォルト設定（append_robots_info() で上書き可能）
DEFAULT_TIMEOUT = 10
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SpotAppRobot/1.0; +https://example.com/robot)"
}
DEFAULT_SLEEP_NEW_NETLOC = 0.6
DEFAULT_SLEEP_SAME_NETLOC = 0.15

__all__ = ["append_robots_info"]

def _get_robots_info_for_netloc(
    scheme: str,
    netloc: str,
    *,
    timeout: int,
    headers: Dict[str, str],
    robots_cache: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    netloc の robots.txt を取得して RobotFileParser を返す（キャッシュ込み）
    仕様:
      - 404 は unknown 扱い（allowed にしない）
      - 200 でも 'User-agent:' が本文に無ければ unknown 扱い
    """
    if netloc in robots_cache:
        return robots_cache[netloc]

    robots_url = f"{scheme}://{netloc}/robots.txt"
    info: Dict[str, Any] = {
        "robots_url": robots_url,
        "status_code": None,
        "rp": None,
        "notes": "",
        "has_directives": False,
    }

    try:
        resp = requests.get(robots_url, headers=headers, timeout=timeout, allow_redirects=True)
        info["robots_url"] = resp.url if resp is not None else robots_url
        status = resp.status_code if resp is not None else None
        info["status_code"] = status

        # 404 → unknown（保守的）
        if status == 404:
            info["notes"] = "robots.txt not found"
            robots_cache[netloc] = info
            return info

        # 2xx → 解析。ただし directives（User-agent: 等）が無ければ unknown
        if status and 200 <= status < 300 and resp.text:
            text = resp.text
            if "user-agent" not in text.lower():
                info["notes"] = "200 but no robots directives detected"
                robots_cache[netloc] = info
                return info

            rp = robotparser.RobotFileParser()
            rp.parse(text.splitlines())
            info["rp"] = rp
            info["has_directives"] = True
            robots_cache[netloc] = info
            return info

        # 403/401/5xx 等 → unknown
        info["notes"] = f"robots.txt returned HTTP {status}"
        robots_cache[netloc] = info
        return info

    except requests.RequestException as e:
        info["notes"] = f"request error: {e.__class__.__name__}"
        robots_cache[netloc] = info
        return info


def _can_fetch_url(
    url: str,
    *,
    user_agent: str,
    timeout: int,
    headers: Dict[str, str],
    robots_cache: Dict[str, Dict[str, Any]],
) -> Tuple[str, Optional[str], Optional[int], str]:
    """
    URLに対して robots.txt 上の可否を判定。
    返り値: (can_fetch_str: 'allowed'/'blocked'/'unknown', robots_url, status_code, notes)
    """
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return "unknown", None, None, "invalid url"

        info = _get_robots_info_for_netloc(
            parsed.scheme, parsed.netloc,
            timeout=timeout, headers=headers, robots_cache=robots_cache
        )

        # directives が取得できた場合のみ can_fetch を使う
        if info.get("has_directives") and info.get("rp") is not None:
            # 一般には path のみを判定対象に（クエリは除外する方が通例）
            path = parsed.path or "/"
            can = info["rp"].can_fetch(user_agent, path)
            return ("allowed" if can else "blocked"), info["robots_url"], info["status_code"], info["notes"]

        # 404 / directivesなし / エラー等 → unknown
        return "unknown", info.get("robots_url"), info.get("status_code"), info.get("notes")

    except Exception as e:
        return "unknown", None, None, f"unexpected error: {e.__class__.__name__}"


def append_robots_info(
    input_csv: str,
    output_csv: str,
    *,
    user_agent: str = "*",
    timeout: int = DEFAULT_TIMEOUT,
    headers: Dict[str, str] = DEFAULT_HEADERS,
    sleep_new_netloc: float = DEFAULT_SLEEP_NEW_NETLOC,
    sleep_same_netloc: float = DEFAULT_SLEEP_SAME_NETLOC,
) -> str:
    """
    外部から呼び出せる関数。本関数は input_csv を読み、robots判定を付与して output_csv に書き出す。
    戻り値は output_csv のパス。
    """
    robots_cache: Dict[str, Dict[str, Any]] = {}

    with open(input_csv, newline="", encoding="utf-8") as f_in:
        reader = csv.DictReader(f_in)
        fieldnames_in = reader.fieldnames or []
        has_snippet = "snippet" in set(fn or "" for fn in fieldnames_in)

        # 出力CSVヘッダ
        base_cols = ["title", "url"]
        if has_snippet:
            base_cols.append("snippet")
        extra_cols = ["robots_url", "robots_http_status", "robots_can_fetch", "notes"]
        fieldnames_out = base_cols + extra_cols

        with open(output_csv, "w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames_out)
            writer.writeheader()

            seen = set()         # 重複URL除去
            last_netloc = None   # ドメイン切替ウェイト

            for row in reader:
                title = (row.get("title") or "").strip()
                url   = (row.get("url") or "").strip()

                if not url:
                    continue
                if url in seen:
                    continue
                seen.add(url)

                parsed = urlparse(url)
                netloc = parsed.netloc

                # ドメインごとにウェイト（礼儀 & ブロック回避）
                if last_netloc is None or last_netloc != netloc:
                    time.sleep(sleep_new_netloc)
                    last_netloc = netloc
                else:
                    time.sleep(sleep_same_netloc)

                # robots判定
                can, robots_url, status, notes = _can_fetch_url(
                    url,
                    user_agent=user_agent,
                    timeout=timeout,
                    headers=headers,
                    robots_cache=robots_cache,
                )

                out = {
                    "title": title,
                    "url": url,
                    "robots_url": robots_url or "",
                    "robots_http_status": status if status is not None else "",
                    "robots_can_fetch": can,
                    "notes": notes or ""
                }
                if has_snippet:
                    out["snippet"] = row.get("snippet", "")

                writer.writerow(out)

    return output_csv


# 直接実行時は従来通りの入出力で回す
if __name__ == "__main__":
    INPUT_CSV  = "./csv/test.csv"
    OUTPUT_CSV = "./robot_checked/test.csv"
    out_path = append_robots_info(
        INPUT_CSV,
        OUTPUT_CSV,
        user_agent="*",
        timeout=DEFAULT_TIMEOUT,
        headers=DEFAULT_HEADERS,
        sleep_new_netloc=DEFAULT_SLEEP_NEW_NETLOC,
        sleep_same_netloc=DEFAULT_SLEEP_SAME_NETLOC,
    )
    print(f"✅ 完了: {out_path}")
