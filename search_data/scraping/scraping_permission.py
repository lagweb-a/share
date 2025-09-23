# -*- coding: utf-8 -*-
"""
robots_check + document_check の結果を統合して、
スクレイピング可能か (scraping_allowed) を判定するスクリプト
"""

import csv
from pathlib import Path


def ensure_parent(path_str: str) -> str:
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)
    return path_str


def append_scraping_permission(robot_csv: str, doc_csv: str, output_csv: str):
    """
    robot_check と document_check の結果をマージして
    'scraping_allowed' カラムを付与する
    """

    # ロボット結果を辞書化 {url: row}
    robots = {}
    with open(robot_csv, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            url = row.get("url", "").strip()
            if url:
                robots[url] = row

    # ドキュメント結果を辞書化 {url: row}
    docs = {}
    with open(doc_csv, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            url = row.get("url", "").strip()
            if url:
                docs[url] = row

    # 出力フィールド
    out_fields = []
    if robots:
        out_fields.extend(list(next(iter(robots.values())).keys()))
    if docs:
        for k in next(iter(docs.values())).keys():
            if k not in out_fields:
                out_fields.append(k)
    out_fields.append("scraping_allowed")

    ensure_parent(output_csv)
    with open(output_csv, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=out_fields)
        writer.writeheader()

        # 全URLの和集合で処理
        all_urls = set(robots.keys()) | set(docs.keys())
        for url in sorted(all_urls):
            row = {}
            if url in robots:
                row.update(robots[url])
            if url in docs:
                row.update(docs[url])

            # 判定ロジック
            robots_status = row.get("robots_can_fetch", "").lower()
            tos_status = row.get("tos_can_scrape", "").lower()

            if robots_status == "blocked" or tos_status == "forbidden":
                row["scraping_allowed"] = "NO"
            else:
                row["scraping_allowed"] = "YES"

            writer.writerow(row)

    print(f"✅ 統合結果を書き出しました: {output_csv}")
    return output_csv


# CLI 実行用
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Merge robot_check and document_check results and judge scraping permission."
    )
    parser.add_argument("robot_csv", help="robot_checked の CSV ファイル")
    parser.add_argument("doc_csv", help="document_checked の CSV ファイル")
    parser.add_argument("output_csv", help="最終出力 CSV ファイル")

    args = parser.parse_args()

    merge_robot_and_doc(args.robot_csv, args.doc_csv, args.output_csv)
