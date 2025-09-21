# -*- coding: utf-8 -*-
"""
スクレイピング可否チェックパイプライン
- 入力: keywords, basename (例: "tickets")
- 出力: ./final_checked/{basename}_final.csv
"""

import os
from get_url import collect_urls
from check_robot import append_robots_info
from check_document import append_tos_info
from scraping_permission import merge_robot_and_doc  # ← 追加


def run_pipeline(keywords, basename="tickets", max_results=100):
    """
    keywords: list[str] - 検索ワード
    basename: str - ファイルのベース名 (拡張子やディレクトリは自動で付与)
    max_results: int - 各キーワードから取得する件数
    """

    # 1. URL収集
    input_csv = f"./csv/{basename}.csv"
    print(f"🔍 Step1: URL収集 → {input_csv}")
    url_csv = collect_urls(keywords, input_csv, max_results=max_results)
    print(f"✅ URL収集完了: {url_csv}")

    # 2. robots.txt チェック
    robots_csv = f"./robot_checked/{basename}_with_robots.csv"
    print(f"🤖 Step2: robots.txt チェック → {robots_csv}")
    robots_out = append_robots_info(url_csv, robots_csv)
    print(f"✅ robots判定完了: {robots_out}")

    # 3. 利用規約チェック
    tos_csv = f"./document_checked/{basename}_with_tos.csv"
    print(f"📑 Step3: 利用規約チェック → {tos_csv}")
    tos_out = append_tos_info(robots_out, tos_csv)
    print(f"✅ 利用規約判定完了: {tos_out}")

    # 4. robots & ToS 統合してスクレイピング可否判定
    final_csv = f"./final_checked/{basename}_final.csv"
    print(f"🔒 Step4: スクレイピング可否判定 → {final_csv}")
    allowed_out = merge_robot_and_doc(robots_out, tos_out, final_csv)
    print(f"✅ スクレイピング可否判定完了: {allowed_out}")

    return allowed_out


if __name__ == "__main__":
    # ======= ここだけ実行前に編集 =======
    keywords = [
        "学割 site:.jp",
        "学割 チケット 横浜 site:.jp",
        "学割 チケット 神奈川 site:.jp",
    ]
    basename = "tickets"   # 出力の基礎ファイル名
    # ===================================

    final_csv = run_pipeline(keywords, basename)
    print("🎉 パイプライン完了: 出力ファイル =", final_csv)
