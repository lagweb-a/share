from ddgs import DDGS
import csv
from typing import List

__all__ = ["collect_urls"]

def collect_urls(
    keywords: List[str],
    output_csv: str,
    max_results: int = 100,
    filters: List[str] = ["学割", "学生", "学生料金"],
) -> str:
    """
    DuckDuckGo検索を使って、学割関連URLを収集してCSVに保存する関数。

    Args:
        keywords: 検索キーワードのリスト
        output_csv: 出力するCSVファイルのパス
        max_results: 各キーワードで取得する最大件数
        filters: タイトル・スニペットに含まれるべきキーワード

    Returns:
        output_csv: 出力されたCSVファイルのパス
    """
    results = []

    with DDGS() as ddgs:
        for kw in keywords:
            for r in ddgs.text(kw, max_results=max_results):
                title = r.get("title", "")
                url = r.get("href", "")
                snippet = r.get("body", "")

                # タイトルまたはスニペットにフィルタワードが含まれる場合のみ追加
                if any(x in (title + snippet) for x in filters):
                    results.append([title, url, snippet])

    # CSVに保存
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "url", "snippet"])
        writer.writerows(results)

    print(f"{len(results)}件を保存しました ✅ -> {output_csv}")
    return output_csv


# スクリプト単体実行時は従来の挙動を残す
if __name__ == "__main__":
    keywords = [
        "学割 site:.jp",
        "学割 チケット 横浜 site:.jp",
        "学割 チケット 神奈川 site:.jp",
    ]
    collect_urls(keywords, "./csv/student_discount_ticket.csv")
