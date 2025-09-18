import csv
import urllib.robotparser
from urllib.parse import urlparse

# CSVファイルを読み込む
csv_file = "student_discount_sites_1.csv"

# User-agent を指定
user_agent = "*"

with open(csv_file, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        site_url = row["url"]  # CSVの "url" 列を取得
        parsed = urlparse(site_url)
        
        # robots.txt の URL を作成
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        
        # robots.txt を読み込む
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
        except:
            print(f"{site_url} の robots.txt を取得できませんでした ❌")
            continue
        
        # アクセス可能かチェック
        if rp.can_fetch(user_agent, site_url):
            print(f"{site_url} はスクレイピング可能です ✅")
        else:
            print(f"{site_url} はスクレイピング禁止です ❌")
