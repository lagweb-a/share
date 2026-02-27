"""Simple helper to fetch event CSV and run basic pandas analysis.

Usage:
    python analysis.py [URL] [TOKEN]

If you already have events.csv you can just import pandas and open it.
"""

import sys
import requests
import pandas as pd


def download_events(url, token, outpath="events.csv"):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    r = requests.get(url, headers=headers, stream=True)
    r.raise_for_status()
    with open(outpath, "wb") as f:
        for chunk in r.iter_content(1024 * 8):
            if chunk:
                f.write(chunk)
    print(f"wrote {outpath}")
    return outpath


def analyze(path="events.csv"):
    df = pd.read_csv(path, parse_dates=["created_at"])
    print(df.head())
    print("\n各イベントタイプ集計")
    print(df["event_type"].value_counts())
    print("\n人気ターゲットランキング")
    print(df.groupby("target_id").size().sort_values(ascending=False).head(20))
    return df


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        url = sys.argv[1]
    else:
        url = "http://localhost:5001/admin/export-events"
    token = sys.argv[2] if len(sys.argv) >= 3 else None
    csv = download_events(url, token)
    analyze(csv)
