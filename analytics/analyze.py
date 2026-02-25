"""
Simple analytics runner.
Computes basic summary stats from the existing sqlite DB and inserts a row into
`analytics_summaries` table used by the Flask admin view.

Run manually:
  python analytics\analyze.py

This script is intentionally simple and uses sqlite3 + pandas for convenience.
"""
from pathlib import Path
import sqlite3
import json
from datetime import datetime
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / 'reviews.db'
if not DB_PATH.exists():
    # try relative path (sqlite:///reviews.db used by app)
    DB_PATH = ROOT / 'reviews.db'

print('DB:', DB_PATH)
conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row

# Reviews
try:
    df_r = pd.read_sql_query('SELECT rating FROM review', conn)
    total_reviews = int(len(df_r))
    avg_review_rating = float(df_r['rating'].dropna().astype(float).mean()) if total_reviews else None
except Exception:
    total_reviews = 0
    avg_review_rating = None

# Member comments
try:
    df_c = pd.read_sql_query('SELECT target_id, rating FROM member_comments', conn)
    total_member_comments = int(len(df_c))
    avg_member_rating = float(df_c['rating'].dropna().astype(float).mean()) if total_member_comments else None
    top_targets = (
        df_c.groupby('target_id').size().reset_index(name='count')
        .sort_values('count', ascending=False)
        .head(5)
        .to_dict(orient='records')
    )
except Exception:
    total_member_comments = 0
    avg_member_rating = None
    top_targets = []

payload = {
    'created_at': datetime.utcnow().isoformat(),
    'total_reviews': total_reviews,
    'avg_review_rating': avg_review_rating,
    'total_member_comments': total_member_comments,
    'avg_member_rating': avg_member_rating,
    'top_targets': top_targets,
}

print('Summary:', json.dumps(payload, ensure_ascii=False, indent=2))

# insert into analytics_summaries
try:
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO analytics_summaries (created_at, total_reviews, avg_review_rating, total_member_comments, avg_member_rating, top_targets_json) VALUES (?, ?, ?, ?, ?, ?)',
        (
            payload['created_at'],
            payload['total_reviews'],
            payload['avg_review_rating'],
            payload['total_member_comments'],
            payload['avg_member_rating'],
            json.dumps(payload['top_targets'], ensure_ascii=False),
        ),
    )
    conn.commit()
    print('Inserted analytics summary (id=', cur.lastrowid, ')')
except Exception as e:
    print('Failed to insert analytics summary:', type(e).__name__, e)
finally:
    conn.close()
