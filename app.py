from collections import defaultdict
import math
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from flask import Flask, request, render_template, jsonify

from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///reviews.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
DATA_PATH = Path(__file__).parent / "data" / "data.csv"

# 地方と都道府県のメタデータ（代表座標は県庁所在地付近）
PREF_REGION_INFO = {
    "北海道": {"region": "北海道", "center": [43.06417, 141.34694], "zoom": 7, "radius": 280_000},
    "青森県": {"region": "東北", "center": [40.82444, 140.74], "zoom": 8, "radius": 120_000},
    "岩手県": {"region": "東北", "center": [39.70361, 141.1525], "zoom": 8, "radius": 130_000},
    "宮城県": {"region": "東北", "center": [38.26889, 140.87194], "zoom": 8, "radius": 110_000},
    "秋田県": {"region": "東北", "center": [39.71861, 140.1025], "zoom": 8, "radius": 120_000},
    "山形県": {"region": "東北", "center": [38.24056, 140.36333], "zoom": 8, "radius": 110_000},
    "福島県": {"region": "東北", "center": [37.75, 140.46778], "zoom": 8, "radius": 140_000},
    "茨城県": {"region": "関東", "center": [36.34139, 140.44667], "zoom": 8, "radius": 110_000},
    "栃木県": {"region": "関東", "center": [36.56583, 139.88361], "zoom": 8, "radius": 110_000},
    "群馬県": {"region": "関東", "center": [36.39111, 139.06083], "zoom": 8, "radius": 120_000},
    "埼玉県": {"region": "関東", "center": [35.85694, 139.64889], "zoom": 9, "radius": 90_000},
    "千葉県": {"region": "関東", "center": [35.60472, 140.12333], "zoom": 8, "radius": 110_000},
    "東京都": {"region": "関東", "center": [35.68944, 139.69167], "zoom": 9, "radius": 90_000},
    "神奈川県": {"region": "関東", "center": [35.44778, 139.6425], "zoom": 9, "radius": 90_000},
    "新潟県": {"region": "中部", "center": [37.90222, 139.02361], "zoom": 8, "radius": 150_000},
    "富山県": {"region": "中部", "center": [36.69528, 137.21139], "zoom": 8, "radius": 110_000},
    "石川県": {"region": "中部", "center": [36.59444, 136.62556], "zoom": 8, "radius": 110_000},
    "福井県": {"region": "中部", "center": [36.06411, 136.21944], "zoom": 8, "radius": 110_000},
    "山梨県": {"region": "中部", "center": [35.66389, 138.56833], "zoom": 9, "radius": 90_000},
    "長野県": {"region": "中部", "center": [36.65139, 138.18111], "zoom": 8, "radius": 130_000},
    "岐阜県": {"region": "中部", "center": [35.39111, 136.72222], "zoom": 8, "radius": 120_000},
    "静岡県": {"region": "中部", "center": [34.97694, 138.38306], "zoom": 8, "radius": 130_000},
    "愛知県": {"region": "中部", "center": [35.18028, 136.90667], "zoom": 8, "radius": 110_000},
    "三重県": {"region": "近畿", "center": [34.73028, 136.50861], "zoom": 8, "radius": 120_000},
    "滋賀県": {"region": "近畿", "center": [35.00444, 135.86833], "zoom": 9, "radius": 90_000},
    "京都府": {"region": "近畿", "center": [35.01167, 135.76833], "zoom": 9, "radius": 90_000},
    "大阪府": {"region": "近畿", "center": [34.68639, 135.52], "zoom": 9, "radius": 90_000},
    "兵庫県": {"region": "近畿", "center": [34.69139, 135.18306], "zoom": 8, "radius": 120_000},
    "奈良県": {"region": "近畿", "center": [34.68528, 135.83278], "zoom": 9, "radius": 90_000},
    "和歌山県": {"region": "近畿", "center": [34.22611, 135.1675], "zoom": 8, "radius": 120_000},
    "鳥取県": {"region": "中国", "center": [35.50361, 134.23833], "zoom": 8, "radius": 110_000},
    "島根県": {"region": "中国", "center": [35.47222, 133.05056], "zoom": 8, "radius": 130_000},
    "岡山県": {"region": "中国", "center": [34.66167, 133.935], "zoom": 8, "radius": 110_000},
    "広島県": {"region": "中国", "center": [34.39639, 132.45944], "zoom": 8, "radius": 120_000},
    "山口県": {"region": "中国", "center": [34.18583, 131.47139], "zoom": 8, "radius": 130_000},
    "徳島県": {"region": "四国", "center": [34.06583, 134.55944], "zoom": 9, "radius": 90_000},
    "香川県": {"region": "四国", "center": [34.34028, 134.04333], "zoom": 9, "radius": 90_000},
    "愛媛県": {"region": "四国", "center": [33.84167, 132.76611], "zoom": 8, "radius": 110_000},
    "高知県": {"region": "四国", "center": [33.55889, 133.53111], "zoom": 8, "radius": 120_000},
    "福岡県": {"region": "九州・沖縄", "center": [33.60639, 130.41806], "zoom": 8, "radius": 120_000},
    "佐賀県": {"region": "九州・沖縄", "center": [33.24944, 130.29889], "zoom": 9, "radius": 90_000},
    "長崎県": {"region": "九州・沖縄", "center": [32.74472, 129.87361], "zoom": 8, "radius": 130_000},
    "熊本県": {"region": "九州・沖縄", "center": [32.78972, 130.74167], "zoom": 8, "radius": 120_000},
    "大分県": {"region": "九州・沖縄", "center": [33.23806, 131.6125], "zoom": 8, "radius": 110_000},
    "宮崎県": {"region": "九州・沖縄", "center": [31.90778, 131.42028], "zoom": 8, "radius": 130_000},
    "鹿児島県": {"region": "九州・沖縄", "center": [31.56028, 130.55806], "zoom": 8, "radius": 140_000},
    "沖縄県": {"region": "九州・沖縄", "center": [26.2125, 127.68111], "zoom": 8, "radius": 120_000},
}

REGION_PRESET = {
    "北海道": {"zoom": 6, "radius": 600_000},
    "東北": {"zoom": 6, "radius": 360_000},
    "関東": {"zoom": 6, "radius": 260_000},
    "中部": {"zoom": 6, "radius": 360_000},
    "近畿": {"zoom": 6, "radius": 260_000},
    "中国": {"zoom": 6, "radius": 280_000},
    "四国": {"zoom": 6, "radius": 200_000},
    "九州・沖縄": {"zoom": 6, "radius": 360_000},
}

PREFECTURE_NAMES = list(PREF_REGION_INFO.keys())


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """2地点間の距離（km）を計算"""
    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    return 6371.0 * c


def extract_pref_city(address: str) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(address, str):
        return None, None
    cleaned = re.sub(r"〒\s*\d{3}-?\d{4}", "", address)
    cleaned = cleaned.replace("　", " ").strip()
    pref = next((p for p in PREFECTURE_NAMES if p in cleaned), None)
    if not pref:
        return None, None
    rest = cleaned.split(pref, 1)[1].strip()
    pattern = re.compile(r"([\w一-龠ぁ-んァ-ヶー]+?(?:市|区|町|村|郡\s*[\w一-龠ぁ-んァ-ヶー]+?(?:町|村)?))")
    match = pattern.search(rest)
    city = match.group(1).replace(" ", "") if match else None
    return pref, city


def compute_bbox(coords: List[Tuple[float, float]]):
    if not coords:
        return None
    lats = [lat for lat, lon in coords]
    lons = [lon for lat, lon in coords]
    return [min(lons), min(lats), max(lons), max(lats)]

def compute_center_radius(
    coords: List[Tuple[float, float]], fallback_center: List[float]
):
    valid = [(lat, lon) for lat, lon in coords if not pd.isna(lat) and not pd.isna(lon)]
    if not valid:
        return fallback_center, None, None
    avg_lat = sum(lat for lat, _ in valid) / len(valid)
    avg_lon = sum(lon for _, lon in valid) / len(valid)
    center = [avg_lat, avg_lon]
    max_distance_km = max(
        haversine_km(avg_lat, avg_lon, lat, lon) for lat, lon in valid
    )
    radius_m = int((max_distance_km + 5) * 1000)
    bbox = compute_bbox(valid)
    return center, radius_m, bbox

def load_spots():
    # CSVをDataFrameとして読み込み
    df = pd.read_csv(DATA_PATH, encoding="utf-8")

    # lat/lon列をfloatに変換（空ならNaNになる）
    for col in ("lat", "lon"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    prefs = []
    cities = []
    regions = []
    for addr in df.get("address", ""):
        pref, city = extract_pref_city(addr)
        prefs.append(pref or "")
        cities.append(city or "")
        region = PREF_REGION_INFO.get(pref or "", {}).get("region", "")
        regions.append(region)
    df["prefecture"] = prefs
    df["city"] = cities
    df["region"] = regions

    # NaN を空文字に置き換えて dict のリストに変換
    spots = df.fillna("").to_dict(orient="records")
    return spots


def build_geo_tree(spots: List[Dict]) -> Dict[str, Dict]:
    tree: Dict[str, Dict] = {}
    coords_by_pref: defaultdict[str, List[Tuple[float, float]]] = defaultdict(list)
    coords_by_city: defaultdict[Tuple[str, str], List[Tuple[float, float]]] = defaultdict(list)

    for spot in spots:
        pref = spot.get("prefecture") or ""
        city = spot.get("city") or ""
        lat = spot.get("lat")
        lon = spot.get("lon")
        try:
            lat_val = float(lat)
            lon_val = float(lon)
        except (TypeError, ValueError):
            continue
        if pref:
            coords_by_pref[pref].append((lat_val, lon_val))
            if city:
                coords_by_city[(pref, city)].append((lat_val, lon_val))

    region_accumulator: defaultdict[str, List[List[float]]] = defaultdict(list)
    for pref_name, info in PREF_REGION_INFO.items():
        region_key = info["region"]
        pref_center = info["center"]
        region_entry = tree.setdefault(
            region_key,
            {
                "center": pref_center[:],
                "zoom": REGION_PRESET.get(region_key, {}).get("zoom", 6),
                "radius": REGION_PRESET.get(region_key, {}).get("radius", 300_000),
                "prefs": {},
            },
        )
        region_accumulator[region_key].append(pref_center)

        pref_coords = coords_by_pref.get(pref_name, [])
        pref_center_calc, pref_radius, pref_bbox = compute_center_radius(
            pref_coords, pref_center
        )
        pref_entry = {
            "center": pref_center_calc,
            "zoom": PREF_REGION_INFO[pref_name].get("zoom", 8),
            "radius": pref_radius or PREF_REGION_INFO[pref_name].get("radius", 100_000),
            "bbox": pref_bbox,
            "cities": {},
        }

        region_entry["prefs"][pref_name] = pref_entry

    for region_key, centers in region_accumulator.items():
        if not centers:
            continue
        avg_lat = sum(c[0] for c in centers) / len(centers)
        avg_lon = sum(c[1] for c in centers) / len(centers)
        tree[region_key]["center"] = [avg_lat, avg_lon]

    for (pref_name, city_name), coords in coords_by_city.items():
        pref_entry = tree.get(PREF_REGION_INFO.get(pref_name, {}).get("region", ""), {}).get("prefs", {}).get(pref_name)
        if not pref_entry:
            continue
        center, radius, bbox = compute_center_radius(coords, pref_entry["center"])
        pref_entry["cities"][city_name] = {
            "center": center,
            "zoom": max(pref_entry.get("zoom", 10), 11),
            "radius": radius or max(20_000, pref_entry.get("radius", 80_000) // 3),
            "bbox": bbox,
        }

    return tree


@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/spots")
def api_spots():
    q = (request.args.get("q") or "").strip().lower()

    data = load_spots()

    # フィルタ処理（name/desc/tagsに含まれるか）
    def filtering(s):
        return (
            not q
            or q in str(s["name"]).lower()
            or q in str(s["desc"]).lower()
            or q in str(s["tags"]).lower()
        )

    return jsonify([s for s in data if filtering(s)])


@app.route("/api/geo")
def api_geo():
    spots = load_spots()
    tree = build_geo_tree(spots)
    return jsonify(tree)

# ---- API ----
@app.route("/api/reviews", methods=["POST"])
def post_review():
    data = request.get_json(force=True) or {}
    place_id = data.get("place_id", "")
    place_name = data.get("place_name", "")
    author = data.get("author", "名無しさん")
    comment = data.get("comment", "")
    rating = int(data.get("rating", 0))
    created_at = datetime.utcnow()
    print("たのむよー")
    # 1. DB保存
    r = Review(
        author=author,
        comment=comment,
        rating=rating,
        created_at=created_at
    )
    db.session.add(r)
    db.session.commit()

    # 2. スプレッドシート保存（場所ID・場所名も追加）
    try:
        print("Google Sheets保存test")
        worksheet.append_row([
            str(place_id), str(place_name), str(created_at.isoformat()), str(author), str(comment), str(rating)
        ])
    except Exception as e:
        print("Google Sheets保存エラー:", e)

    return jsonify({"success": True, "id": r.id}), 201

@app.route("/api/reviews", methods=["GET"])
def get_reviews():
    reviews = Review.query.order_by(Review.created_at.desc()).all()
    return jsonify([
        {
            "id": r.id,
            "author": r.author,
            "comment": r.comment,
            "rating": r.rating,
            "created_at": r.created_at.isoformat()
        }
        for r in reviews
    ])

@app.route("/api/reviews/<int:rid>", methods=["PUT"])
def update_review(rid):
    r = Review.query.get_or_404(rid)
    data = request.get_json(force=True) or {}
    if "author" in data:  r.author  = data["author"].strip()
    if "comment" in data: r.comment = data["comment"].strip()
    if "rating" in data:  r.rating  = int(data["rating"])
    db.session.commit()
    return jsonify({"success": True})

@app.route("/api/reviews/<int:rid>", methods=["DELETE"])
def delete_review(rid):
    r = Review.query.get_or_404(rid)
    db.session.delete(r)
    db.session.commit()
    return jsonify({"success": True})

# Reviewモデル
class Review(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    author = db.Column(db.String(80), nullable=False)
    comment = db.Column(db.Text, nullable=False)
    rating = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

with app.app_context():
    db.create_all()

# Google Sheets認証
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDS_FILE = 'sheet-api-470504-a2538cff344f.json'  # 認証ファイル名
SPREADSHEET_ID = '1ADyw0jV2GETPE6Lr5eam1LayuScfz2AA-OopGoWQ6oo'  # ←必ず書き換え

creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
worksheet = sh.sheet1

if __name__ == "__main__":
    # 開発用：自動リロード
    app.run(host="127.0.0.1", port=5001, debug=True)
