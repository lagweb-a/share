from collections import defaultdict
from functools import wraps
import json
import math
import os, json
import firebase_admin
from firebase_admin import credentials, auth as fb_auth
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import firebase_admin
from firebase_admin import auth as fb_auth
from firebase_admin import credentials
import gspread
import pathlib
from google.oauth2.service_account import Credentials

app = Flask(__name__)
CORS(app, supports_credentials=True)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///reviews.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


# ---------------------------
# Firebase Admin init / verify (FIXED)
# ---------------------------
def init_firebase_admin():
    """
    1) 環境変数 FIREBASE_SERVICE_ACCOUNT_JSON にJSON文字列が入っていればそれを使う
    2) 無ければ app.py と同じ階層の serviceAccountKey.json を読み込む
    """
    cred_obj = None

    # 1) env var JSON（Render本番）
    sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        try:
            cred_obj = credentials.Certificate(json.loads(sa_json))
            print("[firebase] using credentials from env var")
        except Exception as e:
            raise RuntimeError(
                f"[firebase] FIREBASE_SERVICE_ACCOUNT_JSON is set but invalid: {e}"
            )

    # 2) local file（ローカル開発用）
    if cred_obj is None:
        key_path = pathlib.Path(__file__).with_name("serviceAccountKey.json")
        if not key_path.exists():
            raise RuntimeError(
                "FIREBASE_SERVICE_ACCOUNT_JSON is not set and "
                f"{key_path.name} not found next to app.py"
            )
        cred_obj = credentials.Certificate(str(key_path))
        print(f"[firebase] using credentials file: {key_path}")

    # Firebase 初期化（多重初期化防止）
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred_obj)
        # initialize app once
        try:
            firebase_admin.get_app()
        except ValueError:
            firebase_admin.initialize_app(cred_obj)


# ← アプリ起動時に一度だけ初期化
init_firebase_admin()


def verify_firebase_id_token():
    """Authorization: Bearer <ID_TOKEN> を検証。成功時は dict を返す。"""
    authz = request.headers.get("Authorization", "")
    if not authz.startswith("Bearer "):
        # ここは静かに None（仕様を変えない）
        return None

    id_token = authz.split(" ", 1)[1].strip()
    if not id_token:
        return None

    # JWTっぽいか簡易チェック（3セグメント）
    if id_token.count(".") != 2:
        print("[auth] token format invalid (not JWT-like).")
        return None

    try:
        decoded = fb_auth.verify_id_token(id_token)
        return decoded
    except Exception as e:
        # 失敗理由を握りつぶすと永遠に原因が分からないのでログだけ出す
        # トークン全文は出さない（漏洩防止）
        print("[auth] verify_id_token failed:", repr(e))
        return None


def login_required(fn):
    """Firebase IDトークンを要求するデコレータ。"""

    @wraps(fn)
    def wrapper(*args, **kwargs):
        user = verify_firebase_id_token()
        if not user:
            return jsonify({"error": "unauthorized"}), 401
        request.firebase_user = user
        return fn(*args, **kwargs)

    return wrapper


def current_user():
    """検証済みのFirebaseユーザー情報(dict)を返す。"""
    return getattr(request, "firebase_user", None) or verify_firebase_id_token() or {}


def json_no_store(payload, status=200):
    response = jsonify(payload)
    response.status_code = status
    response.headers["Cache-Control"] = "no-store"
    return response


def _uid_from_request():
    user = current_user()
    return user.get("uid") if user else None


def _trim_search_history(uid: str, keep: int = 20) -> None:
    obsolete = (
        SearchHistory.query.filter_by(uid=uid)
        .order_by(SearchHistory.created_at.desc())
        .offset(keep)
        .all()
    )
    for row in obsolete:
        db.session.delete(row)



@app.get("/api/public")
def public_api():
    return jsonify({"ok": True})


@app.get("/api/member-only")
@login_required
def member_only():
    user = current_user()
    payload = {
        "message": "member ok",
        "uid": user.get("uid"),
        "email": user.get("email"),
    }
    return json_no_store(payload)


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
BOUNDARY_CACHE: Dict[str, Optional[Dict]] = {}

TAG_KEYWORDS = {
    "レストラン": ["レストラン", "食堂", "ダイニング", "料理店"],
    "居酒屋": ["居酒屋", "酒場", "立ち飲み", "バル"],
    "カフェ": ["カフェ", "喫茶", "coffee"],
    "ラーメン": ["ラーメン", "らーめん", "中華そば"],
    "みなとみらい": ["みなとみらい", "桜木町", "赤レンガ", "ランドマークタワー"],
    "カラオケ": ["カラオケ", "ビッグエコー", "まねきねこ"],
    "ボウリング": ["ボウリング"],
    "ダーツ": ["ダーツ"],
    "美術館・博物館": ["美術館", "博物館", "ミュージアム","アート"]
}

BUDGET_RANGE_RE = re.compile(r"(\d{3,5})\s*(?:円)?\s*[~〜～\-−ー]\s*(\d{3,5})\s*円?")


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


def parse_tag_text(raw_tags: str) -> List[str]:
    if not isinstance(raw_tags, str):
        return []
    values = re.split(r"[|,、\s]+", raw_tags)
    cleaned = [v.strip() for v in values if v and v.strip()]
    return list(dict.fromkeys(cleaned))


def extract_budget_tags(text: str) -> List[str]:
    if not isinstance(text, str) or not text:
        return []
    tags: List[str] = []
    for low, high in BUDGET_RANGE_RE.findall(text):
        tag = f"{int(low)}円~{int(high)}円"
        tags.append(tag)
    return list(dict.fromkeys(tags))


def infer_spot_tags(row: Dict) -> List[str]:
    name = str(row.get("name") or "")
    desc = str(row.get("description") or "")
    address = str(row.get("address") or "")
    price = str(row.get("price") or "")
    pref = str(row.get("prefecture") or "")
    city = str(row.get("city") or "")

    text_blob = "\n".join([name, desc, address, price]).lower()
    tags = parse_tag_text(str(row.get("tags") or ""))

    for tag_name, keywords in TAG_KEYWORDS.items():
        if any(keyword.lower() in text_blob for keyword in keywords):
            tags.append(tag_name)

    tags.extend(extract_budget_tags(price))

    if pref:
        tags.append(pref)
    if city:
        tags.append(city)

    return list(dict.fromkeys([t for t in tags if t]))


def tokenize_query(query: str) -> List[str]:
    cleaned = re.sub(r"[|,、]+", " ", (query or "").strip().lower())
    return [t for t in cleaned.split() if t]


def spot_searchable_text(spot: Dict) -> str:
    fields = [
        spot.get("name", ""),
        spot.get("description", ""),
        spot.get("address", ""),
        spot.get("price", ""),
        spot.get("prefecture", ""),
        spot.get("city", ""),
        spot.get("region", ""),
        spot.get("tags", ""),
    ]
    return "\n".join(str(v) for v in fields if v).lower()


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
    for spot in spots:
        merged_tags = infer_spot_tags(spot)
        spot["tags"] = "|".join(merged_tags)
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


@app.route("/signup")
def signup_page():
    return render_template("signup.html")


@app.route("/api/spots")
def api_spots():
    q = (request.args.get("q") or "").strip()
    query_tokens = tokenize_query(q)

    data = load_spots()

    for s in data:
        s["student_text"] = (s.get("student_text") or "").strip()

    # フィルタ処理 (name/desc/tagsに含まれるか)
    def filtering(s):
        if not query_tokens:
            return True
        haystack = spot_searchable_text(s)
        return all(token in haystack for token in query_tokens)

    return jsonify([s for s in data if filtering(s)])


@app.route("/api/geo")
def api_geo():
    spots = load_spots()
    tree = build_geo_tree(spots)
    return jsonify(tree)


def _fetch_boundary_geojson(query: str) -> Optional[Dict]:
    cached = BOUNDARY_CACHE.get(query)
    if cached is not None:
        return cached

    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": query,
                "format": "jsonv2",
                "polygon_geojson": 1,
                "addressdetails": 1,
                "countrycodes": "jp",
                "limit": 1,
            },
            headers={"User-Agent": "map-filter-app/1.0"},
            timeout=10,
        )
        response.raise_for_status()
        items = response.json()
    except requests.RequestException:
        BOUNDARY_CACHE[query] = None
        return None

    geojson = None
    if items:
        candidate = items[0].get("geojson")
        if isinstance(candidate, dict) and candidate.get("type") in {
            "Polygon",
            "MultiPolygon",
        }:
            geojson = candidate

    BOUNDARY_CACHE[query] = geojson
    return geojson


@app.get("/api/geo-boundary")
def api_geo_boundary():
    pref = (request.args.get("pref") or "").strip()
    city = (request.args.get("city") or "").strip()
    if not pref:
        return json_no_store({"error": "pref is required"}, 400)

    query = f"{city}, {pref}, Japan" if city else f"{pref}, Japan"
    geojson = _fetch_boundary_geojson(query)
    if not geojson:
        return json_no_store({"geojson": None, "query": query})
    return json_no_store({"geojson": geojson, "query": query})


@app.post("/api/favorites/add")
@login_required
def favorites_add():
    uid = _uid_from_request()
    if not uid:
        return json_no_store({"error": "invalid-token"}, 401)
    data = request.get_json(silent=True) or {}
    item_id = str(data.get("item_id") or "").strip()
    if not item_id:
        return json_no_store({"error": "item_id required"}, 400)

    favorite = Favorite.query.filter_by(uid=uid, item_id=item_id).first()
    if favorite:
        return json_no_store({"ok": True, "id": favorite.id})

    favorite = Favorite(uid=uid, item_id=item_id)
    db.session.add(favorite)
    db.session.commit()
    return json_no_store({"ok": True, "id": favorite.id}, 201)


@app.get("/api/favorites/list")
@login_required
def favorites_list():
    uid = _uid_from_request()
    if not uid:
        return json_no_store({"error": "invalid-token"}, 401)
    favorites = (
        Favorite.query.filter_by(uid=uid)
        .order_by(Favorite.created_at.desc())
        .all()
    )
    items = [
        {
            "item_id": fav.item_id,
            "created_at": fav.created_at.isoformat(),
        }
        for fav in favorites
    ]
    return json_no_store({"items": items})


@app.delete("/api/favorites/remove")
@login_required
def favorites_remove():
    uid = _uid_from_request()
    if not uid:
        return json_no_store({"error": "invalid-token"}, 401)
    data = request.get_json(silent=True) or {}
    item_id = str(data.get("item_id") or "").strip()
    if not item_id:
        return json_no_store({"error": "item_id required"}, 400)

    favorite = Favorite.query.filter_by(uid=uid, item_id=item_id).first()
    if favorite:
        db.session.delete(favorite)
        db.session.commit()
    return json_no_store({"ok": True})


@app.post("/api/comments")
@login_required
def post_member_comment():
    uid = _uid_from_request()
    if not uid:
        return json_no_store({"error": "invalid-token"}, 401)
    data = request.get_json(force=True) or {}
    # coerce to str because clients may send numeric IDs; avoid AttributeError on .strip()
    target_id = str(data.get("target_id") or "").strip()
    target_name = (str(data.get("target_name") or "").strip() or None)
    author = (str(data.get("author") or "").strip() or None)
    body = str(data.get("body") or "").strip()
    rating = int(data.get("rating") or 0)
    if not target_id or not body:
        return json_no_store({"error": "target_id and body required"}, 400)
    if rating < 1 or rating > 5:
        return json_no_store({"error": "rating must be between 1 and 5"}, 400)

    comment = MemberComment(
        uid=uid,
        target_id=target_id,
        target_name=target_name,
        author=author,
        body=body,
        rating=rating,
    )
    db.session.add(comment)
    db.session.commit()
    payload = {
        "id": comment.id,
        "target_id": comment.target_id,
        "target_name": comment.target_name,
        "author": comment.author,
        "body": comment.body,
        "rating": comment.rating,
        "created_at": comment.created_at.isoformat(),
    }
    # 2. スプレッドシートへ追記（失敗しても処理を止めない）
    try:
        print("Google Sheets: append member comment")
        worksheet.append_row([
            str(comment.id),
            str(comment.target_id),
            str(comment.target_name or ""),
            str(comment.uid or ""),
            str(comment.author or ""),
            str(comment.body or ""),
            str(comment.rating),
            str(comment.created_at.isoformat()),
        ])
    except Exception as e:
        # ログに型名とメッセージを出す（詳細な認証情報は出さない）
        print("Google Sheets保存エラー (member comment):", type(e).__name__, str(e))

    return json_no_store(payload, 201)


@app.get("/api/comments")
@login_required
def get_member_comments():
    uid = _uid_from_request()
    if not uid:
        return json_no_store({"error": "invalid-token"}, 401)
    target_id = (request.args.get("target_id") or "").strip()
    query = MemberComment.query.filter_by(uid=uid)
    if target_id:
        query = query.filter_by(target_id=target_id)
    comments = query.order_by(MemberComment.created_at.desc()).all()
    items = [
        {
            "id": c.id,
            "target_id": c.target_id,
            "target_name": c.target_name,
            "author": c.author,
            "body": c.body,
            "rating": c.rating,
            "created_at": c.created_at.isoformat(),
        }
        for c in comments
    ]
    return json_no_store({"comments": items})


@app.post("/api/search-history")
@login_required
def save_search_query():
    uid = _uid_from_request()
    if not uid:
        return json_no_store({"error": "invalid-token"}, 401)
    data = request.get_json(silent=True) or {}
    query_text = str(data.get("query") or "").strip()
    if not query_text:
        return json_no_store({"error": "query required"}, 400)

    existing = SearchHistory.query.filter_by(uid=uid, query_text=query_text).first()
    now = datetime.utcnow()
    if existing:
        existing.created_at = now
    else:
        db.session.add(SearchHistory(uid=uid, query_text=query_text, created_at=now))
    db.session.flush()
    _trim_search_history(uid)
    db.session.commit()
    return json_no_store({"ok": True})



@app.get("/api/search-history")
@login_required
def list_queries():
    uid = _uid_from_request()
    if not uid:
        return json_no_store({"error": "invalid-token"}, 401)
    records = (
        SearchHistory.query.filter_by(uid=uid)
        .order_by(SearchHistory.created_at.desc())
        .limit(20)
        .all()
    )
    items = [
        {"query": r.query_text, "created_at": r.created_at.isoformat()}
        for r in records
    ]
    return json_no_store({"queries": items})



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
    if "author" in data:  r.author  = str(data["author"]).strip()
    if "comment" in data: r.comment = str(data["comment"]).strip()
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


class Favorite(db.Model):
    __tablename__ = "favorites"

    id = db.Column(db.Integer, primary_key=True)
    uid = db.Column(db.String(128), nullable=False, index=True)
    item_id = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        db.UniqueConstraint("uid", "item_id", name="uq_favorites_uid_item"),
    )


class MemberComment(db.Model):
    __tablename__ = "member_comments"

    id = db.Column(db.Integer, primary_key=True)
    uid = db.Column(db.String(128), nullable=False, index=True)
    target_id = db.Column(db.String(255), nullable=False, index=True)
    target_name = db.Column(db.String(255), nullable=True)
    author = db.Column(db.String(80), nullable=True)
    body = db.Column(db.Text, nullable=False)
    rating = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


class SearchHistory(db.Model):
    __tablename__ = "search_history"

    id = db.Column(db.Integer, primary_key=True)
    uid = db.Column(db.String(128), nullable=False, index=True)
    query_text = db.Column(db.String(255), nullable=False)  # ← query をやめる
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)


with app.app_context():
    # データベースマイグレーション: 既存テーブルにカラムを追加
    from sqlalchemy import text
    try:
        db.session.execute(text("ALTER TABLE search_history ADD COLUMN query_text VARCHAR(255) NOT NULL DEFAULT ''"))
        db.session.commit()
        print("Added query_text column to search_history")
    except Exception as e:
        print(f"Column already exists or error: {e}")
    db.create_all()


# Google Sheets認証
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1ADyw0jV2GETPE6Lr5eam1LayuScfz2AA-OopGoWQ6oo'

sa_json = os.environ.get("GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON")

if sa_json and sa_json.strip():
    creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
else:
    # ローカル開発用（ファイル）
    key_path = pathlib.Path(__file__).with_name("sheet-api-470504-a2538cff344f.json")
    if not key_path.exists():
        raise RuntimeError("GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON is not set and sheet-api-*.json not found next to app.py")
    creds = Credentials.from_service_account_file(str(key_path), scopes=SCOPES)

gc = gspread.authorize(creds)
worksheet = gc.open_by_key(SPREADSHEET_ID).sheet1

if __name__ == "__main__":
    # 開発用：自動リロード
    app.run(host="127.0.0.1", port=5001, debug=True)
