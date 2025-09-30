from flask import Flask, request, render_template, jsonify
import pandas as pd
from pathlib import Path
from geopy.geocoders import Nominatim
import time

app = Flask(__name__)
DATA_PATH = Path(__file__).parent / "data" / "data.csv"

geolocator = Nominatim(user_agent="disc_app_test")

def geocode_address(address):
    try:
        loc = geolocator.geocode(address)
        if loc:
            return loc.latitude, loc.longitude
    except Exception as e:
        print("geocode error:", e)
    return None, None

def load_spots():
    # CSVをDataFrameとして読み込み
    df = pd.read_csv(DATA_PATH, encoding="utf-8")

    # lat/lon列をfloatに変換（空ならNaNになる）
    for col in ("lat", "lon"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # lat/lonが無い行だけ住所から取得
    # for idx, row in df.iterrows():
    #     if pd.isna(row["lat"]) or pd.isna(row["lon"]):
    #         # 住所がある前提（nameやdescだけなら変換不可）
    #         address = f"{row.get('address','')}"
    #         if address:
    #             lat, lon = geocode_address(address)
    #             if lat and lon:
    #                 df.at[idx, "lat"] = lat
    #                 df.at[idx, "lon"] = lon
    #                 time.sleep(1)  # Nominatimレート制限対策

    # NaN を空文字に置き換えて dict のリストに変換
    spots = df.fillna("").to_dict(orient="records")
    return spots


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

if __name__ == "__main__":
    # 開発用：自動リロード
    app.run(host="127.0.0.1", port=5001, debug=True)
