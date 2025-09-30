# -*- coding: utf-8 -*-
"""
Convert scraped CSV -> catalog CSV

入力 (scraped CSV):
  必須カラム: url, title, address
  例: url,final_url,source_page,title,address,discount_text,...

出力 (catalog CSV):
  id,name,url,address,lat,lon,tags,description,image_url,price

仕様:
- name <- title
- url  <- url
- address <- address
- それ以外(lat/lon/tags/description/image_url/price)は空欄を出力
- id は連番。--id-prefix / --start-id / --zero-pad で整形可能
"""

import csv
import sys
from pathlib import Path
import argparse

def ensure_parent(path_str: str) -> str:
    p = Path(path_str)
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)

def convert_scraped_to_catalog(
    input_csv: str,
    output_csv: str,
    id_prefix: str = "",
    start_id: int = 1,
    zero_pad: int = 0,
) -> str:
    """
    入力: scraped の結果CSV（ヘッダに url,title,address を含む）
    出力: id,name,url,address,lat,lon,tags,description,image_url,price のCSV
    """
    # 出力カラム（順序厳守）
    out_fields = [
        "id", "name", "url", "address",
        "lat", "lon", "tags", "description", "image_url", "price"
    ]

    def make_id(n: int) -> str:
        s = str(n)
        if zero_pad and zero_pad > 0:
            s = s.zfill(zero_pad)
        return f"{id_prefix}{s}"

    input_path = Path(input_csv)
    if not input_path.exists():
        raise FileNotFoundError(f"Input not found: {input_csv}")

    ensure_parent(output_csv)

    # 読み込み（BOM対策で utf-8-sig を試し、失敗したら utf-8）
    def open_reader(path):
        try:
            f = open(path, newline="", encoding="utf-8-sig")
            return f, csv.DictReader(f)
        except Exception:
            f = open(path, newline="", encoding="utf-8")
            return f, csv.DictReader(f)

    fin, reader = open_reader(input_csv)
    try:
        # 必須カラムチェック
        required = {"url", "title", "address"}
        if reader.fieldnames is None:
            raise ValueError("入力CSVのヘッダが読めませんでした。ファイル/エンコーディングをご確認ください。")
        missing = [c for c in required if c not in reader.fieldnames]
        if missing:
            raise ValueError(f"入力CSVに必要なカラムがありません: {missing}. 既存カラム: {reader.fieldnames}")

        with open(output_csv, "w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=out_fields)
            writer.writeheader()

            count = 0
            for i, row in enumerate(reader, start=start_id):
                src_url = (row.get("url") or "").strip()
                src_title = (row.get("title") or "").strip()
                src_addr = (row.get("address") or "").strip()

                # url/title/address が全て空ならスキップ
                if not (src_url or src_title or src_addr):
                    continue

                out_row = {
                    "id": make_id(i),
                    "name": src_title,
                    "url": src_url,
                    "address": src_addr,
                    "lat": "",
                    "lon": "",
                    "tags": "",
                    "description": "",
                    "image_url": "",
                    "price": "",
                }
                writer.writerow(out_row)
                count += 1

    finally:
        try:
            fin.close()
        except Exception:
            pass

    print(f"✅ Converted {count} rows → {output_csv}")
    return output_csv


def main():
    ap = argparse.ArgumentParser(description="Convert scraped CSV to catalog CSV.")
    ap.add_argument("input", help="scraped の結果CSVパス（url,title,address を含む）")
    ap.add_argument("--out", default="./converted_csv/catalog.csv", help="出力CSVのパス")
    ap.add_argument("--id-prefix", default="", help="id の接頭辞（例: item_）")
    ap.add_argument("--start-id", type=int, default=1, help="id の開始番号（例: 1）")
    ap.add_argument("--zero-pad", type=int, default=0, help="id の数値部分のゼロ埋め桁（例: 6 -> 000001）")

    args = ap.parse_args()

    try:
        convert_scraped_to_catalog(
            input_csv=args.input,
            output_csv=args.out,
            id_prefix=args.id_prefix,
            start_id=args.start_id,
            zero_pad=args.zero_pad,
        )
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
