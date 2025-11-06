import json
import os
from datetime import datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1._helpers import DatetimeWithNanoseconds

SERVICE_ACCOUNT_PATH = "firebase-service-account.json"
OUTPUT_JSON_PATH = "recipes_export.json"
COLLECTION_NAME = "recipes"


def initialize_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def convert_for_json(value):
    """Firestore→JSON変換用"""
    if isinstance(value, DatetimeWithNanoseconds):
        return value.isoformat()
    if isinstance(value, list):
        return [convert_for_json(v) for v in value]
    if isinstance(value, dict):
        return {k: convert_for_json(v) for k, v in value.items()}
    return value


def parse_datetime(value: str):
    """ISO8601文字列→datetime（タイムゾーン対応）"""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def load_existing_recipes():
    """既存のJSONを読み込む（なければ空リスト）"""
    if not os.path.exists(OUTPUT_JSON_PATH):
        return []
    with open(OUTPUT_JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_latest_updated_at(recipes):
    """既存データの中で最新updatedAtを探す"""
    dates = [parse_datetime(r.get("updatedAt")) for r in recipes if r.get("updatedAt")]
    dates = [d for d in dates if d is not None]
    return max(dates) if dates else None


def fetch_recipes_updated_after(db, after_datetime):
    """updatedAt > after_datetime のデータをFirestoreから取得"""
    if after_datetime is None:
        print("ℹ️ 既存データがないため全件取得します。")
        query = db.collection(COLLECTION_NAME)
    else:
        print(f"🔍 Firestoreから {after_datetime.isoformat()} 以降の更新を取得中...")
        query = db.collection(COLLECTION_NAME).where(
            filter=firestore.FieldFilter("updatedAt", ">", after_datetime)
        )

    docs = query.stream()
    recipes = []
    for doc in docs:
        data = doc.to_dict()
        data["id"] = doc.id
        data = convert_for_json(data)
        recipes.append(data)
    return recipes


def merge_recipes(existing, new):
    """既存レシピと新規レシピをidでマージ"""
    merged = {r["id"]: r for r in existing}
    for r in new:
        merged[r["id"]] = r
    return list(merged.values())


def save_to_json(data):
    """JSON保存"""
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ {len(data)} 件のレシピを {OUTPUT_JSON_PATH} に保存しました。")


def main():
    db = initialize_firestore()

    # 🔹 1. 既存データ読み込み
    existing_recipes = load_existing_recipes()
    latest_updated = get_latest_updated_at(existing_recipes)

    # 🔹 2. Firestoreから差分取得
    new_recipes = fetch_recipes_updated_after(db, latest_updated)

    if not new_recipes:
        print("✨ 新しい更新はありません。")
        return
    print(f"✅ {len(new_recipes)} 件のレシピを 取得しました。")

    # 🔹 3. マージして保存
    merged = merge_recipes(existing_recipes, new_recipes)
    save_to_json(merged)


if __name__ == "__main__":
    main()
