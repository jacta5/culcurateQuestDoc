import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urljoin
from datetime import datetime, timezone
from pathlib import Path

INPUT_URL = "https://jacta5.github.io/culcurateQuestDoc/menuPlanner/docs/recipes_export.json"

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "ogp-cache.json"


def extract_youtube_thumbnail(url):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)

    if "youtube.com" in parsed.netloc and "v" in query:
        return f"https://img.youtube.com/vi/{query['v'][0]}/maxresdefault.jpg"

    if "youtu.be" in parsed.netloc:
        video_id = parsed.path.lstrip("/")
        return f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg"

    return None


def fetch_ogp(url):
    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        html = response.text
    except Exception as e:
        print(f"[ERROR] Failed to fetch {url}: {e}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    def ogp(property_name):
        tag = soup.find("meta", property=property_name)
        if tag and tag.get("content"):
            return tag["content"]
        return None

    image = ogp("og:image")
    title = ogp("og:title")
    description = ogp("og:description")

    if image:
        image = urljoin(url, image)

    if not image:
        yt_img = extract_youtube_thumbnail(url)
        if yt_img:
            image = yt_img

    return {
        "url": url,
        "image": image,
        "title": title,
        "description": description,
        "updatedAt": datetime.now(timezone.utc).isoformat()  # ← ★追加
    }


def main():
    try:
        print(f"[Fetch JSON] {INPUT_URL}")
        res = requests.get(INPUT_URL, timeout=10)
        recipes = res.json()
    except Exception as e:
        print(f"[ERROR] Cannot load JSON from URL: {e}")
        return

    ogp_cache = {}

    for item in recipes:
        url = item.get("url")
        if not url:
            continue

        print(f"[Fetch OGP] {url}")
        ogp = fetch_ogp(url)
        if ogp:
            ogp_cache[url] = ogp

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(ogp_cache, f, ensure_ascii=False, indent=2)

    print(f"\nDONE! Saved OGP cache to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
