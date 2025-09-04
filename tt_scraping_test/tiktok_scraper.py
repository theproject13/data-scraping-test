from apify_client import ApifyClient
import logging
import time
import os
from decouple import config
import pandas as pd
from typing import List, Dict

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Ambil token dari .env
APIFY_TOKEN = config('APIFY_TOKEN')
ACTOR_ID = "clockworks~free-tiktok-scraper"

def run_tiktok_scraper(video_urls: list, sync_mode: bool = False) -> list:
    """Jalankan TikTok scraper dan ambil data."""
    # Inisialisasi client
    client = ApifyClient(APIFY_TOKEN)

    # Persiapan input untuk Actor (menggunakan postURLs sesuai kebutuhan Actor)
    run_input = {
        "postURLs": video_urls,  # Ganti startUrls dengan postURLs
        "resultsPerPage": 100,
        "proxyCountryCode": "ID",  # Sesuaikan dengan kebutuhan (ID untuk Indonesia)
        "excludePinnedPosts": False,
        "scrapeRelatedVideos": False,
        "shouldDownloadVideos": False,
        "shouldDownloadCovers": False,
        "shouldDownloadSubtitles": False,
        "shouldDownloadSlideshowImages": False,
        "shouldDownloadAvatars": False,
        "shouldDownloadMusicCovers": False,
    }

    try:
        # Jalankan Actor secara asinkronus
        logger.info("Menjalankan Actor secara asinkronus...")
        run = client.actor(ACTOR_ID).call(run_input=run_input)
        run_id = run["id"]
        logger.info(f"Run ID: {run_id}. Menunggu hasil...")

        # Polling status run sampai selesai (max 60 detik)
        for _ in range(12):  # 12 x 5 detik = 60 detik
            run_status = client.run(run_id).get()
            status = run_status.get("status")
            logger.info(f"Status run: {status}")
            if status == "SUCCEEDED":
                break
            elif status in ["FAILED", "TIMED_OUT"]:
                raise Exception(f"Run gagal dengan status: {status}")
            time.sleep(5)  # Tunggu 5 detik sebelum cek lagi
        else:
            raise Exception("Run timeout setelah 60 detik")

        # Ambil dataset setelah run selesai
        dataset_id = run_status["defaultDatasetId"]
        logger.info(f"Ambil dataset dari ID: {dataset_id}")
        items = list(client.dataset(dataset_id).iterate_items())
        if not items:
            logger.warning("Tidak ada data dari dataset.")
        return items

    except Exception as e:
        logger.error(f"Error saat menjalankan scraper: {e}")
        raise

def normalize_items(items: List[Dict]) -> pd.DataFrame:
    """Normalisasi data TikTok ke DataFrame."""
    rows = []
    for item in items:
        author = item.get("authorMeta", {})
        music = item.get("musicMeta", {})
        rows.append({
            "video_id": item.get("id", ""),
            "video_url": item.get("webVideoUrl", item.get("url", "")),
            "caption": item.get("text", ""),
            "create_time": item.get("createTime", ""),
            "views": item.get("playCount", 0),
            "likes": item.get("diggCount", 0),
            "comments": item.get("commentCount", 0),
            "shares": item.get("shareCount", 0),
            "username": author.get("name", ""),
            "nickname": author.get("nickName", ""),
            "followers": author.get("fans", 0),
            "following": author.get("following", 0),
            "total_videos": author.get("video", 0),
            "music_name": music.get("musicName", ""),
            "music_author": music.get("musicAuthor", "")
        })
    df = pd.DataFrame(rows)
    return df

def export_excel(df: pd.DataFrame, path: str = "output/tiktok_metrics.xlsx") -> str:
    """Simpan DataFrame ke Excel."""
    os.makedirs("output", exist_ok=True)  # Buat folder kalau belum ada
    cols = [
        "video_id", "video_url", "caption", "create_time",
        "views", "likes", "comments", "shares",
        "username", "nickname", "followers", "following", "total_videos",
        "music_name", "music_author"
    ]
    cols = [c for c in cols if c in df.columns]
    df[cols].to_excel(path, index=False)
    return path

if __name__ == "__main__":
    VIDEO_URLS = [
        "https://www.tiktok.com/@idealis92/video/7545305469414411576",
        "https://www.tiktok.com/@idealis92/video/7544924362500017414"
    ]

    try:
        items = run_tiktok_scraper(VIDEO_URLS)
        if not items:
            print("Tidak ada data. Cek URL atau kuota Apify.")
        else:
            df = normalize_items(items)
            output_path = export_excel(df)
            print(f"Selesai. Data tersimpan di: {output_path} | {len(df)} baris")
    except Exception as e:
        print(f"Error: {e}")