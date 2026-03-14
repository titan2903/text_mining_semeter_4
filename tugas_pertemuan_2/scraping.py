from pathlib import Path
from typing import Any, Optional
import pandas as pd
from google_play_scraper import Sort, reviews


def scrape_noisy_reviews(
    app_id: str,
    fetch_count: int = 200,
    lang: str = "id",
    country: str = "id",
    sort: Sort = Sort.NEWEST,
    filter_score_with: Optional[int] = None,
    batch_size: int = 200,
) -> pd.DataFrame:
    """
    Mengekstrak ulasan dari Play Store secara bertahap (pagination).

    Parameters
    ----------
    app_id : str
        Package name aplikasi di Play Store.
    fetch_count : int
        Jumlah maksimum ulasan yang ingin diambil.
    lang : str
        Bahasa ulasan (default: id).
    country : str
        Negara ulasan (default: id).
    sort : Sort
        Urutan ulasan (default: NEWEST).
    filter_score_with : Optional[int]
        Filter rating 1-5. Gunakan None untuk semua skor.
    batch_size : int
        Banyak ulasan per request API.
    """
    if fetch_count <= 0:
        raise ValueError("fetch_count harus lebih dari 0.")
    if batch_size <= 0:
        raise ValueError("batch_size harus lebih dari 0.")

    fetched_reviews: list[dict[str, Any]] = []
    continuation_token = None

    try:
        while len(fetched_reviews) < fetch_count:
            current_count = min(batch_size, fetch_count - len(fetched_reviews))
            result, continuation_token = reviews(
                app_id,
                lang=lang,
                country=country,
                sort=sort,
                count=current_count,
                filter_score_with=filter_score_with,
                continuation_token=continuation_token,
            )

            if not result:
                break

            for review in result:
                fetched_reviews.append(
                    {
                        "reviewId": review.get("reviewId"),
                        "userName": review.get("userName"),
                        "score": review.get("score"),
                        "at": review.get("at"),
                        "content": review.get("content"),
                        "thumbsUpCount": review.get("thumbsUpCount"),
                    }
                )

            print(f"Progress scraping: {len(fetched_reviews)}/{fetch_count}")

            if continuation_token is None:
                break

        if not fetched_reviews:
            return pd.DataFrame(
                columns=[
                    "reviewId",
                    "userName",
                    "score",
                    "at",
                    "content",
                    "thumbsUpCount",
                ]
            )

        df = pd.DataFrame(fetched_reviews)
        if df["reviewId"].notna().any():
            df = df.drop_duplicates(subset=["reviewId"])
        else:
            df = df.drop_duplicates(subset=["userName", "content", "at"])

        df = df.reset_index(drop=True)
        df["at"] = pd.to_datetime(df["at"], errors="coerce")
        return df

    except Exception as e:
        print(f"Error saat melakukan scraping: {e}")
        return pd.DataFrame()


def main() -> None:
    # Access by KAI
    target_app = "com.kai.kaiticketing"
    total_data = 1000
    output_path = Path(__file__).resolve().parent / "raw_playstore_reviews.csv"

    print(f"Memulai scraping untuk {target_app}...")
    df_reviews = scrape_noisy_reviews(
        target_app,
        fetch_count=total_data,
        lang="id",
        country="id",
        sort=Sort.NEWEST,
        filter_score_with=None,
    )

    if df_reviews.empty:
        print("Gagal mendapatkan data. Periksa App ID, koneksi, atau limit Play Store.")
        return

    print(f"Berhasil mengekstrak {len(df_reviews)} baris data.")
    # Simpan CSV di folder script agar lokasi file konsisten.
    df_reviews.to_csv(output_path, index=False)
    print(f"Data disimpan ke '{output_path}'")
    print("\nPreview 3 data teratas:")
    print(df_reviews[["score", "content", "at"]].head(3).to_string(index=False))


if __name__ == "__main__":
    main()
