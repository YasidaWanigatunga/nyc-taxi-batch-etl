from pathlib import Path
import requests

from .config import RAW_DATA_DIR, TRIP_DATA_URL, ZONE_LOOKUP_URL

CHUNK = 1024 * 1024


def _download(url: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and destination.stat().st_size > 0:
        print(f"skip (exists): {destination.name}")
        return destination

    tmp = destination.with_suffix(destination.suffix + ".part")
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=CHUNK):
                fh.write(chunk)
    tmp.rename(destination)

    print(f"downloaded: {destination.name} ({destination.stat().st_size:,} bytes)")
    return destination


def download_trip_month(month: str) -> str:
    dest = RAW_DATA_DIR / f"yellow_tripdata_{month}.parquet"
    return str(_download(TRIP_DATA_URL.format(month=month), dest))


def download_zone_lookup() -> str:
    return str(_download(ZONE_LOOKUP_URL, RAW_DATA_DIR / "taxi_zone_lookup.csv"))