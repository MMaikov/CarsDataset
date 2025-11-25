# thecarspec_scraper.py
from bs4 import BeautifulSoup
from bs4.element import Tag
import requests
from typing import cast, Callable, Sequence, List, TypeVar
from urllib.parse import urljoin
import csv
from multiprocessing.pool import Pool
import logging
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
import pickle
import time

# CONFIG
BASE_URL = "https://www.thecarspec.net"
PICKLE_PATH = Path('theCarSpec_cache.pickle')
CSV_PATH = Path('../CSV_outputs/theCarSpec_cars.csv')
TEST_MODE = False  # True -> piirab brändide arvu (kiirem testimiseks)

# Logging & session globals
session = None

def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(processName)s] %(message)s"
    )

def init_session():
    global session
    session = requests.Session()
    retries = Retry(total=5, connect=5, read=5, backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

def initialize_process():
    init_logging()
    init_session()

def download_page(link: str) -> str | None:
    global session
    if session is None:
        logging.error("Session is not initialized!")
        return None
    try:
        r = session.get(link, timeout=30, allow_redirects=True)
        if len(r.history) > 10:
            logging.error("Too many redirects for %s", link)
            return None
        r.raise_for_status()
        return r.text
    except requests.exceptions.TooManyRedirects:
        logging.error("Too many redirects for %s", link)
        return None
    except requests.exceptions.HTTPError as e:
        logging.error("Got HTTP Error %s for %s", getattr(e.response, "status_code", ""), link)
        return None
    except requests.exceptions.Timeout:
        logging.error("Request timed out for %s", link)
        return None
    except requests.exceptions.RequestException:
        logging.exception("Got exception for %s", link)
        return None

# ------------ parsing functions ------------

def parse_brands(link: str) -> list[str]:
    html = download_page(link)
    if html is None:
        logging.error("Couldn't download page %s", link)
        return []

    soup = BeautifulSoup(html, "html.parser")
    brands = []
    for h3 in soup.find_all("h3"):
        a = h3.find("a", href=True)
        if a and a["href"].startswith("https://www.thecarspec.net/model/"):
            name = a.get_text(strip=True)
            if name and len(name) > 1:
                # store the brand page URL
                brands.append(urljoin(link, a["href"]))
    # dedupe preserving order
    seen = set()
    unique_brands = []
    for b in brands:
        if b not in seen:
            seen.add(b)
            unique_brands.append(b)
    if TEST_MODE:
        unique_brands = unique_brands[:2]
    logging.info("Found %d brand pages to parse", len(unique_brands))
    return unique_brands

def parse_models(brand_url: str) -> list[str]:
    html = download_page(brand_url)
    if not html:
        logging.error("Couldn't download brand page %s", brand_url)
        return []
    soup = BeautifulSoup(html, "html.parser")
    models = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/cars/") or href.startswith("https://www.thecarspec.net/cars/"):
            # check that this anchor contains the spec-box (same heuristic)
            if a.find("div", class_="spec-box"):
                models.add(urljoin(BASE_URL, href))
    logging.info("Parsed %d models from %s", len(models), brand_url)
    return list(models)

def parse_variants(model_url: str) -> list[str]:
    html = download_page(model_url)
    if not html:
        logging.error("Couldn't download model page %s", model_url)
        return []
    soup = BeautifulSoup(html, "html.parser")
    variants = set()
    for div in soup.find_all("div", class_="individual-car-title"):
        a = div.find("a", href=True)
        if a and "/car-detail/" in a["href"]:
            variants.add(urljoin(BASE_URL, a["href"]))
    logging.info("Parsed %d variants from %s", len(variants), model_url)
    return list(variants)

def direct_text(tag: Tag) -> str:
    if tag is None:
        return ""
    result = ''.join(tag.find_all(string=True, recursive=False)).strip().replace('\n', '')
    if result:
        return result
    for child in tag.contents:
        if isinstance(child, Tag):
            return ''.join(child.find_all(string=True, recursive=False)).strip().replace('\n', '')
    return ''

def parse_car(detail_url: str) -> dict[str, str] | None:
    html = download_page(detail_url)
    if not html:
        logging.error("Couldn't download detail page %s", detail_url)
        return None

    soup = BeautifulSoup(html, "html.parser")
    data: dict[str,str] = {"URL": detail_url}

    # 1) Version from <h5> if present
    title_h5 = soup.find("h5")
    if title_h5:
        title_text = title_h5.get_text(strip=True)
        # attempt to replicate earlier logic — try to remove 'Specs' and extract trailing part
        title_clean = title_text.replace("Specs", "").strip()
        parts = title_clean.split()
        if len(parts) > 3:
            # basic heuristic
            data["Version"] = " ".join(parts[3:]).strip(',')

    # 2) Extract tables in <div class="car-details-discription">
    for section in soup.find_all("div", class_="car-details-discription"):
        h3 = section.find("h3")
        section_name = h3.get_text(strip=True) if h3 else "Unknown"
        for row in section.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if not th or not td:
                continue
            key = th.get_text(strip=True).rstrip(":").strip()
            # prefer <h4> inside td if exists (per original)
            value_elem = td.find("h4")
            if value_elem:
                value = value_elem.get_text(strip=True)
            else:
                value = td.get_text(strip=True, separator=" ")
            value = " ".join(value.split())

            # normalize some keys similar to original mapping
            if key == "Brand":
                data["Brand"] = value
            elif key == "Model":
                data["Model"] = value
            elif key == "Year production start":
                data["Year production start"] = value
            elif key == "Fuel type":
                data["Fuel type"] = value
            elif "Power (hp)" in key or "horsepower" in key.lower():
                data["Power (hp)"] = value
            elif key == "Curb weight kg -lbs total":
                # take the first token as kg
                data["Curb weight (kg)"] = value.split()[0] if value else value
            else:
                # fallback store raw
                if key not in data:
                    data[key] = value

    # success heuristic: at least 5 fields and Version present (like original)
    if len(data) > 6 and "Version" in data:
        logging.info("[SUCCESS] %s %s -> %s", data.get("Brand", "?"), data.get("Model", "?"), data.get("Version"))
        return data
    else:
        logging.debug("Insufficient data for %s (fields=%d)", detail_url, len(data))
        return None

# Generic pool map with tqdm
T = TypeVar("T")
R = TypeVar("R")
def pool_map(pool: Pool, description: str, fun: Callable[[T], R], args: Sequence[T]) -> List[R]:
    results: List[R] = []
    for result in tqdm(pool.imap_unordered(fun, args), total=len(args), desc=description):
        results.append(result)
    return results

def write_to_csv(filepath: Path, cars: list[dict[str,str]]) -> None:
    if not cars:
        logging.warning("No cars to write to CSV")
        return
    all_keys: set[str] = set()
    for car in cars:
        all_keys.update(car.keys())

    priority_keys: list[str] = ["Brand", "Model", "Version", "Year production start",
                                "Engine version", "Fuel type", "Power (hp)", "Top Speed",
                                "Curb weight (kg)", "URL"]
    fieldnames: list[str] = priority_keys + [k for k in all_keys if k not in priority_keys]

    for car in cars:
        for key in fieldnames:
            car.setdefault(key, '')

    with filepath.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, delimiter=',')
        writer.writeheader()
        writer.writerows(cars)
    logging.info("Wrote %d cars to CSV %s", len(cars), filepath)

def write_pickle(filepath: Path, cars: list[dict[str,str]]) -> None:
    with filepath.open('wb') as f:
        pickle.dump(cars, f)
    logging.info("Pickle saved to %s", filepath)

def read_pickle(filepath: Path) -> list[dict[str,str]]:
    with filepath.open('rb') as f:
        return pickle.load(f)

def parse_cars() -> list[dict[str,str]] | None:
    URL = BASE_URL
    with Pool(processes=12, initializer=initialize_process) as pool:
        logging.info("Parsing brand links")
        brand_links = parse_brands(URL)
        logging.info("There are %d brands to parse", len(brand_links))
        if len(brand_links) < 1:
            logging.error("Amount of brand links is zero")
            return None

        logging.info("Parsing models for each brand")
        model_links_nested = pool_map(pool, "Scraping models from each brand", parse_models, brand_links)
        model_links = [item for sublist in model_links_nested for item in sublist]
        logging.info("There are %d model links", len(model_links))
        if len(model_links) < 1:
            logging.error("Amount of model links is zero")
            return None

        logging.info("Parsing variants for each model")
        generation_variant_links_nested = pool_map(pool, "Scraping variants from each model", parse_variants, model_links)
        variant_links = [item for sublist in generation_variant_links_nested for item in sublist]
        logging.info("There are %d variant links", len(variant_links))
        if len(variant_links) < 1:
            logging.error("Amount of variant links is zero")
            return None

        logging.info("Parsing car for each variant")
        cars_nested = pool_map(pool, "Scraping cars from each variant", parse_car, variant_links)
        cars = [c for c in cars_nested if c is not None]
        logging.info("Parsed %d cars", len(cars))
        if len(cars) < 1:
            logging.error("Amount of cars parsed is zero")
            return None

    return cars

def main() -> None:
    initialize_process()
    start = time.time()

    # check and possibly remove broken pickle like original
    if PICKLE_PATH.exists():
        try:
            with PICKLE_PATH.open('rb') as f:
                cached = pickle.load(f)
            if not isinstance(cached, list) or len(cached) < 100:
                logging.info("Cached pickle looks partial or small -> removing")
                PICKLE_PATH.unlink()
        except Exception:
            logging.info("Pickle invalid -> removing")
            try:
                PICKLE_PATH.unlink()
            except Exception:
                pass

    if not PICKLE_PATH.exists():
        logging.info("Starting full scraping run")
        cars = parse_cars()
        if cars is None:
            logging.error("Parsed 'cars' is none — aborting")
            return

        try:
            write_pickle(PICKLE_PATH, cars)
        except Exception:
            logging.exception("Failed to pickle cars dataset!")

        try:
            write_to_csv(CSV_PATH, cars)
        except Exception:
            logging.exception("Failed to write cars dataset to CSV!")

    else:
        try:
            cars = read_pickle(PICKLE_PATH)
            logging.info("Loaded %d cars from pickle", len(cars))
            write_to_csv(CSV_PATH, cars)
        except Exception:
            logging.exception("Failed to read/convert existing pickle")
            return

    end = time.time()
    logging.info("All done. Took %.1f s", end - start)

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    duration = end - start
    logging.info("Took %f s", duration)