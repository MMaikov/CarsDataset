# -*- coding: utf-8 -*-
import csv
import pickle
import os
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from multiprocessing.pool import Pool
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================= CONFIG =============================
BASE_URL = "https://www.thecarspec.net"
CSV_FILE = Path("../CSV_outputs/theCarSpec_cars.csv")
PICKLE_FILE = Path("theCarSpec_cache.pickle")
TEST_MODE = False  # True = ainult 2 brändi

# ============================= SESSION – VÄGA STABIILNE =============================
session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry, pool_maxsize=10)
session.mount("https://", adapter)
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
})

def download(url: str) -> str | None:
    try:
        r = session.get(url, timeout=30, allow_redirects=True)
        if len(r.history) > 10:
            return None
        r.raise_for_status()
        return r.text
    except:
        return None


# ============================= 1. BRÄNDID =============================
def get_brands():
    print("Laen avalehte ja otsin brände...")
    html = download(BASE_URL)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    brands = []
    for h3 in soup.find_all("h3"):
        a = h3.find("a", href=True)
        if a and a["href"].startswith("https://www.thecarspec.net/model/"):
            name = a.get_text(strip=True)
            if name and len(name) > 1:
                brands.append((name, a["href"]))
    brands = list(dict.fromkeys(brands))
    print(f"LEITUD {len(brands)} BRÄNDI! (nt {', '.join(b[0] for b in brands[:5])}...)")
    return brands[:2] if TEST_MODE else brands


# ============================= 2. MUDELID =============================
def get_models(brand_url: str):
    html = download(brand_url)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    models = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/cars/") or href.startswith("https://www.thecarspec.net/cars/"):
            if a.find("div", class_="spec-box"):
                models.add(urljoin(BASE_URL, href))
    return list(models)


# ============================= 3. VARIANDID =============================
def get_variants(model_url: str):
    html = download(model_url)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    variants = set()

    for div in soup.find_all("div", class_="individual-car-title"):
        a = div.find("a", href=True)
        if a and "/car-detail/" in a["href"]:
            variants.add(urljoin(BASE_URL, a["href"]))

    return list(variants)


# ============================= 4. DETAILANDMED – TÄIELIKULT SINU STRUKTUUR =============================
def get_car_data(detail_url: str):
    html = download(detail_url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    data = {"URL": detail_url}

    # 1. Otsime versiooni nime pealkirjast
    title_h5 = soup.find("h5")
    if title_h5:
        title_text = title_h5.get_text(strip=True)
        # Näide: "Abarth 124 Spider (Roadster) 124 GT 2018,2019,2020 Specs"
        parts = title_text.replace("Specs", "").strip().split()
        if len(parts) > 3:
            version = " ".join(parts[-4:]) if "2018" in parts else " ".join(parts[3:])
            data["Version"] = version.strip(",")

    # 2. Kõik tabelid <div class="car-details-discription">
    for section in soup.find_all("div", class_="car-details-discription"):
        h3 = section.find("h3")
        section_name = h3.get_text(strip=True) if h3 else "Unknown"

        for row in section.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                key = th.get_text(strip=True).rstrip(":").strip()
                value_elem = td.find("h4")
                value = value_elem.get_text(strip=True) if value_elem else td.get_text(strip=True, separator=" ")
                value = " ".join(value.split())

                # Puhastame võtme
                if key == "Brand": data["Brand"] = value
                elif key == "Model": data["Model"] = value
                elif key == "Year production start": data["Year production start"] = value
                elif key == "Fuel type": data["Fuel type"] = value
                elif key == "Power (hp)" in key or "horsepower" in key.lower(): data["Power (hp)"] = value
                elif key == "Curb weight kg -lbs total": data["Curb weight (kg)"] = value.split()[0]
                else:
                    data[key] = value

    # Kui saime vähemalt 5 välja + Version → loeme edukaks
    if len(data) > 6 and "Version" in data:
        print(f"[EDUKA] {data.get('Brand', '?')} {data.get('Model', '?')} → {data['Version']}")
        return data
    else:
        return None


# ============================= PÕHIPROGRAMM =============================
def main():
    start = time.time()

    # Kustuta vigane pickle
    if PICKLE_FILE.exists():
        try:
            with open(PICKLE_FILE, "rb") as f:
                test = pickle.load(f)
            if len(test) < 10000:
                print("Vanem pickle poolik → kustutan")
                os.remove(PICKLE_FILE)
        except:
            print("Pickle vigane → kustutan")
            os.remove(PICKLE_FILE)

    if not PICKLE_FILE.exists():
        print("=== ALUSTAN TÄIELIKKU KRAAPIMIST ===\n")

        brands = get_brands()
        if not brands:
            print("Brände ei leitud!")
            return

        with Pool(12) as pool:
            print(f"\nKraabin {len(brands)} brändi mudelid...")
            models_all = pool.map(get_models, [url for _, url in brands])
            all_models = [m for sub in models_all for m in sub]
            print(f"Leitud {len(all_models)} mudelit")

            print("Otsin variante...")
            variants_all = pool.map(get_variants, all_models)
            all_variants = [v for sub in variants_all for v in sub]
            print(f"LEITUD {len(all_variants)} VARIANTI!\n")

            print("Kraabin detailandmeid (20–50 min)...")
            print("EDUKAD AUTOD:")
            print("-" * 80)

            results = pool.map(get_car_data, all_variants)
            cars = [c for c in results if c is not None]

        # Lisa Brand/Model URL-ist kui puudub
        for car in cars:
            if "Brand" not in car or "Model" not in car:
                parts = car["URL"].rstrip("/").split("/")
                if len(parts) >= 6:
                    car["Brand"] = parts[-4].replace("-", " ").title()
                    car["Model"] = parts[-3].replace("-", " ").title()

        with open(PICKLE_FILE, "wb") as f:
            pickle.dump(cars, f)
        print(f"\nUus pickle salvestatud! ({len(cars)} autot)")

    else:
        with open(PICKLE_FILE, "rb") as f:
            cars = pickle.load(f)
        print(f"Laetud {len(cars)} autot pickle'ist")

    # CSV
    all_keys = set().union(*cars)
    priority = ["Brand", "Model", "Version", "Year production start", "Engine version",
                "Fuel type", "Power (hp)", "Top Speed", "Curb weight (kg)", "URL"]
    fieldnames = [k for k in priority if k in all_keys] + sorted(k for k in all_keys if k not in priority)

    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cars)

    print(f"\nVALMIS! → {CSV_FILE}")
    print(f"Kokku salvestatud: {len(cars)} rida")
    print(f"Kogu aeg: {time.time() - start:.1f} sekundit")


if __name__ == "__main__":
    main()