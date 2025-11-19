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

PICKLE_PATH = Path('cars.pickle')
CSV_PATH = Path('cars.csv')

def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(processName)s] %(message)s"
    )

session = None
def init_session():
    global session
    session = requests.Session()
    retries = Retry (
        total=5,
        connect=5,
        read=5,
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

def initialize_process():
    init_logging()
    init_session()

def download_page(link: str) -> str | None:
    global session
    if session is None:
        logging.error("Session is not initialized!")
        return None

    try:
        r = session.get(link, timeout=10)
        r.raise_for_status()
        return r.text
    except requests.exceptions.TooManyRedirects:
        logging.error("Too many redirects for %s", link)
        return None
    except requests.exceptions.HTTPError as e:
        logging.error("Got HTTP Error %d: %s for %s", e.response.status_code, e.response.reason, link)
        return None
    except requests.exceptions.Timeout:
        logging.error("Request timed out for %s", link)
        return None
    except requests.exceptions.RequestException:
        logging.exception("Got exception for %s", link)
        return None


def parse_brands(link: str) -> list[str]:
    html: str | None = download_page(link)
    if html is None:
        logging.error("Couldn't download page %s", link)
        return []

    soup = BeautifulSoup(html, features='html.parser')
    brands_container = soup.find('div', attrs={'class': 'markite'})

    if brands_container is None:
        logging.error("Brands container is none for %s", link)
        return []

    brands: list[str] = []
    for brand_a in brands_container.find_all('a'):
        try:
            brand = cast(str | None, brand_a.get('title'))
            if brand is not None:
                brand_link: str = cast(str, brand_a.get('href'))
                brand_link: str = urljoin(link, brand_link)
                brands.append(brand_link)
        except Exception:
            logging.exception("Got exception for %s", link)

    return brands


def parse_models(link: str) -> list[str]:
    html: str | None = download_page(link)
    if html is None:
        logging.error("Couldn't download page %s", link)
        return []

    soup = BeautifulSoup(html, features='html.parser')

    models_container = soup.find('ul', attrs={'class': 'modelite'})
    if models_container is None:
        logging.error("Models container is none for %s", link)
        return []

    models: list[str] = []
    for modelite_ul in models_container.find_all('ul'):
        for modelite_li in modelite_ul.find_all('li'):
            try:
                modelite_a = modelite_li.find('a')
                assert modelite_a is not None

                model_link: str = cast(str, modelite_a.get('href'))
                model_link: str = urljoin(link, model_link)

                models.append(model_link)
            except Exception:
                logging.exception("Got exception for %s", link)
    
    return models

def parse_generations(link: str) -> list[str]:
    html: str | None = download_page(link)
    if html is None:
        logging.error("Couldn't download page %s", link)
        return []

    soup = BeautifulSoup(html, features='html.parser')

    generations_container = soup.find('table', attrs={'id': 'generr'})
    if generations_container is None:
        logging.error("Generations container is none for page %s", link)
        return []

    generations: list[str] = []
    for generation in generations_container.find_all('th'):
        try:
            generation_a = generation.find('a')
            assert generation_a is not None
            generation_link: str = cast(str, generation_a.get('href'))
            generation_link: str = urljoin(link, generation_link)
            generations.append(generation_link)
        except Exception:
            logging.exception("Got an exception for %s", link)
    
    return generations

def parse_variants(link: str) -> list[str]:
    html: str | None = download_page(link)
    if html is None:
        logging.error("Couldn't download page %s", link)
        return []
    
    soup = BeautifulSoup(html, features='html.parser')

    variants_container = soup.find('table', attrs={'class': 'carlist'})
    if variants_container is None:
        logging.error("Variants container is none for %s", link)
        return []

    cars: list[str] = []
    for variant in variants_container.find_all('tr', attrs={'class': 'i'}):
        try:
            variant_th = variant.find('th')
            assert variant_th is not None
            variant_a = variant_th.find('a')
            assert variant_a is not None
            variant_link: str = cast(str, variant_a.get('href'))
            variant_link: str = urljoin(link, variant_link)
            cars.append(variant_link)
        except Exception:
            logging.exception("Got an exception for %s", link)
    
    return cars


def direct_text(tag: Tag) -> str:
    # Return only the text directly inside the tag, excluding child tags.
    result = ''.join(tag.find_all(string=True, recursive=False)).strip().replace('\n', '')
    if result:
        return result
    
    for child in tag.contents:
        if isinstance(child, Tag):
            return ''.join(child.find_all(string=True, recursive=False)).strip().replace('\n', '')
        
    return ''


def parse_car(link: str) -> dict[str, str]:
    html: str | None = download_page(link)
    if html is None:
        logging.error("Couldn't download page %s", link)
        return {}
    
    soup = BeautifulSoup(html, features='html.parser')

    table = soup.find('table', attrs={'class': 'cardetailsout car2'})
    if table is None:
        logging.error("Table is none for %s", link)
        return {}

    attributes: dict[str,str]= {}
    for row in table.find_all('tr'):
        try:
            # We are not interested in rows that have a class,
            # since that doesn't contain any information
            if row.has_attr('class'):
                continue

            key_tag = row.find('th')
            assert key_tag is not None
            key: str = direct_text(key_tag)
            value_tag = row.find('td')
            assert value_tag is not None
            value = direct_text(value_tag)

            if key not in attributes:
                attributes[key] = value
        except Exception:
            logging.exception("Got an exception for %s", link)

    return attributes


T = TypeVar("T")
R = TypeVar("R")
def pool_map(pool: Pool, description: str, fun: Callable[[T], R], args: Sequence[T]) -> List[R]:
    results: List[R] = []
    for result in tqdm(pool.imap_unordered(fun, args), total=len(args), desc=description):
        results.append(result)
    return results

def write_to_csv(filepath: Path, cars: list[dict[str, str]]) -> None:
    all_keys: set[str] = set()
    for car in cars:
        all_keys.update(car.keys())

    
    # For easier viewing we want some columns to be first and grouped together
    priority_keys: list[str] = ['Brand', 'Model', 'Generation', 'Start of production', 
                                'End of production', 'Modification (Engine)', 'Powertrain Architecture',
                                'Body type', 'Fuel Type', 'Max. weight', 'Length', 'Width', 'Height']

    fieldnames: list[str] = priority_keys + [k for k in all_keys if k not in priority_keys]

    for car in cars:
        for key in fieldnames:
            car.setdefault(key, '')

    with filepath.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, delimiter=',')
        writer.writeheader()
        writer.writerows(cars)

def write_pickle(filepath: Path, cars: list[dict[str, str]]) -> None:
    with filepath.open('wb') as f:
        pickle.dump(cars, f)

def read_pickle(filepath: Path) -> list[dict[str, str]]:
    with filepath.open('rb') as f:
        return pickle.load(f)

def parse_cars() -> list[dict[str, str]] | None:
    URL = 'https://www.auto-data.net/en/'
    with Pool(processes=64, initializer=initialize_process) as pool:
        logging.info("Parsing brand links")
        brand_links = parse_brands(URL)
        logging.info(f"There are {len(brand_links)} brands to parse")
        if len(brand_links) < 1:
            logging.error("Amount of brand links is zero")
            return None
        logging.info("Parsing models for each brand")
        model_links = pool_map(pool, "Scraping models from each brand", parse_models, brand_links)
        model_links = [item for sublist in model_links for item in sublist]
        logging.info(f"There are {len(model_links)} models to parse across all brands")
        if len(model_links) < 1:
            logging.error("Amount of model links is zero")
            return None
        
        logging.info("Parsing generations for each model")
        generation_links = pool_map(pool, "Scraping generations from each model of each brand", parse_generations, model_links)
        generation_links = [item for sublist in generation_links for item in sublist]
        logging.info(f"There are {len(generation_links)} generations to parse across all models")
        if len(generation_links) < 1:
            logging.error("Amount of generation links is zero")
            return None

        logging.info("Parsing variants for each generation")
        variant_links = pool_map(pool, "Scraping variants from each generation", parse_variants, generation_links)
        variant_links = [item for sublist in variant_links for item in sublist]
        logging.info(f"There are {len(variant_links)} variants to parse across all generations")
        if len(variant_links) < 1:
            logging.error("Amount of variant links is zero")
            return None

        logging.info("Parsing car for each variant")
        cars: list[dict[str, str]] = pool_map(pool, "Scraping cars from each variant", parse_car, variant_links)
        logging.info(f"Parsed {len(cars)} cars")
        if len(cars) < 1:
            logging.error("Amount of cars parsed is zero")
            return None
        
    return cars

def main() -> None:
    initialize_process()

    if PICKLE_PATH.exists():
        logging.info("Cached dataset found, loading it.")
        cars = read_pickle(PICKLE_PATH)
        logging.info("Loaded %d cars", len(cars))
        write_to_csv(CSV_PATH, cars)
    else:
        logging.info("Didn't find cached dataset, scraping it.")
        cars = parse_cars()
        if cars is None:
            logging.error("Parsed 'cars' is none")
            return    

        try:
            logging.info("Pickling cars dataset")
            write_pickle(PICKLE_PATH, cars)
        except Exception:
            logging.exception("Failed to picke cars dataset!")

        try:
            logging.info("Writing cars dataset to a csv file!")
            write_to_csv(CSV_PATH, cars)
        except Exception:
            logging.exception("Failed to write cars dataset to a csv file")

    logging.info("All done.")


if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    duration = end - start
    logging.info("Took %f s", duration)