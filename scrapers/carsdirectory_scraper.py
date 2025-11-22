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
from dataclasses import dataclass

PICKLE_PATH = Path('carsdirectory.pickle')
CSV_PATH = Path('carsdirectory.csv')

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
    
def direct_text(tag: Tag) -> str:
    # Return only the text directly inside the tag, excluding child tags.
    result = ''.join(tag.find_all(string=True, recursive=False)).strip().replace('\n', '')
    if result:
        return result
    
    for child in tag.contents:
        if isinstance(child, Tag):
            return ''.join(child.find_all(string=True, recursive=False)).strip().replace('\n', '')
        
    return ''

T = TypeVar("T")
R = TypeVar("R")
def pool_map(pool: Pool, description: str, fun: Callable[[T], R], args: Sequence[T]) -> List[R]:
    results: List[R] = []
    for result in tqdm(pool.imap_unordered(fun, args), total=len(args), desc=description):
        results.append(result)
    return results

@dataclass
class BrandPage:
    brand: str
    brand_link: str

def parse_brands(link: str) -> list[BrandPage]:
    html: str | None = download_page(link)
    if html is None:
        logging.error("Couldn't download page %s", link)
        return []
    
    soup = BeautifulSoup(html, features='html.parser')

    brands_container = soup.find('div', attrs={'class': 'justify-content-left'})
    if brands_container is None:
        logging.error("brands container does not exist for page %s", link)
        return []
    
    brands: list[BrandPage] = []
    for brand_tag in brands_container.find_all('div', attrs={'class': 'col-xs-6 p-1 card'}):
        try:
            brand_a = brand_tag.find('a')
            assert brand_a is not None
            brand_link: str = cast(str, brand_a.get('href'))
            brand_link: str = urljoin(link, brand_link)
            brand_name_tag = brand_a.find('div', attrs={'class': 'card-text text-center'})
            assert brand_name_tag is not None
            brand_name: str = direct_text(brand_name_tag)
            brands.append(BrandPage(brand_name, brand_link))
        except Exception:
            logging.exception("Got an exception for %s", link)

    return brands

@dataclass
class ModelPage:
    brand: str
    model: str
    model_link: str

def parse_models(brand_page: BrandPage) -> list[ModelPage]:
    html: str | None = download_page(brand_page.brand_link)
    if html is None:
        logging.error("Couldn't download page %s", brand_page)
        return []
    
    soup = BeautifulSoup(html, features='html.parser')

    models_container = soup.find('div', attrs={'class': 'justify-content-left'})
    if models_container is None:
        logging.error("Models container doesn't exist for page %s", brand_page)
        return []
    
    models: list[ModelPage] = []
    for model_tag in models_container.find_all('div', attrs={'class': 'col-xs-6 p-1 card'}):
        try:
            model_a = model_tag.find('a')
            assert model_a is not None
            model_link: str = cast(str, model_a.get('href'))
            model_link: str = urljoin(brand_page.brand_link, model_link)

            model_name_tag = model_a.find('div', attrs={'class': 'card-text text-center'})
            assert model_name_tag is not None

            model_name: str = direct_text(model_name_tag)

            models.append(ModelPage(brand_page.brand, model_name, model_link))
        except Exception:
            logging.exception("Got an exception for %s", brand_page)

    return models

@dataclass
class GenerationPage:
    brand: str
    model: str
    generation_link: str

def parse_generations(model_page: ModelPage) -> list[GenerationPage]:
    html: str | None = download_page(model_page.model_link)
    if html is None:
        logging.error("Couldn't download page %s", model_page)
        return []
    
    soup = BeautifulSoup(html, features='html.parser')

    generations_container = soup.find('div', attrs={'class': 'justify-content-left'})
    if generations_container is None:
        logging.error("Generations container doesn't exist for %s", model_page)
        return []
    
    generations: list[GenerationPage] = []
    for generation_tag in generations_container.find_all('div', attrs={'class': 'col-xs-6 p-1 card'}):
        try:
            generation_a = generation_tag.find('a')
            assert generation_a is not None
            generation_link: str = cast(str, generation_a.get('href'))
            generation_link: str = urljoin(model_page.model_link, generation_link)
            generations.append(GenerationPage(model_page.brand, model_page.model, generation_link))
        except Exception:
            logging.exception("Got an exception for %s", model_page)

    return generations

@dataclass
class VariantPage:
    brand: str
    model: str
    variant_link: str

def parse_variants(generation_page: GenerationPage) -> list[VariantPage]:
    html: str | None = download_page(generation_page.generation_link)
    if html is None:
        logging.error("Couldn't download page %s", generation_page)
        return []
    
    soup = BeautifulSoup(html, features='html.parser')

    variants_container = soup.find('table', attrs={'class': 'table'})
    if variants_container is None:
        logging.error("Variants container doesn't exist for %s", generation_page)
        return []
    
    variants: list[VariantPage] = []
    for variant_a in variants_container.find_all('a'):
        try:
            variant_link: str = cast(str, variant_a.get('href'))
            variant_link: str = urljoin(generation_page.generation_link, variant_link)
            variants.append(VariantPage(generation_page.brand, generation_page.model, variant_link))
        except Exception:
            logging.exception("Got an exception for %s", generation_page)

    return variants

def parse_car(variant_page: VariantPage) -> dict[str, str]:
    html: str | None = download_page(variant_page.variant_link)
    if html is None:
        logging.error("Couldn't download page %s", variant_page)
        return {}
    
    soup = BeautifulSoup(html, features='html.parser')

    table = soup.find('table', attrs={'class': 'table'})
    if table is None:
        logging.error("Table is none for %s", variant_page)
        return {}
    
    attributes: dict[str, str] = {
        'Brand': variant_page.brand,
        'BrandModel': variant_page.model
    }
    for row in table.find_all('tr'):
        try:
            key_tag = row.find('th')
            value_tag = row.find('td')
            if key_tag is None or value_tag is None:
                continue

            key: str = direct_text(key_tag)
            value: str = direct_text(value_tag)
            if key not in attributes:
                attributes[key] = value
        except Exception:
            logging.exception("Got an exception for %s", variant_page)

    return attributes

def write_to_csv(filepath: Path, cars: list[dict[str, str]]) -> None:
    all_keys: set[str] = set()
    for car in cars:
        all_keys.update(car.keys())

    
    # For easier viewing we want some columns to be first and grouped together
    priority_keys: list[str] = ['Brand', 'BrandModel', 'Generation', 'Start of production', 
                                'End of production', 'Modification', 'Powertrain Architecture',
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
    

def flatten(list: list[list[T]]) -> list[T]:
    return [item for sublist in list for item in sublist]

def parse_cars() -> list[dict[str, str]] | None:
    URL = 'https://www.cars-directory.net/car-specs/'
    with Pool(processes=64, initializer=initialize_process) as pool:
        logging.info("Parsing brand links")
        brand_links = parse_brands(URL)
        logging.info(f"There are {len(brand_links)} brands to parse")
        if len(brand_links) < 1:
            logging.error("Amount of brand links is zero")
            return None
        logging.info("Parsing models for each brand")
        model_links = pool_map(pool, "Scraping models from each brand", parse_models, brand_links)
        model_links = flatten(model_links)
        logging.info(f"There are {len(model_links)} models to parse across all brands")
        if len(model_links) < 1:
            logging.error("Amount of model links is zero")
            return None
        
        logging.info("Parsing generations for each model")
        generation_links = pool_map(pool, "Scraping generations from each model of each brand", parse_generations, model_links)
        generation_links = flatten(generation_links)
        logging.info(f"There are {len(generation_links)} generations to parse across all models")
        if len(generation_links) < 1:
            logging.error("Amount of generation links is zero")
            return None

        logging.info("Parsing variants for each generation")
        variant_links = pool_map(pool, "Scraping variants from each generation", parse_variants, generation_links)
        variant_links = flatten(variant_links)
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
            logging.info("Pickling carsdirectory dataset")
            write_pickle(PICKLE_PATH, cars)
        except Exception:
            logging.exception("Failed to picke carsdirectory dataset!")

        try:
            logging.info("Writing carsdirectory dataset to a csv file!")
            write_to_csv(CSV_PATH, cars)
        except Exception:
            logging.exception("Failed to write carsdirectory dataset to a csv file")

    logging.info("All done.")

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    duration = end - start
    logging.info("Took %f s", duration)