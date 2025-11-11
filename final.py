# final_fixed.py
# Fix: create aiohttp.TCPConnector inside async main() to avoid "no running event loop"

import asyncio
import ssl
import certifi
import json
import time
import hashlib
import logging
import os
import sys
from pathlib import Path
from urllib.parse import urlparse, urljoin
from collections import defaultdict
from typing import List, Dict, Any, Optional, TYPE_CHECKING, Iterable
import re

# Fix Windows console encoding issue with Crawl4AI rich logger
if sys.platform == "win32":
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr.reconfigure(encoding='utf-8')

import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from bs4 import BeautifulSoup

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
)

# Supabase imports
if TYPE_CHECKING:
    from supabase import Client

try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    Client = None  # Type stub for when not available
    logging.warning("supabase-py not installed. Products will not be saved to database")

# -------------------- CONFIG --------------------
# Output directory (configurable via env var, defaults to temp in production)
OUT_DIR_STR = os.getenv("OUT_DIR", "outFinal")
OUT_DIR = Path(OUT_DIR_STR)
# In production (Railway), optionally disable file saving or use temp directory
SAVE_HTML_FILES = os.getenv("SAVE_HTML_FILES", "false").lower() == "true"
if not SAVE_HTML_FILES:
    # Use temp directory in production
    import tempfile
    OUT_DIR = Path(tempfile.gettempdir()) / "crawler_output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH = OUT_DIR / "manifest.jsonl" if SAVE_HTML_FILES else None

# Concurrency settings (configurable via env vars)
GLOBAL_CONCURRENCY = int(os.getenv("GLOBAL_CONCURRENCY", "16"))
HEAVY_CONCURRENCY = int(os.getenv("HEAVY_CONCURRENCY", "4"))
PER_DOMAIN_LIMIT = int(os.getenv("PER_DOMAIN_LIMIT", "3"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
]

PROXIES = []  # optional proxies

HEAVY_EXPECTED_SELECTOR = (
    ".product, .product-card, .product-item, .search-result, ul > li, "
    "[data-hook='product-item'], [data-hook='product-list-grid-item'], "
    "[id^='comp-'] [class*='product'], main, body"
)

PAGE_TIMEOUT_MS = int(os.getenv("PAGE_TIMEOUT_MS", "45000"))
DELAY_AFTER_WAIT = float(os.getenv("DELAY_AFTER_WAIT", "2.0"))

API_RETRY_ATTEMPTS = int(os.getenv("API_RETRY_ATTEMPTS", "3"))

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://whfjofihihlhctizchmj.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6IndoZmpvZmloaWhsaGN0aXpjaG1qIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjEzNzQzNDMsImV4cCI6MjA3Njk1MDM0M30.OsJnOqeJgT5REPg7uxkGmmVcHIcs5QO4vdyDi66qpR0")

# Product extraction config
EXTRACT_PRODUCTS = os.getenv("EXTRACT_PRODUCTS", "true").lower() == "true"
MAX_PRODUCTS_PER_PAGE = int(os.getenv("MAX_PRODUCTS_PER_PAGE", "50"))
# -------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Prepare SSL context using certifi (do NOT create connector here)
_SSL_CTX = ssl.create_default_context(cafile=certifi.where())

# Supabase client (lazy initialization)
_supabase_client: Optional[Any] = None

def get_supabase_client() -> Optional[Any]:
    global _supabase_client
    if not SUPABASE_AVAILABLE or not SUPABASE_KEY:
        return None
    if _supabase_client is None:
        try:
            _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
            logging.info("[✓] Connected to Supabase for product storage")
        except Exception as exc:
            logging.warning(f"[!] Failed to initialize Supabase client: {exc}")
            _supabase_client = None
    return _supabase_client

# concurrency controls
domain_locks = defaultdict(lambda: asyncio.Semaphore(PER_DOMAIN_LIMIT))
global_sem = asyncio.Semaphore(GLOBAL_CONCURRENCY)
heavy_sem = asyncio.Semaphore(HEAVY_CONCURRENCY)

# -------------------- helpers --------------------

def safe_filename_for_url(url: str) -> str:
    p = urlparse(url)
    base = (p.netloc + p.path) or p.netloc or "page"
    safe = "".join(ch if (ch.isalnum() or ch in "._-") else "_" for ch in base)[:200]
    short_hash = hashlib.sha1(url.encode()).hexdigest()[:10]
    return f"{safe}_{short_hash}.html"

LD_JSON_RE = re.compile(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S)
INLINE_JSON_VAR_RE = re.compile(r'(?:(?:window|window\.)?[_A-Za-z0-9]+(?:State|INITIAL_DATA|INITIAL_STATE|__PRELOADED_STATE__|productsState|PRODUCTS_DATA) *= *({.*?});)', re.S)
XHR_ENDPOINT_RE = re.compile(r"""(?:(?:fetch\(|axios\.get|axios\(|XMLHttpRequest\().*?['"]([^'"]{10,200})['"])""", re.I)

# Wix-specific patterns
WIX_STORES_API_RE = re.compile(r'/_api/wix-ecommerce-renderer-web/store/products/query|/_api/catalog-reader-server/api/v1/products/query', re.I)
WIX_DATA_RE = re.compile(r'<script[^>]*data-url=[^>]*stores-reader[^>]*>(.*?)</script>', re.S)

def extract_ld_json(html: str):
    out = []
    for m in LD_JSON_RE.finditer(html):
        try:
            payload = json.loads(m.group(1))
            out.append(payload)
        except Exception:
            continue
    return out

def extract_inline_json(html: str):
    for m in INLINE_JSON_VAR_RE.finditer(html):
        txt = m.group(1)
        try:
            return json.loads(txt)
        except Exception:
            try:
                txt2 = re.sub(r',\s*([}\]])', r'\1', txt)
                return json.loads(txt2)
            except Exception:
                continue
    return None

def find_xhr_candidates(html: str, base_url: str):
    candidates = []
    
    # Standard XHR/fetch patterns
    for m in XHR_ENDPOINT_RE.finditer(html):
        uri = m.group(1)
        if uri.startswith("/"):
            uri = urljoin(base_url, uri)
        candidates.append(uri)
    
    # Wix-specific API endpoints
    if 'wix' in html.lower() or 'parastorage' in html.lower():
        # Common Wix e-commerce API patterns
        wix_apis = [
            '/_api/wix-ecommerce-renderer-web/store/products/query',
            '/_api/catalog-reader-server/api/v1/products/query',
            '/_api/wix-stores/v1/products/query',
        ]
        for api in wix_apis:
            full_url = urljoin(base_url, api)
            if full_url not in candidates:
                candidates.append(full_url)
    
    return candidates

def extract_wix_product_data(html: str) -> Optional[Dict]:
    """Extract Wix product data from script tags."""
    # Look for Wix viewer model or product data
    patterns = [
        r'window\.viewerModel\s*=\s*({.*?});',
        r'window\.wixapps\s*=\s*({.*?});',
        r'window\.productsState\s*=\s*({.*?});',
        r'__INITIAL_STATE__\s*=\s*({.*?});',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except Exception:
                continue
    return None

# -------------------- Product Extraction (HTML-based) --------------------

def _build_selector_sets() -> Dict[str, List[str]]:
    """Define comprehensive selectors for product cards and fields.
    Note: BeautifulSoup select() doesn't support case-insensitive flag,
    so we use multiple variations for case-insensitive matching.
    """
    return {
        "result_containers": [
            'ul.products', 'ul.product-list', 'ul.search-results',
            'div.products', 'div.product-list', 'div.search-results',
            'ul.results-base', 'div.results-base', 'div.results-items',
            'div[class*="listing"]', 'div[class*="Listing"]', 'div[class*="LISTING"]',
            'div[class*="product-grid"]', 'div[class*="ProductGrid"]',
            'div[data-component*="product"]', 'div[data-testid*="result"]',
            'section[class*="grid"]', 'section[class*="listing"]',
            'section[class*="catalog"]', 'div[class*="grid"]',
            'section[class*="product"]', 'section[class*="result"]', 'main',
            # Wix-specific selectors
            '[data-hook="product-list-wrapper"]', '[data-hook="product-item"]',
            '[data-hook="product-list-grid-item"]', '[id^="comp-"][class*="product"]',
            'div[data-hook="product-list-gallery"]', 'article[data-hook="product-item"]',
        ],
        "product_cards": [
            '[data-component="product"]', '[data-qa*="product"]',
            '[data-testid*="product"]', '[data-cy*="product"]',
            '[itemscope][itemtype*="Product"]',
            'div[data-product-id]', 'article[data-product-id]',
            'div[data-asin]', 'li[data-asin]', 'li[data-id*="product"]',
            'div[data-testid*="product-card"]', 'li[class*="product"]',
            'li[class*="Product"]', 'li[class*="grid"]',
            'div[class*="product"]', 'div[class*="Product"]',
            'div[class*="item"]', 'div[class*="Item"]',
            'div[class*="card"]', 'div[class*="Card"]',
            'div[class*="result"]', 'article[class*="product"]',
            'article[class*="item"]',
            'li.product-base', 'li.product-item',
            # Wix-specific product card selectors
            '[data-hook="product-item"]', '[data-hook="product-list-grid-item"]',
            '[id^="comp-"][class*="product"]', 'article[data-hook="product-item"]',
            'div[data-hook="product-item-container"]', 'li[data-hook="product-list-grid-item"]',
        ],
        "title": [
            '[itemprop="name"]', 'a[title]', 'a[class*="title"]',
            'a[class*="Title"]', 'a[data-testid*="title"]',
            'h1', 'h2', 'h3', 'h4',
            '[class*="title"]', '[class*="Title"]',
            '[class*="name"]', '[class*="Name"]',
            '[aria-label*="product"]',
            # Wix-specific
            '[data-hook="product-item-name"]', '[data-hook="product-item-title"]',
            'h3[data-hook="product-item-name"]',
        ],
        "link": [
            'a[href*="/product"]', 'a[href*="/item"]', 'a[href*="/p/"]',
            'a[href*="?pid="]', 'a[data-testid*="product"]',
            'a[data-track*="product"]', 'a[href]', '[itemprop="url"]',
            # Wix-specific
            '[data-hook="product-item-link"]', 'a[data-hook="product-item-container"]',
        ],
        "image": [
            'img[src]', 'img[data-src]', 'img[data-original]',
            'img[data-lazy-src]', 'img[data-srcset]', 'source[data-srcset]',
            '[data-background-image]', '[itemprop="image"]',
            # Wix-specific
            'img[data-hook="product-item-image"]', '[data-hook="product-item-img"]',
            'div[data-hook="product-item-media"] img',
        ],
        "price": [
            '[itemprop="price"]', '[class*="price"]', '[class*="Price"]',
            '[class*="offer"]', '[data-price]', 'span[data-price]',
            'div[data-price]', 'span[class*="amount"]', 'span[class*="value"]',
            'meta[itemprop="price"][content]',
            # Wix-specific
            '[data-hook="product-item-price"]', '[data-hook="formatted-primary-price"]',
            'span[data-hook="product-item-price-to-pay"]',
        ],
        "currency": [
            'meta[itemprop="priceCurrency"][content]',
            '[class*="currency"]', 'span[data-currency]',
        ],
        "rating": [
            '[itemprop="ratingValue"]', '[class*="rating"]',
            '[class*="Rating"]', '[aria-label*="rating"]',
        ],
        "reviews": [
            '[itemprop="reviewCount"]', '[class*="review"]',
            '[class*="Review"]', '[aria-label*="review"]',
        ],
        "brand": [
            '[itemprop="brand"]', '[class*="brand"]', '[class*="Brand"]',
            '[data-brand]',
        ],
        "sku": [
            '[itemprop="sku"]', '[data-sku]', '[data-product-sku]',
            '[class*="sku"]', '[class*="Sku"]',
        ],
    }

def _clean_text(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned or None

def _parse_price(raw: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    if not raw:
        return None, None
    txt = raw.strip()
    currency = None
    lowered = txt.lower()
    if any(sym in lowered for sym in ["₹", "rs", "rs.", "inr"]):
        currency = "INR"
    elif "$" in txt or "usd" in lowered:
        currency = "USD"
    elif "€" in txt or "eur" in lowered:
        currency = "EUR"
    elif "£" in txt or "gbp" in lowered:
        currency = "GBP"
    num_match = re.findall(r"[\d,.]+", txt)
    if not num_match:
        return None, currency
    num = num_match[0].replace(",", "")
    try:
        return float(num), currency
    except Exception:
        return None, currency

def _parse_float(raw: Optional[str]) -> Optional[float]:
    if not raw:
        return None
    m = re.findall(r"[\d.]+", str(raw))
    if not m:
        return None
    try:
        return float(m[0])
    except Exception:
        return None

def _parse_int(raw: Optional[str]) -> Optional[int]:
    if not raw:
        return None
    m = re.findall(r"\d+", str(raw))
    if not m:
        return None
    try:
        return int(m[0])
    except Exception:
        return None

def _is_blacklisted_link(href: str) -> bool:
    if not href:
        return True
    h = href.lower()
    blacklist = ['login', 'register', 'signup', 'account', 'cart', 'wishlist',
                 'checkout', 'help', 'faq', 'contact', 'privacy', 'terms']
    if any(h.startswith(prefix) for prefix in ('javascript:', 'mailto:', 'tel:')):
        return True
    return any(keyword in h for keyword in blacklist)

def _is_product_like_path(href: str) -> bool:
    try:
        parsed = urlparse(href)
        path = (parsed.path or '').lower()
        query = (parsed.query or '').lower()
        combined = f"{path}?{query}"
        product_keywords = [
            '/product', '/products', '/item', '/items', '/p/', '/dp/', '/pd/', '/pdp',
            '/shop/', '/store/', '/catalog', '/collection', '/listing', '/detail', '/sku',
        ]
        if any(kw in combined for kw in product_keywords):
            return True
        negative = ['search', 'account', 'contact', 'login', 'register']
        if any(neg in combined for neg in negative):
            return False
        if path.count('/') >= 2 and len(path) > 3:
            return True
        if any(char.isdigit() for char in path.split('/')[-1:]):
            return True
        return False
    except Exception:
        return False

def _is_valid_product(product: Dict[str, Any], base_url: str) -> bool:
    url = product.get('product_url')
    title = _clean_text(product.get('title'))
    if not url:
        return False
    if _is_blacklisted_link(url):
        return False
    if not _is_product_like_path(url) and not (product.get('price') and title):
        return False
    if title and len(title) < 2:
        return False
    if not title and not product.get('price'):
        return False
    return True

def _extract_products_from_json(data: Any, base_url: str, max_items: int = MAX_PRODUCTS_PER_PAGE, depth: int = 0) -> List[Dict[str, Any]]:
    """Extract products from JSON API response."""
    products = []
    if depth > 5:  # Prevent infinite recursion
        return products
    
    try:
        if isinstance(data, list):
            for item in data:
                products.extend(_extract_products_from_json(item, base_url, max_items, depth + 1))
                if len(products) >= max_items:
                    break
        elif isinstance(data, dict):
            # Handle Wix API response structure
            if 'products' in data and isinstance(data['products'], list):
                for wix_product in data['products']:
                    extracted = _extract_wix_product(wix_product, base_url)
                    if extracted:
                        products.append(extracted)
                        if len(products) >= max_items:
                            return products[:max_items]
            
            # Handle schema.org ListItem that wraps product data
            data_type = str(data.get('@type') or data.get('type') or '').lower()
            if data_type == 'listitem' and isinstance(data.get('item'), dict):
                inner = dict(data['item'])
                inner.setdefault('name', data.get('name'))
                inner.setdefault('url', data.get('url') or inner.get('@id'))
                products.extend(_extract_products_from_json(inner, base_url, max_items, depth + 1))
                if len(products) >= max_items:
                    return products[:max_items]

            if data_type == 'product':
                image = data.get('image')
                if isinstance(image, list) and image:
                    image = image[0]
                if isinstance(image, dict):
                    image = image.get('url') or image.get('src')
                candidate = {
                    'title': data.get('name') or data.get('title'),
                    'product_url': urljoin(base_url, data.get('url') or data.get('@id') or ''),
                    'image_url': urljoin(base_url, image or ''),
                    'price': None,
                    'currency': None,
                    'raw_price': None,
                    'rating': None,
                    'review_count': None,
                    'brand': None,
                    'sku': None,
                }
                offers = data.get('offers')
                if isinstance(offers, list) and offers:
                    offers = offers[0]
                if isinstance(offers, dict):
                    candidate['raw_price'] = str(offers.get('price') or '')
                    candidate['price'], candidate['currency'] = _parse_price(candidate['raw_price'])
                    candidate['currency'] = candidate['currency'] or offers.get('priceCurrency')
                    candidate['availability'] = offers.get('availability')
                agg_rating = data.get('aggregateRating')
                if isinstance(agg_rating, dict):
                    candidate['rating'] = _parse_float(agg_rating.get('ratingValue'))
                    candidate['review_count'] = _parse_int(agg_rating.get('reviewCount'))
                brand = data.get('brand')
                if isinstance(brand, dict):
                    brand = brand.get('name') or brand.get('brand')
                candidate['brand'] = _clean_text(brand if isinstance(brand, str) else data.get('brand'))
                candidate['sku'] = _clean_text(data.get('sku'))
                if _is_valid_product(candidate, base_url):
                    products.append(candidate)
                    if len(products) >= max_items:
                        return products[:max_items]

            # Check if this dict looks like a product
            has_product_fields = any(key in data for key in ['name', 'title', 'productName', 'product_name', 'productUrl', 'product_url', 'price', 'image', 'imageUrl'])
            if has_product_fields:
                image = data.get('image') or data.get('imageUrl') or data.get('image_url')
                if isinstance(image, list) and image:
                    image = image[0]
                if isinstance(image, dict):
                    image = image.get('url') or image.get('src')
                product = {
                    'title': data.get('name') or data.get('title') or data.get('productName') or data.get('product_name'),
                    'product_url': urljoin(base_url, data.get('url') or data.get('productUrl') or data.get('product_url') or ''),
                    'image_url': urljoin(base_url, image or ''),
                    'price': _parse_price(str(data.get('price') or data.get('salePrice') or ''))[0],
                    'currency': data.get('currency') or data.get('priceCurrency'),
                    'raw_price': str(data.get('price') or data.get('salePrice') or ''),
                    'rating': _parse_float(str(data.get('rating') or data.get('ratingValue') or '')),
                    'review_count': _parse_int(str(data.get('reviewCount') or data.get('reviewsCount') or '')),
                    'brand': _clean_text(data.get('brand') or data.get('manufacturer')),
                    'sku': _clean_text(data.get('sku') or data.get('productId') or data.get('id')),
                }
                if _is_valid_product(product, base_url):
                    products.append(product)
                    if len(products) >= max_items:
                        return products
            
            # Recursively search nested structures
            for key, value in data.items():
                key_lower = str(key).lower()
                if any(k in key_lower for k in ['product', 'products', 'item', 'items', 'listing', 'result', 'entries', 'hits']):
                    products.extend(_extract_products_from_json(value, base_url, max_items, depth + 1))
                    if len(products) >= max_items:
                        break
    except Exception:
        pass
    
    return products[:max_items]

def _extract_wix_product(wix_data: Dict, base_url: str) -> Optional[Dict[str, Any]]:
    """Extract product from Wix API response format."""
    try:
        # Wix products have specific structure
        product_id = wix_data.get('id') or wix_data.get('productId')
        name = wix_data.get('name')
        slug = wix_data.get('slug') or wix_data.get('urlPart')
        
        # Build product URL
        product_url = None
        if slug:
            product_url = urljoin(base_url, f'/product-page/{slug}')
        elif product_id:
            product_url = urljoin(base_url, f'/product/{product_id}')
        
        # Get price
        price_data = wix_data.get('price') or wix_data.get('priceData')
        raw_price = None
        parsed_price = None
        currency = None
        
        if isinstance(price_data, dict):
            raw_price = str(price_data.get('price') or price_data.get('formatted') or '')
            parsed_price = float(price_data.get('price', 0)) if price_data.get('price') else None
            currency = price_data.get('currency')
        elif isinstance(price_data, (int, float)):
            parsed_price = float(price_data)
            raw_price = str(price_data)
        
        # Get image
        media = wix_data.get('media') or wix_data.get('images')
        image_url = None
        if isinstance(media, list) and media:
            first_media = media[0]
            if isinstance(first_media, dict):
                image_url = first_media.get('url') or first_media.get('src')
            elif isinstance(first_media, str):
                image_url = first_media
        elif isinstance(media, dict):
            image_url = media.get('mainMedia', {}).get('url') or media.get('url')
        
        if image_url and not image_url.startswith('http'):
            image_url = urljoin(base_url, image_url)
        
        product = {
            'title': name,
            'product_url': product_url,
            'image_url': image_url,
            'price': parsed_price,
            'currency': currency or 'INR',
            'raw_price': raw_price,
            'rating': None,
            'review_count': None,
            'brand': wix_data.get('brand'),
            'sku': wix_data.get('sku') or product_id,
        }
        
        if _is_valid_product(product, base_url):
            return product
    except Exception:
        pass
    return None

def _dedupe_products(products: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for product in products:
        url = product.get('product_url')
        title = _clean_text(product.get('title'))
        key = (url or '').strip() or (title or '')
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(product)
    return deduped

def extract_products_from_sources(
    html: Optional[str],
    ld_sources: Optional[List[Any]],
    inline_source: Optional[Any],
    base_url: str,
    max_items: int = MAX_PRODUCTS_PER_PAGE,
) -> List[Dict[str, Any]]:
    """Collect products from any combination of HTML and JSON sources."""
    if not EXTRACT_PRODUCTS:
        return []

    candidates: List[Dict[str, Any]] = []

    if html:
        try:
            candidates.extend(extract_products_from_html(html, base_url, max_items))
        except Exception:
            logging.debug("HTML extraction failed", exc_info=True)

    for block in ld_sources or []:
        try:
            candidates.extend(_extract_products_from_json(block, base_url, max_items))
        except Exception:
            logging.debug("LD JSON extraction failed", exc_info=True)

    if inline_source:
        try:
            candidates.extend(_extract_products_from_json(inline_source, base_url, max_items))
        except Exception:
            logging.debug("Inline JSON extraction failed", exc_info=True)

    if not candidates:
        return []

    return _dedupe_products(candidates)[:max_items]

def extract_products_from_html(html: str, base_url: str, max_items: int = MAX_PRODUCTS_PER_PAGE) -> List[Dict[str, Any]]:
    """Extract products from HTML using BeautifulSoup."""
    if not html or len(html) < 500:
        return []
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
    except Exception:
        return []
    
    selector_sets = _build_selector_sets()
    products: List[Dict[str, Any]] = []
    
    # Find result containers
    containers = []
    for sel in selector_sets["result_containers"]:
        try:
            els = soup.select(sel)
            if els:
                containers = els
                break
        except Exception:
            continue
    
    # Find product cards
    cards = []
    if containers:
        for container in containers:
            for sel in selector_sets["product_cards"]:
                try:
                    els = container.select(sel)
                    cards.extend(els)
                except Exception:
                    continue
    else:
        for sel in selector_sets["product_cards"]:
            try:
                els = soup.select(sel)
                if els:
                    cards.extend(els)
            except Exception:
                continue
    
    # Extract fields from each card
    for card in cards[:max_items * 2]:  # Check more than needed for validation
        try:
            product = {}
            
            # Title
            title = None
            for sel in selector_sets["title"]:
                try:
                    el = card.select_one(sel)
                    if el:
                        title = el.get('content') or el.get('title') or el.get('aria-label') or el.get_text(strip=True)
                        title = _clean_text(title)
                        if title:
                            break
                except Exception:
                    continue
            if not title:
                # Try link title
                try:
                    a = card.select_one('a[href]')
                    if a:
                        title = _clean_text(a.get('title') or a.get_text(strip=True))
                except Exception:
                    pass
            product['title'] = title
            
            # Link
            link_href = None
            for sel in selector_sets["link"]:
                try:
                    el = card.select_one(sel)
                    if el:
                        link_href = el.get('href') or el.get('content')
                        if link_href:
                            break
                except Exception:
                    continue
            product['product_url'] = urljoin(base_url, link_href) if link_href else None
            
            # Image
            image_src = None
            for sel in selector_sets["image"]:
                try:
                    el = card.select_one(sel)
                    if el:
                        image_src = (el.get('src') or el.get('data-src') or 
                                    el.get('data-original') or el.get('data-srcset') or
                                    el.get('content'))
                        if image_src:
                            break
                except Exception:
                    continue
            product['image_url'] = urljoin(base_url, image_src) if image_src else None
            
            # Price
            raw_price = None
            for sel in selector_sets["price"]:
                try:
                    el = card.select_one(sel)
                    if el:
                        raw_price = el.get('content') or el.get_text(strip=True)
                        raw_price = _clean_text(raw_price)
                        if raw_price:
                            break
                except Exception:
                    continue
            parsed_price, currency = _parse_price(raw_price)
            product['price'] = parsed_price
            product['currency'] = currency
            product['raw_price'] = raw_price
            
            # Rating
            rating_text = None
            for sel in selector_sets["rating"]:
                try:
                    el = card.select_one(sel)
                    if el:
                        rating_text = el.get('content') or el.get_text(strip=True)
                        if rating_text:
                            break
                except Exception:
                    continue
            product['rating'] = _parse_float(rating_text)
            
            # Reviews
            review_text = None
            for sel in selector_sets["reviews"]:
                try:
                    el = card.select_one(sel)
                    if el:
                        review_text = el.get_text(strip=True)
                        if review_text:
                            break
                except Exception:
                    continue
            product['review_count'] = _parse_int(review_text)
            
            # Brand
            brand = None
            for sel in selector_sets["brand"]:
                try:
                    el = card.select_one(sel)
                    if el:
                        brand = el.get('content') or el.get('data-brand') or el.get_text(strip=True)
                        brand = _clean_text(brand)
                        if brand:
                            break
                except Exception:
                    continue
            product['brand'] = brand
            
            # SKU
            sku = None
            for sel in selector_sets["sku"]:
                try:
                    el = card.select_one(sel)
                    if el:
                        sku = el.get('content') or el.get('data-sku') or el.get('data-product-sku') or el.get_text(strip=True)
                        sku = _clean_text(sku)
                        if sku:
                            break
                except Exception:
                    continue
            product['sku'] = sku
            
            # Validate and add
            if _is_valid_product(product, base_url):
                products.append(product)
                if len(products) >= max_items:
                    break
        except Exception:
            continue
    
    # Deduplicate by URL
    seen_urls = set()
    unique_products = []
    for p in products:
        url = p.get('product_url')
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_products.append(p)
    
    return unique_products[:max_items]

def _sanitize_text(value: Optional[str], max_length: int = 2000) -> Optional[str]:
    """Sanitize and truncate text values."""
    if not value:
        return None
    if isinstance(value, str):
        # Remove null bytes and control characters
        value = value.replace('\x00', '').replace('\r', ' ').replace('\n', ' ')
        # Truncate if too long
        if len(value) > max_length:
            value = value[:max_length]
        return value.strip() or None
    return str(value)[:max_length] if value else None

def _sanitize_url(value: Optional[str]) -> Optional[str]:
    """Sanitize URL values."""
    if not value:
        return None
    url = str(value).strip()
    if len(url) > 2048:  # URL max length
        url = url[:2048]
    return url or None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=0.5, max=2), retry=retry_if_exception_type(Exception))
def _insert_product_with_retry(client, db_data: Dict[str, Any]) -> bool:
    """Insert a single product with retry logic."""
    try:
        response = client.table("r_product_data").insert(db_data).execute()
        return bool(response.data)
    except Exception as e:
        error_msg = str(e).lower()
        # Don't retry on duplicate/unique constraint errors
        if "duplicate" in error_msg or "unique" in error_msg or "violates unique constraint" in error_msg:
            return True  # Count as success
        # Don't retry on validation errors
        if "violates check constraint" in error_msg or "invalid input" in error_msg:
            logging.debug(f"[DB] Validation error (skipping): {error_msg[:200]}")
            return False
        # Retry on server errors
        raise

def save_products_to_supabase(products: List[Dict[str, Any]], platform_url: str, platform: str, product_type_id: Optional[int] = None) -> int:
    """Save products to Supabase database with improved error handling."""
    client = get_supabase_client()
    if not client or not products:
        return 0
    
    saved_count = 0
    failed_count = 0
    error_details = []
    
    for idx, product in enumerate(products):
        try:
            # Validate and sanitize rating
            rating = product.get("rating")
            if rating is not None:
                try:
                    rating_float = float(rating)
                    if rating_float < 0:
                        rating = 0.0
                    elif rating_float > 100:
                        rating = 100.0
                    else:
                        rating = round(rating_float, 2)
                except (ValueError, TypeError):
                    rating = None
            
            # Validate and sanitize price
            price = product.get("price")
            if price is not None:
                try:
                    price_float = float(price)
                    if price_float < 0:
                        price = None
                    elif price_float > 999999999.99:
                        price = 999999999.99
                    else:
                        price = round(price_float, 2)
                except (ValueError, TypeError):
                    price = None
            
            # Validate and sanitize reviews
            reviews = product.get("review_count")
            if reviews is not None:
                try:
                    reviews_int = int(float(reviews))
                    if reviews_int < 0:
                        reviews = None
                    elif reviews_int > 2147483647:  # Max int32
                        reviews = None
                    else:
                        reviews = reviews_int
                except (ValueError, TypeError, OverflowError):
                    reviews = None
            
            # Sanitize text fields
            product_name = _sanitize_text(product.get("title"), max_length=500)
            product_url = _sanitize_url(product.get("product_url"))
            image_url = _sanitize_url(product.get("image_url"))
            original_price = _sanitize_text(product.get("raw_price"), max_length=100)
            brand = _sanitize_text(product.get("brand"), max_length=200)
            platform_url_safe = _sanitize_url(platform_url)
            
            # Skip if essential fields are missing
            if not product_name or not product_url:
                failed_count += 1
                continue
            
            db_data = {
                "platform_url": platform_url_safe,
                "product_name": product_name,
                "original_price": original_price,
                "current_price": price,
                "product_url": product_url,
                "product_image_url": image_url,
                "rating": rating,
                "reviews": reviews,
                "brand": brand,
            }
            
            # Add product_type_id if provided
            if product_type_id is not None:
                db_data["product_type_id"] = int(product_type_id)
            
            # Try to insert with retry
            try:
                if _insert_product_with_retry(client, db_data):
                    saved_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                error_msg = str(e)[:200]
                if "500" not in error_msg:  # Don't log 500 errors repeatedly
                    error_details.append(f"Product {idx+1}: {error_msg}")
                
        except Exception as e:
            failed_count += 1
            error_msg = str(e)[:200]
            error_details.append(f"Product {idx+1} validation: {error_msg}")
            continue
    
    # Log results
    if saved_count > 0:
        log_msg = f"[DB] Saved {saved_count}/{len(products)} products to Supabase"
        if failed_count > 0:
            log_msg += f" ({failed_count} failed)"
        logging.info(log_msg)
        
        # Log first few errors for debugging
        if error_details and len(error_details) <= 5:
            for err in error_details:
                logging.debug(f"[DB] Error: {err}")
        elif error_details:
            logging.debug(f"[DB] {len(error_details)} errors occurred (first: {error_details[0]})")
    
    return saved_count

def fetch_pending_urls_from_db(limit: int = 100, worker_id: str = None) -> List[Dict[str, Any]]:
    """Fetch pending URLs from product_page_urls table.
    
    Also resets stuck URLs that have been in 'processing' status for too long (>30 minutes).
    This handles cases where the script was interrupted and URLs were left in 'processing' status.
    """
    client = get_supabase_client()
    if not client:
        return []
    
    try:
        from datetime import datetime, timezone, timedelta
        
        # First, reset stuck URLs that have been processing for more than 30 minutes
        # This handles interruptions where URLs were left in 'processing' status
        stuck_threshold = datetime.now(timezone.utc) - timedelta(minutes=30)
        try:
            stuck_reset = client.table("product_page_urls").update({
                "processing_status": "pending",
                "claimed_by": None,
                "claimed_at": None
            }).eq("processing_status", "processing").lt("claimed_at", stuck_threshold.isoformat()).execute()
            
            if stuck_reset.data:
                logging.info(f"[DB] Reset {len(stuck_reset.data)} stuck URLs from 'processing' to 'pending'")
        except Exception as e:
            logging.warning(f"[DB] Could not reset stuck URLs: {e}")
        
        # Claim URLs by updating claimed_by and claimed_at
        query = client.table("product_page_urls").select("id, product_type_id, product_page_url").eq("processing_status", "pending").limit(limit)
        
        # Fetch URLs
        response = query.execute()
        urls = response.data if response.data else []
        
        # Claim them by updating status to processing
        if urls and worker_id:
            url_ids = [url["id"] for url in urls]
            for url_id in url_ids:
                try:
                    client.table("product_page_urls").update({
                        "processing_status": "processing",
                        "claimed_by": worker_id,
                        "claimed_at": datetime.now(timezone.utc).isoformat()
                    }).eq("id", url_id).execute()
                except Exception:
                    pass
        
        return urls
    except Exception as e:
        logging.error(f"[DB] Error fetching pending URLs: {e}")
        return []

def update_url_processing_result(
    url_id: int,
    success: bool,
    products_found: int,
    products_saved: int,
    error_message: Optional[str] = None
):
    """Update product_page_urls table with processing results."""
    client = get_supabase_client()
    if not client:
        return
    
    try:
        from datetime import datetime, timezone
        update_data = {
            "processing_status": "completed" if success else "failed",
            "success": success,
            "products_found": products_found,
            "products_saved": products_saved,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
        
        if error_message:
            update_data["error_message"] = error_message[:500]  # Limit error message length
        
        client.table("product_page_urls").update(update_data).eq("id", url_id).execute()
    except Exception as e:
        logging.error(f"[DB] Error updating URL result for id {url_id}: {e}")

@retry(stop=stop_after_attempt(API_RETRY_ATTEMPTS), wait=wait_exponential(min=1, max=8), retry=retry_if_exception_type(Exception))
async def fetch_json_api(session: aiohttp.ClientSession, api_url: str, referer: str = None):
    headers = {
        "User-Agent": USER_AGENTS[0], 
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json",
    }
    if referer:
        headers["Referer"] = referer
    
    timeout = aiohttp.ClientTimeout(total=15)
    proxy = PROXIES[hash(api_url) % len(PROXIES)] if PROXIES else None
    
    # Check if it's a Wix API endpoint
    is_wix = 'wix' in api_url or 'catalog-reader' in api_url
    
    if is_wix:
        # Wix APIs typically require POST with specific payload
        payload = {
            "includeVariants": True,
            "onlyVisible": True,
            "withOptions": True,
            "includeHiddenProducts": False,
            "paging": {"limit": 50, "offset": 0}
        }
        try:
            async with session.post(api_url, headers=headers, json=payload, timeout=timeout, proxy=proxy) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception:
            pass
    
    # Fallback to GET request
    try:
        async with session.get(api_url, headers=headers, timeout=timeout, proxy=proxy) as resp:
            txt = await resp.text(errors="ignore")
            try:
                return json.loads(txt)
            except Exception:
                j = re.sub(r'^[^(]*\(\s*', '', txt)
                j = re.sub(r'\)\s*;?$', '', j)
                return json.loads(j)
    except Exception:
        return None

# -------------------- network/fast path --------------------

async def fetch_html(session: aiohttp.ClientSession, url: str):
    headers = {"User-Agent": USER_AGENTS[0]}
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    proxy = PROXIES[hash(url) % len(PROXIES)] if PROXIES else None
    async with session.get(url, headers=headers, timeout=timeout, proxy=proxy) as resp:
        text = await resp.text(errors="ignore")
        return resp.status, text, str(resp.url)

# -------------------- heavy render via Crawl4AI --------------------

async def heavy_render(url: str, expected_selector: str = None):
    async with heavy_sem:
        browser_conf = BrowserConfig(headless=True)
        
        # Detect if it's a Wix site and adjust js_code accordingly
        is_wix = 'wix' in url.lower()
        
        js_scroll = [
            # Slow progressive scroll
            "(()=>{window.scrollTo({top: document.body.scrollHeight/4, behavior: 'smooth'});return true})()",
            "(()=>{window.scrollTo({top: document.body.scrollHeight/2, behavior: 'smooth'});return true})()",
            "(()=>{window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'});return true})()",
        ]
        
        if is_wix:
            # Additional Wix-specific JavaScript to trigger lazy loading
            js_scroll.extend([
                # Trigger Wix lazy load events
                "(()=>{window.dispatchEvent(new Event('scroll'));return true})()",
                "(()=>{window.dispatchEvent(new Event('resize'));return true})()",
            ])
        
        # Clean up selector - remove empty selectors that cause timeout errors
        clean_selector = None
        if expected_selector:
            # Remove empty selectors (caused by trailing commas)
            parts = [s.strip() for s in expected_selector.split(',') if s.strip()]
            if parts:
                clean_selector = ', '.join(parts)
        
        # Try with wait_for first, but fallback to no wait if it fails
        run_conf = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            stream=False,
            wait_for=(f"css:{clean_selector}" if clean_selector else None),
            js_code=js_scroll,
            page_timeout=PAGE_TIMEOUT_MS,
            delay_before_return_html=DELAY_AFTER_WAIT,
            scan_full_page=True,  # Enable full page scan for Wix
            wait_for_images=True,  # Wait for images on Wix sites
            scroll_delay=0.5,  # Slower scroll for dynamic content
        )
        async with AsyncWebCrawler(config=browser_conf) as crawler:
            try:
                res = await crawler.arun(url, config=run_conf)
                html = getattr(res, "html", "") or getattr(res, "content", "") or ""
                screenshot = getattr(res, "screenshot", None)
                return res, html, screenshot
            except Exception as e:
                error_msg = str(e)
                # If timeout on selector, try again without wait_for
                if "timeout" in error_msg.lower() or "wait condition failed" in error_msg.lower():
                    logging.debug(f"[HEAVY] Selector timeout for {url}, retrying without wait condition")
                    try:
                        # Retry without wait_for - just wait for page load
                        fallback_conf = CrawlerRunConfig(
                            cache_mode=CacheMode.BYPASS,
                            stream=False,
                            wait_for=None,  # No selector wait
                            js_code=js_scroll,
                            page_timeout=PAGE_TIMEOUT_MS,
                            delay_before_return_html=DELAY_AFTER_WAIT,
                            scan_full_page=True,
                            wait_for_images=False,  # Don't wait for images on retry
                            scroll_delay=0.5,
                        )
                        res = await crawler.arun(url, config=fallback_conf)
                        html = getattr(res, "html", "") or getattr(res, "content", "") or ""
                        screenshot = getattr(res, "screenshot", None)
                        return res, html, screenshot
                    except Exception as e2:
                        logging.debug(f"[HEAVY] Fallback also failed for {url}: {str(e2)[:100]}")
                        return None, "", None
                else:
                    logging.debug(f"[HEAVY] Error for {url}: {error_msg[:100]}")
                    return None, "", None

# -------------------- pipeline worker --------------------

async def process_url(
    url: str, 
    session: aiohttp.ClientSession, 
    manifest_fh,
    url_id: Optional[int] = None,
    product_type_id: Optional[int] = None
):
    parsed = urlparse(url)
    domain = parsed.netloc or "unknown"
    async with domain_locks[domain]:
        async with global_sem:
            ts0 = time.time()
            status = {"url": url, "start": ts0, "url_id": url_id, "product_type_id": product_type_id}
            products_found = 0
            products_saved = 0
            success = False
            error_msg = None
            
            try:
                st, html, final_url = await fetch_html(session, url)
                status["http_status"] = st
                status["final_url"] = final_url

                ld = extract_ld_json(html)
                inline = extract_inline_json(html)
                if ld or inline:
                    if SAVE_HTML_FILES:
                        save_dir = OUT_DIR / domain
                        save_dir.mkdir(parents=True, exist_ok=True)
                        fname = safe_filename_for_url(url)
                        path = save_dir / fname
                        payload = {"ld": ld, "inline": inline}
                        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                    else:
                        path = None

                    products = extract_products_from_sources(html, ld, inline, final_url)
                    products_found = len(products)
                    if products:
                        saved_count = save_products_to_supabase(products, final_url, domain, product_type_id)
                        products_saved = saved_count
                        status["products_extracted"] = products_found
                        status["products_saved"] = products_saved
                    
                    success = products_found > 0
                    status.update({"stage": "fast-json", "ok": success, "path": str(path) if path else None, "elapsed": time.time()-ts0})
                    if manifest_fh:
                        manifest_fh.write(json.dumps(status, default=str) + "\n")
                        manifest_fh.flush()
                    logging.info(f"[FAST] {url} -> {path if path else 'N/A'} ({products_found} products, {products_saved} saved)")
                    
                    # Update database
                    if url_id:
                        update_url_processing_result(url_id, success, products_found, products_saved, error_msg)
                    return

                candidates = find_xhr_candidates(html, final_url)
                if candidates:
                    for c in candidates:
                        try:
                            api_json = await fetch_json_api(session, c, referer=final_url)
                            if api_json:
                                if SAVE_HTML_FILES:
                                    save_dir = OUT_DIR / domain
                                    save_dir.mkdir(parents=True, exist_ok=True)
                                    fname = safe_filename_for_url(url)
                                    path = save_dir / fname
                                    path.write_text(json.dumps({"api": api_json}, ensure_ascii=False), encoding="utf-8")
                                else:
                                    path = None
                                
                                # Try to extract products from API JSON response
                                products = _dedupe_products(_extract_products_from_json(api_json, final_url))
                                products_found = len(products)
                                if products:
                                    saved_count = save_products_to_supabase(products, final_url, domain, product_type_id)
                                    products_saved = saved_count
                                    status["products_extracted"] = products_found
                                    status["products_saved"] = products_saved
                                
                                success = products_found > 0
                                status.update({"stage": "fast-api", "api": c, "ok": success, "path": str(path) if path else None, "elapsed": time.time()-ts0})
                                if manifest_fh:
                                    manifest_fh.write(json.dumps(status, default=str) + "\n")
                                    manifest_fh.flush()
                                logging.info(f"[API] {url} -> {path if path else 'N/A'} via {c} ({products_found} products, {products_saved} saved)")
                                
                                # Update database
                                if url_id:
                                    update_url_processing_result(url_id, success, products_found, products_saved, error_msg)
                                return
                        except Exception:
                            logging.debug(f"API fetch failed for {c}", exc_info=True)
                            continue

                expected_selector = HEAVY_EXPECTED_SELECTOR
                res, rendered_html, screenshot = await heavy_render(url, expected_selector=expected_selector)
                if res and getattr(res, "success", False) and rendered_html and len(rendered_html) > 1500:
                    path = None
                    if SAVE_HTML_FILES:
                        save_dir = OUT_DIR / domain
                        save_dir.mkdir(parents=True, exist_ok=True)
                        fname = safe_filename_for_url(url)
                        path = save_dir / fname
                        path.write_text(rendered_html, encoding="utf-8")
                        if screenshot:
                            try:
                                import base64
                                ssb = screenshot
                                if isinstance(ssb, str):
                                    ssb = base64.b64decode(ssb)
                                (path.with_suffix(".png")).write_bytes(ssb)
                            except Exception:
                                pass
                    
                    rendered_ld = extract_ld_json(rendered_html)
                    rendered_inline = extract_inline_json(rendered_html)
                    products = extract_products_from_sources(rendered_html, rendered_ld, rendered_inline, final_url)
                    products_found = len(products)
                    if products:
                        saved_count = save_products_to_supabase(products, final_url, domain, product_type_id)
                        products_saved = saved_count
                        status["products_extracted"] = products_found
                        status["products_saved"] = products_saved
                    
                    success = products_found > 0
                    path_str = str(path) if path else None
                    status.update({"stage": "heavy", "ok": success, "path": path_str, "elapsed": time.time()-ts0})
                    if manifest_fh:
                        manifest_fh.write(json.dumps(status, default=str) + "\n")
                        manifest_fh.flush()
                    logging.info(f"[HEAVY] {url} -> {path_str if path_str else 'N/A'} ({products_found} products, {products_saved} saved)")
                    
                    # Update database
                    if url_id:
                        update_url_processing_result(url_id, success, products_found, products_saved, error_msg)
                    return
                else:
                    if SAVE_HTML_FILES:
                        save_dir = OUT_DIR / domain
                        save_dir.mkdir(parents=True, exist_ok=True)
                        fname = safe_filename_for_url(url)
                        (save_dir / ("failed_" + fname)).write_text(html[:4000], encoding="utf-8")
                    error_msg = "no content or render failed"
                    success = False
                    status.update({"stage": "failed", "ok": False, "err": error_msg, "elapsed": time.time()-ts0})
                    if manifest_fh:
                        manifest_fh.write(json.dumps(status, default=str) + "\n")
                        manifest_fh.flush()
                    logging.warning(f"[FAIL] {url} ({error_msg})")
                    
                    # Update database
                    if url_id:
                        update_url_processing_result(url_id, success, products_found, products_saved, error_msg)
                    return

            except Exception as e:
                error_msg = str(e)[:500]
                success = False
                status.update({"stage": "exception", "ok": False, "err": error_msg, "elapsed": time.time()-ts0})
                manifest_fh.write(json.dumps(status, default=str) + "\n")
                manifest_fh.flush()
                logging.exception("process_url exception")
                
                # Update database
                if url_id:
                    update_url_processing_result(url_id, success, products_found, products_saved, error_msg)
                return

# -------------------- main --------------------

async def main_from_db(batch_size: int = 100, max_batches: Optional[int] = None):
    """Main function that processes URLs from database."""
    import socket
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    logging.info(f"[DB] Starting worker: {worker_id}")
    
    connector = aiohttp.TCPConnector(ssl=_SSL_CTX, limit=GLOBAL_CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Open manifest file only if SAVE_HTML_FILES is enabled
        manifest_file = None
        if MANIFEST_PATH:
            manifest_file = open(MANIFEST_PATH, "a", encoding="utf-8")
            mf = manifest_file
        else:
            mf = None
        try:
            batch_num = 0
            total_processed = 0
            
            while True:
                if max_batches and batch_num >= max_batches:
                    break
                
                # Fetch pending URLs from database
                pending_urls = fetch_pending_urls_from_db(limit=batch_size, worker_id=worker_id)
                
                if not pending_urls:
                    logging.info(f"[DB] No more pending URLs. Processed {total_processed} URLs total.")
                    break
                
                batch_num += 1
                logging.info(f"[DB] Batch {batch_num}: Processing {len(pending_urls)} URLs")
                
                # Process all URLs in this batch
                tasks = [
                    process_url(
                        url_data["product_page_url"],
                        session,
                        mf,
                        url_id=url_data["id"],
                        product_type_id=url_data.get("product_type_id")
                    )
                    for url_data in pending_urls
                ]
                
                await asyncio.gather(*tasks)
                total_processed += len(pending_urls)
                
                logging.info(f"[DB] Batch {batch_num} completed. Total processed: {total_processed}")
                
                # Small delay between batches to avoid overwhelming the database
                await asyncio.sleep(1)
        finally:
            if manifest_file:
                manifest_file.close()
    
    logging.info(f"[DB] Worker {worker_id} finished. Total URLs processed: {total_processed}")

async def main(urls):
    """Main function for command-line URL processing (backward compatibility)."""
    # Create the aiohttp connector inside the running event loop (fixes "no running event loop")
    connector = aiohttp.TCPConnector(ssl=_SSL_CTX, limit=GLOBAL_CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Open manifest file only if SAVE_HTML_FILES is enabled
        if MANIFEST_PATH:
            manifest_file = open(MANIFEST_PATH, "a", encoding="utf-8")
            mf = manifest_file
        else:
            manifest_file = None
            mf = None
        try:
            tasks = [process_url(u, session, mf) for u in urls]
            await asyncio.gather(*tasks)
        finally:
            if manifest_file:
                manifest_file.close()

if __name__ == "__main__":
    import sys
    
    # Check if running in database mode
    if "--db" in sys.argv or "--database" in sys.argv:
        # Database mode: process URLs from product_page_urls table
        batch_size = 100
        max_batches = None
        
        # Parse optional arguments
        if "--batch-size" in sys.argv:
            idx = sys.argv.index("--batch-size")
            if idx + 1 < len(sys.argv):
                batch_size = int(sys.argv[idx + 1])
        
        if "--max-batches" in sys.argv:
            idx = sys.argv.index("--max-batches")
            if idx + 1 < len(sys.argv):
                max_batches = int(sys.argv[idx + 1])
        
        start_ts = time.time()
        asyncio.run(main_from_db(batch_size=batch_size, max_batches=max_batches))
        logging.info(f"Done. elapsed={time.time()-start_ts:.2f}s")
    else:
        # Command-line mode: process URLs from arguments or file
        if len(sys.argv) >= 2 and Path(sys.argv[1]).exists():
            lines = [l.strip() for l in open(sys.argv[1], "r", encoding="utf-8") if l.strip()]
            urls = lines
        else:
            urls = sys.argv[1:] or [
                "https://www.meesho.com/search?q=kurthi",
            ]

        start_ts = time.time()
        asyncio.run(main(urls))
        logging.info(f"Done. elapsed={time.time()-start_ts:.2f}s")
