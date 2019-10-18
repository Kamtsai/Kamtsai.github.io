import datetime
import time
import random
from urllib.parse import quote_plus
from pyquery import PyQuery as pq
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


def get_match_prod(prod_name):
    prod_name = quote_plus(prod_name)
    page_url = "https://feebee.com.tw/s/?q={}".format(prod_name)
    html_doc = pq(page_url)
    match_prods = [i.text for i in html_doc(".product_link .large")]
    match_prod_links = ["https://feebee.com.tw" +
                        i.attr("href") for i in html_doc(".product_link_all").items()]
    return match_prods, match_prod_links


def get_retailer_prod(match_prod_link):
    html_doc = pq(match_prod_link)
    pages = [i.text for i in html_doc(".pagination_page")]
    pages.insert(0, '1')
    page_links = ["{}&page={}".format(match_prod_link, i) for i in pages]
    retailer_prod_prices = []
    retailer_links = []
    for pl in page_links:
        html_doc = pq(pl)
        prices = [int(i.text.replace(",", ""))
                  for i in html_doc("#product_list .price")]
        links = [i.attr("data-url") for i in html_doc("a").items()
                 if i.attr("data-url") is not None]
        links = set(links)
        links = list(links)
        retailer_prod_prices += prices
        retailer_links += links
    return retailer_prod_prices, retailer_links


def get_prods(match_prods, match_prod_links):
    prods = []
    for mp, mpl in zip(match_prods, match_prod_links):
        retailer_prod_prices, retailer_links = get_retailer_prod(mpl)
        prod_dict = {}
        prod_dict["prodName"] = mp
        prod_dict["retailerPrice"] = retailer_prod_prices
        prod_dict["retailerLink"] = retailer_links
        prods.append(prod_dict)
    return prods


def batch_scraper(search_products):
    prod_data = {}
    scraping_time = datetime.datetime.now()
    scraping_time = scraping_time.strftime("%Y-%m-%d %X")
    prod_data["scrapingTime"] = scraping_time
    for sp in search_products:
        match_prods, match_prod_links = get_match_prod(sp)
        prods = get_prods(match_prods, match_prod_links)
        prod_data[sp] = prods
        sleep_secs = random.randint(3, 10)
        time.sleep(sleep_secs)
    return prod_data


prod_data = batch_scraper(
    ["surface pro", "surface book", "macbook air", "macbook pro"])
service_account = "/home/ubuntu/yotta-demo-aec23-firebase-adminsdk-g17qh-080bfab13c.json"
cred = credentials.Certificate(service_account)
firebase_admin.initialize_app(cred)
db = firestore.client()
collection_ref = db.collection("products")
collection_ref.add(prod_data)
