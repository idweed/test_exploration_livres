import pkgutil
import re
from io import StringIO

import pandas as pd
import scrapy


def get_ean_asin_file_data(data_file):
    try:
        columns = ['EAN', 'ASIN']
        converter = {'EAN': '{:0>13}'.format}
        data = pd.read_csv(StringIO(data_file), converters=converter, sep=',', usecols=columns)
        data.set_index('ASIN', inplace=True)
        return data.to_dict()['EAN']
    except Exception as err:
        print('stringIO did not work, data unavailable, using test data')
        print(f'{err}')
        return None


def get_data_file(pkg, path):
    contents = pkgutil.get_data(pkg, path)
    return contents.decode('utf-8')


# def create_queries(catalog_entries, products_urls):
#     wells = get_data_file('albert', 'data/villes_crawl_boulanger.csv')
#     data = get_ean_asin_file_data(wells)
#     query_base = lambda: f'https://www.amazon.fr/gp/offer-listing/{asin}/ref=olp_f_new?f_new=true'
#     queries_list = []
#     for index, location in data.iterrows():
#         location = location.to_dict()
#         city = location['Nom_commune']
#         location['product_url'] = [f"https://www.boulanger.com/{product_url}"]
#         queries_list.append((query_base(), location.copy()))
#     return queries_list
# wells = get_data_file('albert', 'data/amazon_exploration_livres_ean/livres_ean_asin_final_dropna.csv')
# data = get_ean_asin_file_data(wells)
# print('toto')

class AmazonSpider(scrapy.Spider):
    name = 'amazon'
    availability = True
    allowed_domains = ['amazon.fr']
    handle_httpstatus_list = [404, 503, 403, 504]
    AUTOTHROTTLE_ENABLED = False
    RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 404, 403, 429]
    CONCURRENT_REQUESTS = 70
    CONCURRENT_REQUESTS_PER_DOMAIN = 70
    DOWNLOAD_TIMEOUT = 10000
    crawlera_enabled = True
    # queries from europe_for_amazon
    crawlera_apikey = 'd94f8fb5fb694e61af35d9fd3ffe255f'
    custom_settings = {
        'RETRY_TIMES': 3,
        'AUTOTHROTTLE_ENABLED': 'False',
        'CONCURRENT_REQUESTS_PER_DOMAIN': 95,
        'CONCURRENT_REQUESTS': 95,
        'DOWNLOAD_TIMEOUT': 100000,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610,
        },
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'X-Crawlera-Profile': 'desktop',
            'X-Crawlera-Max-Retries': '3',
            'X-Crawlera-Timeout': 180000,
            'X-Crawlera-Cookies': 'disable',
            'Save-Data': 'on'
        }
    }

    wells = get_data_file('albert', 'data/amazon_exploration_livres_ean/livres_ean_asin_final_dropna.csv')
    data = get_ean_asin_file_data(wells)
    # ean_asin_dict = sorted(data.items())[300000:]
    ean_asin_dict = sorted(data.items())[300000 + 250000:]
    asin_pattern = r"[a-zA-Z0-9]{10}"
    start_urls = [f'https://www.amazon.fr/dp/{asin}' for (asin, ean) in ean_asin_dict]

    def offer_listing_url(self, x):
        return f'https://www.amazon.fr/gp/offer-listing/{x}/ref=olp_f_new?f_new=true'

    def get_asin_ean_tuple_from_url(self, url):
        clean_url = url.strip('/')
        url_split_list = clean_url.split('/')
        if len(url_split_list) > 3:
            asin = url_split_list[-1]
            if re.match(self.asin_pattern, asin) and asin in self.data.keys():
                return (self.data[asin], asin)
        return None

    def parse(self, response):
        availability = response.xpath('normalize-space(//*[@id="availability"]/span/text())').get()
        if availability is None or availability == '':
            availability = 'Non'
        seller = response.xpath('normalize-space(//*[@id="merchant-info"]/text())').get()
        amazon_product = 'Non'
        if 'Expédié et vendu par Amazon' in seller:
            amazon_product = 'Oui'
        message_availability = response.xpath('//*[@id="outOfStock"]/div/div/span/text()')
        if message_availability and 'indisponible' in message_availability.get():
            availability = 'Non'
        price = response.xpath(
            'normalize-space(//*[@id="buyNewSection"]/div/div/div/div/a/div/div/span/span/text())').get()
        if not price:
            price = response.xpath('normalize-space(//*[@id="buyNewSection"]/div/div/span/span/text())').get()
        if not price:
            price = response.xpath(
                'normalize-space(//*[@id="buyNewSection"]/div/div/div/div[2]/a/div/div[2]/span/span/text())').get()
        shipping_price = response.xpath(
            'normalize-space(//*[@id="buyNewSection"]/div/div/div/div/a/div/div/span/span/text())').get()
        shipping_price_2 = response.xpath('normalize-space(//*[@id="buyNewInner"]/div/div/span/span/text())').get()
        shipping_price_3 = response.xpath('normalize-space(//*[@id="buyNewInner"]/div/div/span/a/text())').get()
        shipping_price_4 = response.xpath(
            'normalize-space(//span[contains(@class, "buyboxShippingLabel")]/span/text())').get()
        shipping_price_5 = response.xpath('normalize-space(//*[@id="buyNewInner"]/div[3]/div/span/span/text())').get()
        if 'livraison' not in shipping_price.lower():
            shipping_price = shipping_price_2
        if 'livraison' not in shipping_price.lower():
            shipping_price = shipping_price_3
        if 'livraison' not in shipping_price.lower():
            shipping_price = shipping_price_4
        if 'livraison' not in shipping_price.lower():
            shipping_price = shipping_price_5
        ean_asin_tuple = self.get_asin_ean_tuple_from_url(response.request.url)
        ean = ean_asin_tuple[0]
        asin = ean_asin_tuple[1]
        if amazon_product == 'Non' and availability and availability != 'Non' and 'indisponible' not in availability.lower():
            yield scrapy.http.request.Request(self.offer_listing_url(asin),
                                              callback=self.parse_offer_listing, meta={'ASIN': asin,
                                                                                       'EAN': ean,
                                                                                       'Disponibilite': availability})

        else:
            yield {'EAN': ean,
                   'ASIN': asin,
                   'Disponibilite': availability,
                   'Prix': price,
                   'Produit Amazon': amazon_product,
                   'Frais de port': shipping_price}

    def parse_offer_listing(self, response: scrapy.http.Response):
        offers_selector_list = response.xpath('//*[@id="olpOfferList"]/div/div/div')
        marketplace_price = None
        stored_price = None
        ean = response.meta['EAN']
        asin = response.meta['ASIN']
        availability = response.meta['Disponibilite']
        item = {'EAN': ean,
                'ASIN': asin,
                'Disponibilite': availability,
                'Prix': None,
                'Produit Amazon': 'Non',
                'Frais de port': None,
                'Prix_MP_Amazon': None}

        if not offers_selector_list:  # Dismiss if no offer was found
            self.logger.info('Failed to find any offer for ean: {} - asin: {}'.format(ean, asin))
            yield item
            return
        stored_price = None
        for offer_selector in offers_selector_list:
            if offer_selector.xpath(
                    'normalize-space(div/div/span/text())').get() != "Neuf":
                continue  # Dismiss the offer if the product is not marked as new
            if offer_selector.xpath('div/h3/img[@alt="Amazon.fr"]'):
                stored_price = offer_selector.xpath(
                    'normalize-space(div[contains(@class, "olpPriceColumn")]/span/text())').get()
                item['Produit Amazon'] = 'Oui'
                item['Frais de port'] = offer_selector.xpath(
                    'normalize-space(span[@class="olpShippingPrice"]/text())').get()
                continue
            elif offer_selector.xpath(
                    'div[@class="a-column a-span2 olpSellerColumn"]/h3[@class="a-spacing-none olpSellerName"]/span[@class="a-size-medium a-text-bold"]/a'):
                is_marketplace = True
            else:
                self.logger.error('Technical: No seller name found')
                yield item
                return
            shipping_price = offer_selector.xpath('normalize-space(span[@class="olpShippingPrice"]/text())').get()
            offer_price = offer_selector.xpath(
                'normalize-space(div[@class="a-column a-span2 olpPriceColumn"]/span[@class="a-size-large a-color-price olpOfferPrice a-text-bold"])').get()
            if not offer_price:
                self.logger.error('Technical: No price found')
                continue
            offer_price = ','.join(offer_price.replace('EUR ', '').split('.'))
            if is_marketplace:
                if marketplace_price is None:
                    marketplace_price = offer_price
                else:
                    if float(offer_price.replace(',', '.')) < float(marketplace_price.replace(',', '.')):
                        marketplace_price = offer_price
            if item['Frais de port'] is None:
                item['Frais de port'] = shipping_price
            item['Prix'] = stored_price
            item['Prix_MP_Amazon'] = marketplace_price
        yield item
