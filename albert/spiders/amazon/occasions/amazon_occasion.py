import logging
import pandas as pd
import scrapy
import keepa
import numpy as np
from albert.spiders.occasion_main import get_occasions_data, get_darty_data


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def process_ean(ean):
    keepa_key = '46bt1moa68mkpnn1s2844i7239dm8d28ua8mv2q40o20da062qgmulnsreim5eev'
    api = keepa.Keepa(keepa_key)
    return_list = []
    num_products_retrieved = 0
    for group in chunker(ean, 80):
        products = group.values
        if '0000000000EAN' in products:
            products = np.delete(products, np.where(products == '0000000000EAN'))
        products = api.query(products, product_code_is_asin=False)
        num_products_retrieved += len(products)
        try:
            for product in products:
                if product['eanList']:
                    ean_curr = product['eanList'][0]
                else:
                    ean_curr = None
                if product['asin']:
                    return_list.append((ean_curr, product["asin"]))
                    # return_list.append((ean_curr, f'https://www.amazon.fr/gp/offer-listing/{product["asin"]}/ref=olp_f_used?f_usedAcceptable=true&f_usedGood=true&f_used=true&f_usedLikeNew=true&f_usedVeryGood=true'))
                else:
                    return_list.append((ean_curr, None))
        except Exception:
            logging.error('keepa error while retrieving ASINs - chunk : {} - {}'.format(group[0], group[-1]))
    return return_list


def get_keepa_data(data):
    url_tuple_list = process_ean(data['EAN'])
    url_df = pd.DataFrame(url_tuple_list, columns=['EAN', 'ASIN'])
    data = data.merge(url_df, on='EAN', how='outer').dropna()
    return data


class AmazonOccSpider(scrapy.Spider):
    name = 'amazon_occ'
    allowed_domains = ['amazon.fr']
    handle_httpstatus_list = [404, 503, 403, 504]
    AUTOTHROTTLE_ENABLED = False
    RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 404, 403, 429]
    CONCURRENT_REQUESTS = 60
    CONCURRENT_REQUESTS_PER_DOMAIN = 60
    DOWNLOAD_TIMEOUT = 1000
    crawlera_enabled = True
    crawlera_apikey = 'ca8e16eeca8e4c9da8dc39383670b2da'
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_DOMAIN': 60,
        'CONCURRENT_REQUESTS': 60,
        'DOWNLOAD_TIMEOUT': 1000,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610,
        },
        'DEFAULT_REQUEST_HEADERS': {
            'X-Crawlera-Profile': 'desktop',
            'X-Crawlera-Max-Retries': '3',
            'X-Crawlera-Timeout': 180000,
        },
        'RETRY_TIMES': 3,
        'AUTOTHROTTLE_ENABLED': 'False'
    }
    start_urls = ['https://www.amazon.fr']

    # <span id="aod-end-of-results" class="a-size-base a-color-base aod-hide">
    # Fin des résultats
    # </span>

    def start_requests(self):
        self.start_urls = self.create_amazon_queries()
        for index, (ean, produit, marque, url) in self.start_urls.iterrows():
            yield scrapy.FormRequest(url,
                                     callback=self.parse,
                                     meta={'EAN': ean,
                                           'MARQUE': marque,
                                           'PRODUIT': produit,
                                           'URL': url})

    def create_amazon_queries(self):
        data = get_occasions_data()
        data = get_keepa_data(data)
        return data

    def parse(self, response):
        try:
            item = {'EAN': response.meta['EAN'],
                    'PRODUCT': response.meta['PRODUIT'],
                    'MARQUE': response.meta['MARQUE'],
                    'URL': response.meta['URL']}
        except KeyError:
            self.logger.warning('meta = {}'.format(response.meta))
            item = response.meta['item']
        darty_data = get_darty_data(str(item['MARQUE']), item['PRODUCT'])
        try:
            item['darty_prix'] = darty_data[2]
            item['darty_ean'] = darty_data[0]
        except Exception:
            item['darty_prix'] = None
            item['darty_ean'] = None

        availability = response.xpath \
            ('normalize-space(//div[@id="availability"])').get()
        if availability.startswith(("Bientôt disponible", "En stock", "Habituellement expédié sous",
                                    "Il ne reste plus que", "Temporairement en rupture de stock",
                                    "Cet article paraîtra")):
            if availability.startswith("Cet article paraîtra"):
                if '.' in availability:
                    availability = availability.split('.')[0]
            item["availability"] = availability

        offers_selector_list = response.xpath('//div[@class="a-row a-spacing-mini olpOffer"]')

        # Dismiss if no offer was found
        if not offers_selector_list:
            self.logger.info("Failed to find any offer for sku: {}".format(response.meta['URL']))

        for offer_selector in offers_selector_list:
            # Dismiss the offer if the product is not marked as new
            if offer_selector.xpath('.//span[@id="pe-olp-exclusive-for-prime-string"]'):
                self.logger.info("Offer is exclusive for Amazon Prime")
                continue
            condition = offer_selector.xpath(
                    'normalize-space(div[@class="a-column a-span3 olpConditionColumn"]/div[@class="a-section a-spacing-small"]/span[@class="a-size-medium olpCondition a-text-bold"])')
            if offer_selector.xpath(
                    'normalize-space(div[@class="a-column a-span3 olpConditionColumn"]/div[@class="a-section a-spacing-small"]/span[@class="a-size-medium olpCondition a-text-bold"])').get() == "Neuf":
                self.logger.info('offer is new {}'.format(item['URL']))
                continue
            is_lowest_price_yet = False
            is_lowest_price_too = False

            # Get the offer price
            offer_price = offer_selector.xpath(
                'normalize-space(div[@class="a-column a-span2 olpPriceColumn"]/span[@class="a-size-large a-color-price olpOfferPrice a-text-bold"])').get()
            if not offer_price:
                self.logger.error('Technical: No price found')
                continue
            if offer_selector.xpath(
                    'div[@class="a-column a-span2 olpSellerColumn"]/h3[@class="a-spacing-none olpSellerName"]/span[@class="a-size-medium a-text-bold"]/a'):
                is_marketplace = True
            elif offer_selector.xpath(
                    'div[@class="a-column a-span2 olpSellerColumn"]/h3[@class="a-spacing-none olpSellerName"]/img[@alt="Amazon.fr"]'):
                is_marketplace = False
            else:
                self.logger.error('no seller')
                return
            offer_price = ''.join(offer_price.replace('EUR ', '').split('.'))
            # price = offer_price.replace(',', '.')
            try:
                item['price']
            except KeyError:
                item['price'] = offer_price
            else:
                if float(offer_price.replace(',', '.')) < float(item['price'].replace(',', '.')):
                    item['price'] = offer_price
            try:
                item['etat']
            except KeyError:
                item['etat'] = condition.get()
        next_page_url_path = response.xpath(
            '//div[@class="a-text-center a-spacing-large"]/ul[@class="a-pagination"]/li[@class="a-last"]/a[text() = "Suivant"]/@href').get()
        if next_page_url_path is None:
            # Dismiss the item if next page not found
            try:
                item['delta_darty_amazon'] = float(item['darty_prix'].replace(',', '.')) - float(item['price'].replace(',', '.'))
            except Exception:
                self.logger.error('no delta available for product {}'.format(response.request.url.strip('/').split('/')[-1]))
            yield item
        else:
            yield scrapy.http.request.Request('https://www.amazon.fr/{}'.format(next_page_url_path),
                                              callback=self.parse, meta={'item': item})
