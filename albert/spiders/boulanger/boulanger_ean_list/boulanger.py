import scrapy
import pandas as pd
import pkgutil
from io import StringIO
import json


def get_dataframe(data_file, stringio=False, columns=None, converter=None):
    '''
    :param converter: converters to use for dataframe
    :param columns: columns to use for dataframe
    :param data_file: either a file or a dictionary we get the data from
    :param stringio: parameter used if the data comes from packaged data
    :return:
    '''
    try:
        if stringio is True:
            try:
                data = pd.read_csv(StringIO(data_file), converters=converter, usecols=columns)
                return data
            except Exception as err:
                print('stringIO did not work, data unavailable, using test data')
                print(f'{err}')
        data = pd.read_csv(data_file, converters=converter)
        return data
    except Exception as err:
        print(f'datafile not available : {data_file}')
        print('using common data in __data variable')
        return None


def get_data_file(pkg, path):
    contents = pkgutil.get_data(pkg, path)
    return contents.decode('utf-8')


def create_queries():
    wells = get_data_file('albert', 'data/boulanger_PEM/EAN PEM.txt')
    df = get_dataframe(wells, stringio=True, columns=['EAN'], converter={'EAN': '{:0>13}'.format})
    df['URLS'] = df.apply(lambda row: f'https://www.boulanger.com/resultats?tr={row.EAN}', axis=1)
    df.drop_duplicates(subset='EAN', keep='first', inplace=True)
    return df


class BoulangerEANSpider(scrapy.Spider):
    name = 'boulanger_ean'
    allowed_domains = ["boulanger.com"]
    handle_httpstatus_list = [404, 503, 403, 504]
    AUTOTHROTTLE_ENABLED = False
    RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 404, 403, 429]
    CONCURRENT_REQUESTS = 60
    CONCURRENT_REQUESTS_PER_DOMAIN = 60
    DOWNLOAD_TIMEOUT = 1000
    crawlera_enabled = True
    crawlera_apikey = 'd4657c24702244628dddd04683675619'
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_DOMAIN': 60,
        'CONCURRENT_REQUESTS': 60,
        'DOWNLOAD_TIMEOUT': 1000,
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
        },
        'RETRY_TIMES': 3,
        'AUTOTHROTTLE_ENABLED': 'False'
    }
    start_urls = [
        'https://www.boulanger.com/'
    ]

    def __init__(self, name='boulanger_ean', **kwargs):
        super().__init__(name='boulanger_ean', **kwargs)
        self.detailed_offer_base_url = 'https://www.boulanger.com/webapp/wcs/stores/servlet/BLGetDynamicOffer?leadOfferCatentryId='

    def start_requests(self):
        df = create_queries()
        for index, (ean, url) in df.iterrows():
            yield scrapy.Request(url,
                                 callback=self.parse,
                                 meta={'EAN': ean})

    def get_price(self, response, ean):
        if response.xpath('//div[@id="pp"]//div[@class="middle"]//span[@itemprop="gtin13"]/text()').get() == ean:
            digits = response.xpath(
                '//div[@id="pp"]//div[@class="middle"]//div[@class="right"]//p[@class="fix-price"]//span[@class="exponent"]/text()').get()
            cents = response.xpath(
                '//div[@id="pp"]//div[@class="middle"]//div[@class="right"]//p[@class="fix-price"]//span[@class="fraction"]/text()').get()
            price = f'{digits},{cents}'
            return price
        return None

    def get_sku_url(self, response: scrapy.http.Response, ean):
        sku = response.xpath('//div[contains(@class, "opCom")]/@data-offer-id').get()
        if sku is None:
            self.logger.info("Failed to find any offer for ean: " + ean)
            return None
        return f'{self.detailed_offer_base_url}{sku}&storeId=10001&catalogId=10001&langId=-2'

    def parse_offer(self, response):
        d = response.meta['info_dict']
        item = d
        if "catentryId" not in response.text:
            self.logger.info("Failed to find any Offer Catentry ID")
            return
        try:
            response_text = json.loads(response.text)
        except ValueError:
            self.logger.error("Failed to load response text as JSON")
            return
        if response_text["catentryId"] is None:
            self.logger.info("Failed to find any Offer Catentry ID")
            return
        try:
            is_offer_own = response_text["offer"]["boulangerOffer"]
        except KeyError:
            return
        # Get the offer price
        try:
            new_price = response_text["offer"]["price"]["amount"]
            item['new_price'] = new_price
        except KeyError:
            item['new_price'] = 'unavailable'

        # Get the offer price
        try:
            offer_price = response_text["offer"]["price"]["amount"]
        except KeyError:
            return

        if isinstance(offer_price, float) and offer_price >= 0.0:
            offer_price = "{:.2f}".format(offer_price).replace(".", ",")

            if is_offer_own:
                item["own_offer_price"] = offer_price
            else:
                item["marketplace_offer_price"] = offer_price
        else:
            return

        # Get the shipping price
        try:
            shipping_price = response_text["shipping"]["cost"]
        except KeyError:
            shipping_price = 0.0

        if isinstance(shipping_price, float) and shipping_price >= 0.0:
            if is_offer_own:
                item["own_shipping_price"] = "5,00" if shipping_price > 0.0 else "0,00"
            else:
                item["marketplace_shipping_price"] = "{:.2f}".format(shipping_price).replace(".", ",")
        else:
            return

        yield item

    def parse(self, response: scrapy.http.Response):
        ean = response.meta['EAN']
        price = self.get_price(response, ean)
        # sku = self.get_sku_url(response, ean)
        url = response.request.url
        if price:
            price = price.replace(',', '.')
        info_dict = {'EAN': ean,
                     'URL': url,
                     'PRIX': price}
        yield info_dict
