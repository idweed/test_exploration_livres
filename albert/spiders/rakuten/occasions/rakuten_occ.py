import logging
import pkgutil
from io import StringIO

import pandas as pd
import scrapy
from fuzzywuzzy import fuzz


def compare_strings(str1, str2):
    ratio = fuzz.partial_token_set_ratio(str1, str2)
    return ratio


def validate_ratio(ratio, min_ratio=86):
    if ratio > min_ratio:
        return True
    return False


def search_url(item):
    return f'https://fr.shopping.rakuten.com/s/{item.replace(" ", "+")}'


def get_data(data_file, stringio=False):
    '''
    :param data_file: either a file or a dictionary we get the data from
    :param stringio: parameter used if the data comes from packaged data
    :return:
    '''
    columns = ['EAN', 'LMARQUE', 'PRODUIT']
    converter = {'EAN': '{:0>13}'.format}
    if stringio is True:
        try:
            data = pd.read_csv(StringIO(data_file), sep=';', converters=converter, usecols=columns)
            data['URLS'] = data['PRODUIT'].apply(search_url)
            data.drop_duplicates(subset='EAN', inplace=True, ignore_index=True)
            return data
        except Exception as err:
            logging.error('stringIO did not work, data unavailable, using test data')
            logging.error(f'{err}')
            return None
    data = pd.read_csv(data_file, sep=',', converters=converter)
    return data


def get_data_file(pkg, path):
    contents = pkgutil.get_data(pkg, path)
    return contents.decode('utf-8')


def create_queries():
    wells = get_data_file('albert', 'data/occasions/occasion_ean_clean.csv')
    data = get_data(wells, stringio=True)
    return data


# create_queries()


class RakutenSpider(scrapy.Spider):
    name = 'rakuten_occ'
    allowed_domains = ['fr.shopping.rakuten.com']
    crawlera_enabled = True
    # crawlera_apikey = 'ca8e16eeca8e4c9da8dc39383670b2da'  # region france
    # crawlera_apikey = '5f5637c76d4245f88a631f85f93a3da6' # region amazon test pricing - france + francophones
    crawlera_apikey = '6236aaf94798498c9de9034ffa9fb796'  # region all - openvalue
    handle_httpstatus_list = [404, 503, 403, 504]
    # COOKIES_DEBUG = True
    AUTOTHROTTLE_ENABLED = False
    # HTTPCACHE_POLICY = 'scrapy.extensions.httpcache.RFC2616Policy'
    RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 404, 403, 429]
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_DOMAIN': 30,
        'CONCURRENT_REQUESTS': 30,
        'DOWNLOAD_TIMEOUT': 10000,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610,
        },
        'DEFAULT_REQUEST_HEADERS': {
            'X-Crawlera-Profile': 'desktop',
            'X-Crawlera-Cookies': 'disable',
            'X-Crawlera-Timeout': 180000,
            'X-Crawlera-Session': 'create',
            # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            # 'Accept-Encoding': 'gzip, deflate, br',
            # 'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            # 'Upgrade-Insecure-Requests': '1',
            'sec-fetch-dest': 'document',
            'cache-control': 'max-age=0',
            # 'sec-fetch-site': 'cross-site',
        },
        'RETRY_TIMES': 2,
        'AUTOTHROTTLE_ENABLED': 'False'
    }
    # start_urls = ['https://fr.shopping.rakuten.com/']

    def __init__(self, name='rakuten_reconditionne', **kwargs):
        super().__init__(name='rakuten_reconditionne', **kwargs)
        self.start_urls = create_queries()[:50]

    def start_requests(self):
        yield scrapy.Request('https://fr.shopping.rakuten.com/',
                             headers={'X-Crawlera-Session': 'create'},
                             meta={'EAN': 0,
                                   'PRODUIT': 'TOTO',
                                   'MARQUE': 'toto',
                                   'URL': 'toto'},
                             callback=self.parse)

    def get_rakuten_product_price(self, current_xpath, url):
        self.logger.warning('getting rakuten price')
        occasion_keywords = ['occasion', 'comme neuf', 'très bon état', 'bon état']
        price = current_xpath.xpath('.//div[@data-qa="used_product"][normalize-space()]/text()').get()
        occasion_type = None
        if price:
            for keyword in occasion_keywords:
                if keyword in price:
                    occasion_type = keyword
                    break
            return_price = current_xpath.xpath(
                './/div[@data-qa="used_product"][normalize-space()]//span/text()').get()
            if return_price and occasion_type:
                return_price = return_price.replace('€', '').strip().replace(',', '.')
            return return_price
        self.logger.warning('no price found for url {} on rakuten'.format(url))
        return None

    def parse(self, response):
        session_id = response.headers.get('X-Crawlera-Session', ''),
        for index, (ean, marque, produit, url) in self.start_urls.iterrows():
            yield scrapy.Request(url,
                                 callback=self.parse_rakuten_page,
                                 headers={'X-Crawlera-Session': session_id},
                                 meta={'EAN': ean,
                                       'MARQUE': marque,
                                       'PRODUIT': produit,
                                       'URL': url})

    def parse_rakuten_page(self, response: scrapy.http.Response) -> object:
        self.logger.debug(response.meta)
        product = response.meta['PRODUIT']
        product_titles = response.xpath('//*[contains(@class, "f6 lh-title")]')
        cmp_ratio = 0
        price = None
        for product_title in product_titles:
        #     current_title = product_title.xpath('./@text').get()
        #     current_ratio = compare_strings(current_title, product)
        #     if validate_ratio(current_ratio) and cmp_ratio < current_ratio:
        #         cmp_ratio = current_ratio
            price = self.get_rakuten_product_price(product_title, response.url)
            if price:
                break
        if response.xpath('//div[@data-qa="used_product"]//span/text()').get():
            yield {'produit': product,
                   'EAN': response.meta['EAN'],
                   'url': response.request.url,
                   # 'html': response.body,
                   'prix': price}
    #
    #     brand_pages = response.xpath('//ul/li/div[2]/p/a/@href').getall()
    #     for brand_url in brand_pages:
    #         brand_name = brand_url[brand_url.rfind('/'):]
    #         brand_name = brand_name[brand_name.find('-') + 1:]
    #         yield scrapy.Request(brand_url,
    #                              callback=self.parse_brand,
    #                              meta={'brand': brand_name})
    #
    # def parse_brand(self, response: scrapy.http.Response) -> object:
    #     if response.xpath('//*[@id="content"]').get():
    #         return None
    #     links = response.xpath('//div/article/div/div[2]/h3/a/@href').getall()
    #     for link in links:
    #         yield scrapy.Request(link,
    #                              callback=self.parse_page_product,
    #                              meta=response.meta)
    #     next_page = response.xpath('//div[@id="js-product-list-top"]//a[@rel="next"]/@href').get()
    #     if next_page:
    #         yield scrapy.Request(next_page,
    #                              callback=self.parse_brand,
    #                              meta=response.meta)
    #
    # def parse_page_product(self, response: scrapy.http.Response) -> dict:
    #     title = response.xpath('//h1[@itemprop="name"]/text()').get()
    #     reference = response.xpath(u'.//p[text()[contains(.,"Modèle")]]/br[last()]/text()').get()
    #     if reference is None:
    #         reference = response.xpath(u'.//p[text()[contains(.,"modèle")]]/br[last()]/text()').get()
    #     if reference is None:
    #         reference = response.xpath(u'//p[text()[contains(.,"modèle")]]/text()').get()
    #     if reference is None:
    #         reference = response.xpath(u'//p//span[text()[contains(.,"référence")]]/text()').get()
    #     if reference is None:
    #         reference = response.xpath(u'//p//span[text()[contains(.,"Référence")]]/text()').get()
    #     if reference is None:
    #         reference = response.xpath(u'//p[text()[contains(.,"référence")]]/text()').get()
    #     if reference is None:
    #         reference = response.xpath(u'//p[text()[contains(.,"Référence")]]/text()').get()
    #     if reference is None:
    #         reference = response.xpath(u'//p[text()[contains(.,"Numéro de série")]]/text()').get()
    #     if reference:
    #         reference = reference[reference.find(':') + 1:].replace('&nbsp;', '').strip()
    #     price = response.xpath('//div[@class="product-prices"]//span//span[@class="price"]/text()').get().replace('\xa0', '')
    #     yield {'titre': title,
    #            'reference': reference,
    #            'prix': price,
    #            'marque': response.meta['brand'],
    #            'url': response.request.url}
