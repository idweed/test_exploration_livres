import scrapy
from albert.spiders.occasion_main import get_occasions_data, fuzzymatch, get_darty_data


class YesyesSpider(scrapy.Spider):
    name = 'yesyes_occ'
    allowed_domains = ['yes-yes.com']
    crawlera_enabled = True
    crawlera_apikey = 'd4657c24702244628dddd04683675619'  # region france
    handle_httpstatus_list = [404, 503, 403, 504]
    AUTOTHROTTLE_ENABLED = False
    RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 404, 403, 429]
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_DOMAIN': 30,
        'CONCURRENT_REQUESTS': 30,
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
            'X-Crawlera-Cookies': 'disable',
            'X-Crawlera-Timeout': 180000,
        },
        'RETRY_TIMES': 3,
        'AUTOTHROTTLE_ENABLED': 'False'
    }
    start_urls = ['https://www.yes-yes.com/j-achete/']

    @staticmethod
    def parse_start_full_url(self, link):
        return f'https://www.yes-yes.com/{link}'

    def parse(self, response: scrapy.http.Response) -> object:
        """
        :param response: response from scrapy - gets the links from the sitemap
        :return: yields all the links through parse_product_page
        """
        # response.selector.remove_namespaces()
        products_list = response.xpath('//div/div[@class="phones-item-action"]/a/@href').getall()
        for link in products_list:
            yield scrapy.Request(self.parse_start_full_url(link),
                                 callback=self.parse_product_page)

    def parse_product_page(self, response: scrapy.http.Response) -> object:
        """
        :param response: scrapy response, contains the product page
        :return: a generator containing the product informations, most of which can be found in the URL
        """
        brand = response.request.url.split('/')[-3]
        brand = brand.split('-')[0]
        product = response.request.url.split('/')[-2]
        product = ' '.join(product.split('-')[:-1])
        url = response.request.url
        price = response.xpath('//*[@id="phone-retail-price"]/text()[normalize-space()]').get().strip()
        darty_data = get_darty_data(brand, product)
        darty_ean = darty_data[0]
        darty_price = darty_data[2]
        yield {'url': url,
               'marque': brand,
               'produit': product,
               'prix': price,
               'darty_ean': darty_ean,
               'darty_prix': darty_price,
               'delta_darty_yesyes': darty_price - price,
               # 'fnac_prix' : darty_ean,
               # 'delta_fnac_yesyes': darty_ean,
               }
