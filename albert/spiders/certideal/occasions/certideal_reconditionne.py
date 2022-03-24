import scrapy
from albert.spiders.occasion_main import get_occasions_data, get_darty_data


class CertidealSpider(scrapy.Spider):
    name = 'certideal_occ'
    allowed_domains = ['certideal.com']
    crawlera_enabled = True
    crawlera_apikey = 'd4657c24702244628dddd04683675619' # region france
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
    start_urls = ['https://certideal.com/plan-du-site']
    start_urls_2 = ['https://certideal.com/iphone-reconditionnes-82',
                    'https://certideal.com/samsung-reconditionnes-90',
                    'https://certideal.com/tablettes-reconditionnees-118',
                    'https://certideal.com/accessoires-153',
                    'https://certideal.com/apple-watch-139']

    def __init__(self):
        super(CertidealSpider, self).__init__()
        self.df = get_occasions_data()

    def parse(self, response: scrapy.http.Response) -> object:
        """
        :param response: contains the links to the different products categories
        :return: a generator for the requests towards the categories
        """
        iphones = response.xpath('//*[@id="listpage_content"]/div[1]/ul/li[1]/ul/li[1]/ul/li/a')
        for (index, iphone) in enumerate(iphones):
            if 'Sans' not in iphone.xpath('text()').get():
                yield scrapy.Request(iphone.xpath('@href').get(),
                                     callback=self.parse_brand,
                                     meta={'marque': 'Apple',
                                           'type de produit': iphone.xpath('text()[normalize-space()]').get()})
        samsung = response.xpath('//*[@id="listpage_content"]/div[1]/ul/li[1]/ul/li[2]/ul/li/a')
        for (index, samsung) in enumerate(samsung):
            yield scrapy.Request(samsung.xpath('@href').get(),
                                 callback=self.parse_brand,
                                 meta={'marque': 'Samsung',
                                       'type de produit': samsung.xpath('text()[normalize-space()]').get()})
        ipad = response.xpath('//*[@id="listpage_content"]/div[1]/ul/li[3]/ul/li/a')
        for (index, ipad) in enumerate(ipad):
            yield scrapy.Request(ipad.xpath('@href').get(),
                                 callback=self.parse_brand,
                                 meta={'marque': 'Apple',
                                       'type de produit': ipad.xpath('text()[normalize-space()]').get()})
        accessoires = response.xpath('//*[@id="listpage_content"]/div[1]/ul/li[4]/ul/li/a')
        for (index, accessoires) in enumerate(accessoires):
            yield scrapy.Request(accessoires.xpath('@href').get(),
                                 callback=self.parse_brand,
                                 meta={'marque': 'Accessoires certideal',
                                       'type de produit': accessoires.xpath('text()[normalize-space()]').get()})
        apple_watch = response.xpath('//*[@id="listpage_content"]/div[1]/ul/li[5]/ul/li/a')
        for (index, apple_watch) in enumerate(apple_watch):
            yield scrapy.Request(apple_watch.xpath('@href').get(),
                                 callback=self.parse_brand,
                                 meta={'marque': 'Apple',
                                       'type de produit': apple_watch.xpath('text()[normalize-space()]').get()})

    def parse_brand(self, response: scrapy.http.Response) -> object:
        brand = response.meta['marque']
        type_of_product = response.meta['type de produit']
        current_url = response.request.url
        if response.xpath('//*[@id="products"]/div[@class="alert alert-error"]').get():
            yield {'marque': brand,
                   'type de produit': type_of_product,
                   'url': current_url,
                   'disponibilité': 'non'}
            return None
        all_products = response.xpath('//*[@id="product_list"]/div/li/div[2]/a')
        for (index, product) in enumerate(all_products):
            product_title = product.xpath('normalize-space(div[1]/h6[@class="product-title"]/text())').get().strip()
            price = product.xpath('normalize-space(div[3]/div/span/text())').get()
            etat = product.xpath('normalize-space(div[3]/p[@class="product-feature-state"]/span/text())').get()
            url = product.xpath('./@href').get()
            darty_data = get_darty_data(brand, product_title)
            if darty_data:
                darty_ean, darty_codic, darty_price = darty_data[0], darty_data[1], darty_data[2]
            else:
                darty_ean, darty_codic, darty_price = None, None, None
            yield {'titre': product_title,
                   'prix': price,
                   'etat': etat,
                   'url': url,
                   'disponibilité': 'oui',
                   'darty_ean': darty_ean,
                   'darty_codic': darty_codic,
                   'darty_price': darty_price}
        next_page = response.xpath('//*[@id="pagination_next"]/a/@href').get()
        if next_page:
            yield scrapy.Request('https://certideal.com{}'.format(next_page),
                                 callback=self.parse_brand,
                                 meta=response.meta)
