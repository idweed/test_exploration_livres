import scrapy

from albert.spiders.occasion_main import get_occasions_data, get_darty_data


class MurfySpider(scrapy.Spider):
    name = 'murfy_occ'
    allowed_domains = ['murfy.fr']
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
    # start_urls = ['https://reconditionne.murfy.fr/brands']
    start_urls = ['https://murfy.fr/reconditionne/']

    def __init__(self):
        super(MurfySpider, self).__init__()
        self.df = get_occasions_data()

    def parse(self, response: scrapy.http.Response) -> object:
        # product_type_list = response.xpath('//li[@class="category"]//a[@data-depth="0"]/@href').getall()
        product_type_list = response.xpath('//*[@id="__next"]//div//div[@class="jss119"]//a/@href').getall()
        for product_type_url in product_type_list:
            product_type = product_type_url[product_type_url.rfind('/'):]
            product_type_2 = product_type[product_type.find('-') + 1:]
            yield scrapy.Request(product_type_url,
                                 callback=self.parse_type_page,
                                 meta={'product_page': product_type_2})

    def parse_type_page(self, response: scrapy.http.Response) -> object:
        if response.xpath('//*[@id="content"]').get():
            return None
        # links = response.xpath('//div/article/div/div[2]/h3/a/@href').getall()
        links = response.xpath('//div[@class="jss192"]/a/@href').getall()
        for link in links:
            yield scrapy.Request(link,
                                 callback=self.parse_page_product,
                                 meta=response.meta)
        next_page = response.xpath('//div[@id="js-product-list-top"]//a[@rel="next"]/@href').get()
        if next_page:
            yield scrapy.Request(next_page,
                                 callback=self.parse_type_page,
                                 meta=response.meta)

    def parse_page_product(self, response: scrapy.http.Response) -> dict:
        title = response.xpath('//h1[@itemprop="name"]/text()').get()
        reference = response.xpath(u'.//p[text()[contains(.,"Modèle")]]/br[last()]/text()').get()
        if reference is None:
            reference = response.xpath(u'.//p[text()[contains(.,"modèle")]]/br[last()]/text()').get()
        if reference is None:
            reference = response.xpath(u'//p[text()[contains(.,"modèle")]]/text()').get()
        if reference is None:
            reference = response.xpath(u'//p//span[text()[contains(.,"référence")]]/text()').get()
        if reference is None:
            reference = response.xpath(u'//p//span[text()[contains(.,"Référence")]]/text()').get()
        if reference is None:
            reference = response.xpath(u'//p[text()[contains(.,"référence")]]/text()').get()
        if reference is None:
            reference = response.xpath(u'//p[text()[contains(.,"Référence")]]/text()').get()
        if reference is None:
            reference = response.xpath(u'//p[text()[contains(.,"Numéro de série")]]/text()').get()
        if reference:
            reference = reference[reference.find(':') + 1:].replace('&nbsp;', '').strip()
        else:
            reference = ''
        price = response.xpath('normalize-space(//div[@class="product-prices"]//span//span[@class="price"]/text())').get().replace(
            '\xa0', '')
        etat = response.xpath('normalize-space(//div[@class="blockreassurance_product"]/div/p/text())').get()
        title = '{} {}'.format(title, reference).lower().replace('en pose libre', '').replace('/', ' ').replace(' à ',
                                                                                                                ' ').replace(
            '.', ' ')
        url = response.request.url
        darty_data = get_darty_data(title, reference)
        if darty_data:
            darty_ean, darty_codic, darty_price = darty_data[0], darty_data[1], darty_data[2]
        else:
            darty_ean, darty_price, darty_codic = '', '', ''
        yield {'darty_ean': darty_ean,
               'darty_codic': darty_codic,
               'titre': title,
               'reference': reference,
               'prix': price,
               'url': url,
               'etat': etat}
