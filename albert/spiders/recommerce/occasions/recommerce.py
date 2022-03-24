import scrapy
from albert.spiders.occasion_main import get_occasions_data, fuzzymatch, get_darty_data


class RecommerceSpider(scrapy.Spider):
    name = 'recommerce_occ'
    allowed_domains = ['recommerce.com']
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
    start_urls = ['https://www.recommerce.com/fr/tous-les-telephones-reconditionnes?product_list_limit=100']
    # df = get_occasions_data()

    # def get_ean(self, titre, marque):
    #     ean = None
    #     df = self.df
    #     marque_rows = df[df['MARQUE'].str.contains(marque, case=False)]
    #     if marque_rows is None:
    #         return None
    #     else:
    #         ean = fuzzymatch(marque_rows, titre)
    #         if ean is None or ean == '':
    #             return None
    #     return ean

    def parse(self, response: scrapy.http.Response) -> object:
        """
        :param response: response from scrapy - gets all items - a 100 at a time
        :return: yields all the processed items as they are available here
        """
        items = response.xpath('//div[@class="product-item"]')
        if items:
            print('ITEMS')
        for (index, item) in enumerate(items):
            brand = item.xpath('.//div[@class="product-item-manufacturer"]//span/text()').get().strip()
            link = item.xpath('.//a/@href').get()
            title_first_part = item.xpath('.//div[@class="product-item-name"]/*/*/text()').get().strip()
            title_second_part = item.xpath('.//div[@class="product-item-attribute"]/text()').get().strip()
            price = item.xpath('.//div[@class="product-item-price"]/*/*/*/*/text()').get().replace('&nbsp;', '').strip()
            stocks = 'oui' if item.xpath('.//span[contains(@class, "outofstock")]').get() is None else 'non'
            title = '{} {}'.format(title_first_part, title_second_part).replace(',', ' ').replace('mono sim',
                                                                                                  '').replace('(',
                                                                                                              '').replace(
                ')',
                '').replace(
                ' ', '%20')
            darty_data = get_darty_data(brand, title)
            darty_ean = darty_data[0]
            darty_prix = darty_data[2]
            title = '{}'.format(title.replace('%20', ' '))
            yield {'darty_ean': darty_ean,
                   'darty_prix':darty_prix,
                   'en stock': stocks,
                   'titre': title,
                   'prix': price,
                   'marque': brand,
                   'url': link}
        next_page = response.xpath('//a[@class="next"]/@href').get()
        if next_page:
            yield scrapy.Request(next_page,
                                 callback=self.parse)
