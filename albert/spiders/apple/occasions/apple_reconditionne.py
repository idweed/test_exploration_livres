import scrapy
from albert.spiders.occasion_main import get_occasions_data, fuzzymatch, get_darty_data


class AppleReconditionneSpider(scrapy.Spider):
    name = 'apple_occ'
    allowed_domains = ['apple.com']
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
    apple_categories = ['mac', 'ipad', 'iphone', 'ipod', 'appletv', 'accessories']
    start_urls = ['https://www.apple.com/fr/shop/refurbished/{}'.format(elem) for elem in apple_categories]
    # df = get_occasions_data()

    # def get_ean(self, titre, marque):
    #     df = self.df
    #     marque_rows = df[df['MARQUE'].str.contains(marque, case=False)]
    #     if marque_rows is None:
    #         return None
    #     else:
    #         ean = fuzzymatch(marque_rows, titre)
    #         print('ean = {}.'.format(ean))
    #         if ean is None or ean == '':
    #             return None
    #         return ean

    def parse(self, response: scrapy.http.Response) -> object:
        """
        :param response: response from scrapy - gets all items - a 100 at a time
        :return: yields all the processed items as they are available here
        """
        product_list = response.xpath('//*[@id="page"]/div[7]/div[3]/ul/li')
        if product_list.get() is None:
            yield {'url': response.request.url,
                   'produit apple': response.request.url.rsplit('/')[-1],
                   'disponibilité': 'non'}
        else:
            for (index, item) in enumerate(product_list):
                title = item.xpath('h3/a/text()[normalize-space()]').get().replace('&nbsp;', '')
                title = title.replace(' (Sans abonnement)', '').replace('-', ' ').replace('–', ' ').replace('(',
                                                                                                            ' ').replace(
                    ')', ' ').replace('reconditionnée', '').replace('reconditionné', '').replace('avec', '').replace(
                    'à ', ' ').replace('et ', ' ').replace(' pouces', ' ').replace('huit', '8').replace('  ', ' ')
                url = item.xpath('h3/a/@href').get()
                price = item.xpath('normalize-space(div/text()[normalize-space()])').get().replace('&nbsp;', '').replace(' €', '')
                darty_data = get_darty_data('apple', title)
                if darty_data:
                    darty_ean = darty_data[0]
                    darty_price = darty_data[2]
                else:
                    darty_ean = ''
                    darty_price = ''
                try:
                    ean_in_df = self.df.loc[self.df['EAN'] == darty_ean]['EAN']
                except Exception:
                    ean_in_df = None
                if price.replace(',', '.').replace(' ', '') == '':
                    price = '0'
                if darty_price.replace(',', '.').replace(' ', '') == '':
                    darty_price = '0'
                yield {'url': url,
                       'produit apple': response.request.url.rsplit('/')[-1],
                       'titre': title,
                       'prix': price,
                       'disponibilité': 'oui',
                       'darty_ean': darty_ean,
                       'df': ean_in_df,
                       'darty_price': darty_price,
                       'delta_darty_apple': float(darty_price.replace(',', '.')) - float(price.replace(',', '.').replace(' ', ''))}
