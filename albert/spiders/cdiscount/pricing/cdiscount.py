import scrapy
import json
from time import sleep
from albert.utils.pricing import PricingClass


def modify_brand(brand):
    brand = str(brand)
    brand = brand.strip()
    brand = brand.replace(' ', '')
    brand = brand.replace('-', '')
    brand = brand.upper()
    return brand


class CdiscountSpider(scrapy.Spider):
    name = 'cdiscount_promo'
    allowed_domains = ['cdiscount.com']
    crawlera_enabled = True
    # crawlera_apikey = 'd4657c24702244628dddd04683675619' # region france
    crawlera_apikey = '5f5637c76d4245f88a631f85f93a3da6'  # region amazon test pricing - france + francophones
    # crawlera_apikey = '57ca6ff8cdeb41c1bd1d4a58261d22bc' # region all - openvalue
    handle_httpstatus_list = [404, 503, 403, 504]
    AUTOTHROTTLE_ENABLED = False
    RETRY_HTTP_CODES = [500, 502, 503, 504, 429]
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_DOMAIN': 20,
        'CONCURRENT_REQUESTS': 20,
        'DOWNLOAD_TIMEOUT': 10000,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610,
        },
        'DEFAULT_REQUEST_HEADERS': {
            'X-Crawlera-Profile': 'desktop',
            'X-Crawlera-Cookies': 'disable',
            'X-Crawlera-Timeout': 180000,
            # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br'
            # 'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            # 'Upgrade-Insecure-Requests': '1',
        },
        'RETRY_TIMES': 1,
        # 'AUTOTHROTTLE_ENABLED': 'False',
        'ROBOTSTXT_OBEY': False,
    }
    start_urls = ['https://www.cdiscount.com/ProductListUC.mvc/UpdateJsonPage']
    payload = {'page': '1',
               'TechnicalForm.SiteMapNodeId': '0',
               'TechnicalForm.DepartmentId': '',
               'TechnicalForm.ProductId': '',
               'hdnPageType': 'Search',
               'TechnicalForm.ContentTypeId': '',
               'TechnicalForm.SellerId': '',
               'TechnicalForm.PageType': 'SEARCH_AJAX',
               'TechnicalForm.LazyLoading.ProductSheets': 'False',
               'TechnicalForm.BrandLicenseId': '0',
               'NavigationForm.CurrentSelectedNavigationPath': '0',
               'NavigationForm.FirstNavigationLinkCount': '1',
               'SortForm.BrandLicenseSelectedCategoryPath': '',
               'SortForm.SelectedNavigationPath': '',
               'ProductListTechnicalForm.Keyword': 'apple+watch',
               'ProductListTechnicalForm.TemplateName': 'InLine'}

    def __init__(self, *args, **kwargs):
        super(CdiscountSpider, self).__init__(*args, **kwargs)
        self.data_class = PricingClass()
        self.ean_brand_list = self.data_class.df[['EAN', 'Editeur']][500:]

    def cdiscount_clean_list_price(self, price):
        return '{}.{}'.format(price[0].strip(), price[1].replace('€', '').strip())

    def cdiscount_price_str_to_float(self, price):
        return price.replace('€', '.').strip()

    def cdiscount_clean_price(self, price):
        self.logger.error(price)
        if len(price) == 2 and isinstance(price, list):
            clean_price = self.cdiscount_clean_list_price(price)
        elif isinstance(price, str):
            clean_price = self.cdiscount_price_str_to_float(price)
        else:
            return False
        return float(clean_price)

    def start_requests(self):
        for index, elem in self.ean_brand_list.iterrows():
            ean = elem['EAN']
            brand = elem['Editeur']
            self.logger.error('{} : {}'.format(ean, brand))
            payload_copy = self.payload.copy()
            brand = modify_brand(brand)
            cdiscount_id = '{}{:0>13}'.format(brand.upper()[:3], ean)
            payload_copy['ProductListTechnicalForm.Keyword'] = cdiscount_id
            self.logger.debug(str(payload_copy))
            yield scrapy.FormRequest(self.start_urls[0], method='POST', formdata=payload_copy, callback=self.parse,
                                     dont_filter=True, meta={'EAN': cdiscount_id})

    def close(self, reason):
        sleep(30)
        self.logger.error('spider closed, proxy terminated, driver closed, reason = {}'.format(reason))

    def parse(self, response: scrapy.http.Response) -> object:
        item = {}
        decoded_content = response.body.decode('utf-8')
        self.logger.warning('request EAN {}'.format(response.meta['EAN']))
        # self.logger.warning('decoded content : {}'.format(decoded_content))
        json_content = json.loads(decoded_content)
        products_list = json_content['products']
        for product in products_list:
            if product['sku'].upper() == response.meta['EAN'].upper():
                product_price_selector = scrapy.Selector(text=product['prx']['val'], type='html')
                price = product_price_selector.xpath('//text()').getall()
                item['price'] = self.cdiscount_clean_price(price)
                item['ean'] = product['sku']
                item['marketplace'] = product['isMKPProduct']
                item['livraison'] = product['freeShip']
                yield item
            else:
                self.logger.warning(
                    'EANs not equal : cdiscount id = {} and ean = {}'.format(product['sku'], response.meta['EAN']))
