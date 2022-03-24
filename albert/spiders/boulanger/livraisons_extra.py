import scrapy
import pandas as pd
import pkgutil
from io import StringIO
import datetime

__data = [
    {
        'zipcode': '75013',
        'city': 'PARIS 13',
        'inseecode': '75113'
    },
    {
        'zipcode': '75019',
        'city': 'PARIS 19',
        'inseecode': '75119'
    },
    {
        'zipcode': '75015',
        'city': 'PARIS 15',
        'inseecode': '75115'
    },
    {
        'zipcode': '60000',
        'city': 'BEAUVAIS',
        'inseecode': '60057'
    },
    {
        'zipcode': '92000',
        'city': 'NANTERRE',
        'inseecode': '92050'
    },
    {
        'zipcode': '51100',
        'city': 'REIMS',
        'inseecode': '51454'
    },
    {
        'zipcode': '13001',
        'city': 'MARSEILLE 01',
        'inseecode': '13201'
    },
    {
        'zipcode': '30250',
        'city': 'SOMMIERES',
        'inseecode': '30321'
    },
    {
        'zipcode': '66000',
        'city': 'PERPIGNAN',
        'inseecode': '66136'
    }
]


def convert_cities_insee_code(requested_cities_file, cities_data_file, final_data_file):
    '''
the function gets the insee codes for the requested_cities_file from the cities_data_file.
The cities_data_file comes from the government provided source :
https://data.opendatasoft.com/explore/dataset/code-postal-code-insee-2015%40public/export/
The final file still needs user alterations in a few rows, don't forget to check it manually
convert_cities_insee_code :
- requested_cities_file : excel file containing the cities we wan't to get data on
- cities_data_file : excel file containing all the data on all the French cities
- final_data_file : excel output file containing all the cross data
In the output file, we want to keep the 5 number format for the INSEE and Zip/Postal codes.
This function needs to be executed only when the requested_cities_file is modified.
It is only meant to be used once because it doesn't usually change.
Olivier Guerin, big data Director sent the file.
    '''
    uppercase = lambda x: (f'{x}'.upper())
    columns_all_cities = ['Nom_commune', 'Code_postal', 'INSEE_COM']
    columns_requested_cities = ['Code_postal', 'Nom_commune']
    converter = {'Code_postal': '{:0>5}'.format,
                 'INSEE_COM': '{:0>5}'.format,
                 'Nom_commune': uppercase}
    try:
        requested_cities = pd.read_excel(requested_cities_file,
                                         usecols=columns_requested_cities,
                                         converters=converter).drop_duplicates()
        cities_data = pd.read_excel(cities_data_file,
                                    usecols=columns_all_cities,
                                    converters=converter).drop_duplicates()
        common = requested_cities.merge(cities_data, on=['Code_postal', 'Nom_commune'], how='left')
        common.to_excel(final_data_file)
    except Exception as err:
        print(
            f'Inseecodes unavailable, {cities_data_file} and {final_data_file} not processed. Data file not created')
        print(err)


def get_city_data(data_file, stringio=False):
    '''
    :param data_file: either a file or a dictionary we get the data from
    :param stringio: parameter used if the data comes from packaged data
    :return:
    '''
    try:
        columns = ['Nom_commune', 'Code_postal', 'INSEE_COM']
        uppercase = lambda x: (f'{x}'.upper())
        converter = {'Code_postal': '{:0>5}'.format,
                     'INSEE_COM': '{:0>5}'.format,
                     'Nom_commune': uppercase}
        if stringio is True:
            try:
                data = pd.read_csv(StringIO(data_file), converters=converter, sep=';', usecols=columns)
                return data
            except Exception as err:
                print('stringIO did not work, data unavailable, using test data')
                print(f'{err}')
        data = pd.read_csv(data_file, converters=converter, sep=';')
        return data
    except Exception as err:
        print(f'datafile not available : {data_file}')
        print('using common data in __data variable')
        return __data


def get_data_file(pkg, path):
    contents = pkgutil.get_data(pkg, path)
    return contents.decode('utf-8')


def process_ean_string(ean):
    return '{:0>13}'.format(ean.split('/')[-1].split('_')[0])


def create_queries(catalog_entries, products_urls, products_brands, products_ean):
    wells = get_data_file('albert', 'data/villes_crawl_boulanger.csv')
    data = get_city_data(wells, stringio=True)
    query_base = lambda: f'https://www.boulanger.com/webapp/wcs/stores/servlet/BLDeliverySlotCalendarScheduleAppointmentTableDisplay?storeId=10001&langId=-2&catalogId=10001&catentryId={catalog_entry}&shipModeId=14551&dailySlot=true&zipcode={zipcode}&city={city}&insee={inseecode}&shipmodeLabel=LVB'
    queries_list = []
    for catalog_entry, product_url, brand, ean in zip(catalog_entries, products_urls, products_brands, products_ean):
        for index, location in data.iterrows():
            location = location.to_dict()
            zipcode = location['Code_postal']
            city = location['Nom_commune']
            inseecode = location['INSEE_COM']
            location['product_url'] = [f"https://www.boulanger.com/{product_url}"]
            location['ean'] = process_ean_string(ean)
            location['brand'] = brand.strip()
            queries_list.append((query_base(), location.copy()))
    return queries_list


# create_queries(['toto'], ['tata'])


class BoulangerLivraisonSpider(scrapy.Spider):
    name = 'boulanger_livraisons_extra'
    allowed_domains = ["boulanger.com"]
    handle_httpstatus_list = [404, 503, 403, 504]
    AUTOTHROTTLE_ENABLED = False
    RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 404, 403, 429]
    CONCURRENT_REQUESTS = 32
    CONCURRENT_REQUESTS_PER_DOMAIN = 32
    DOWNLOAD_TIMEOUT = 1000
    crawlera_enabled = True
    # it seems as if queries have to come from France exclusively
    crawlera_apikey = 'd4657c24702244628dddd04683675619'
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
            'X-Crawlera-Timeout': 180000,
        },
        'RETRY_TIMES': 3,
        'AUTOTHROTTLE_ENABLED': 'False'
    }
    start_urls = [
        'https://www.boulanger.com/c/lave-linge'
    ]

    def __init__(self, name='boulanger_livraisons', **kwargs):
        super().__init__(name='boulanger_livraisons', **kwargs)
        # self.number_of_articles = 4

    def get_catalog_entries(self, response):
        list_entries = response.xpath(
            '//div[@class="productListe"]//div[contains(@class, "product  dynamic-product-data-")]/@class').getall()
        # [
        #                :self.number_of_articles]
        new_data = [w.replace("product  dynamic-product-data-", "") for w in list_entries if 'indispo' not in w]
        return new_data

    def parse(self, response):
        sub_categories_list = response.xpath(
            '//a[@href="/c/gros-electro-menager"]/..//ul//a[@href="/c/lave-linge"]/..//li//a')
        self.logger.warning(sub_categories_list)
        if sub_categories_list is None or sub_categories_list.get() is None:
            self.logger.error('error while getting list of subcategories and urls')
            return
        meta = response.meta
        for subcategory in sub_categories_list[:1]:
            self.logger.warning(subcategory)
            partial_url = subcategory.xpath('./@href').get()
            subcategory_name = subcategory.xpath('./@data-href').get()
            meta['subcategory'] = subcategory_name
            yield scrapy.Request('https://www.boulanger.com/{}?viewSize=100'.format(partial_url),
                                 callback=self.parse_subcategory_page,
                                 meta=meta)
        # yield scrapy.Request('https://www.boulanger.com/c/lave-linge?viewSize=30', callback=self.parse_subcategory_page, meta=meta)

    def process_ean_string(self, ean):
        return '{:0>13}'.format(ean.split('/')[-1].split('_')[0])

    def parse_subcategory_page(self, response):
        num_articles = 4
        catalog_entries = self.get_catalog_entries(response)
        self.logger.warning('request url {}'.format(response.request.url))
        products_urls = response.xpath("//div//div//h2//a/@href").getall()[:num_articles]
        products_brand = response.xpath("//div//div//h2//a//strong/text()").getall()[:num_articles]
        products_ean = response.xpath('//div//div//img/@data-lazy-src').getall()[:num_articles]
        products_available = response.xpath('//div//div//div[contains(@class, "pb-left")]')
        # [:self.number_of_articles]
        queries = create_queries(catalog_entries, products_urls, products_brand, products_ean)
        self.logger.warning(len(products_urls))
        self.logger.warning(len(products_ean))
        self.logger.warning(len(products_brand))
        self.logger.error(products_ean)
        self.logger.error(products_brand)
        self.logger.error(products_urls)
        i = 0
        for query, meta in queries:
            self.logger.error(query)
            # self.logger.error(meta)
            if 'ssentiel' in meta['brand']:
                meta['ean'] = 'Boulanger'
            meta['subcategory'] = response.meta['subcategory']
            i += 1
            # self.logger.error(meta)

            # yield scrapy.Request(query, callback=self.parse_date, meta=meta)

        next_page = response.xpath('//span[contains(@class, "navPage-right")]//a[@class="searchInputMode"]/@href').get()

        if next_page:

            next_page_number = self.get_page_number_from_url(next_page)
            current_page_number = self.get_page_number_from_url(response.request.url)
            if self.go_to_next_page(next_page_number, current_page_number) is False:
                return
            meta = response.meta
            self.logger.error('next page is https://www.boulanger.com{}'.format(next_page))
            yield scrapy.Request('https://www.boulanger.com{}'.format(next_page),
                                 callback=self.parse_subcategory_page,
                                 meta=meta)

    def parse_date(self, response):
        date = response.xpath('//button[contains(@class, "js-aircalendar")]/@data-first-available-date').get()
        if date is not None:
            date = date.replace('_', '/')
        else:
            date = 'Indisponible pour cet article'
        today = datetime.datetime.now()
        french_hour = today + datetime.timedelta(hours=2)
        yield {'Date de livraison': date,
               'Code Postal': response.meta['Code_postal'],
               'Ville': response.meta['Nom_commune'],
               'Code Insee': response.meta['INSEE_COM'],
               'url article': response.meta['product_url'][0],
               'timestamp': '{}'.format(french_hour.isoformat()),
               'famille produit': response.meta['subcategory'].replace('-', ' '),
               'marque produit': response.meta['brand'],
               'EAN': response.meta['ean'],
               }

    @staticmethod
    def get_page_number_from_url(page_number):
        """
        :param page_number: string : example = https://www.boulanger.com/c/lave-linge-hublot?numPage=2&viewSize=100
        :return: integer : next page number
        """
        if page_number:
            parse_on_question_mark = page_number.split('?')
            if parse_on_question_mark and len(parse_on_question_mark) >= 2:
                parse_on_ecom_symbol = parse_on_question_mark[1].split('&')
                if parse_on_ecom_symbol and len(parse_on_ecom_symbol) >= 2:
                    parse_on_equal_symbol = parse_on_ecom_symbol[0].split('=')
                    if parse_on_equal_symbol and len(parse_on_equal_symbol) >= 2:
                        return int(parse_on_equal_symbol[1])
        return 0

    @staticmethod
    def go_to_next_page(next_page_number, current_page_number):
        if next_page_number <= 1:
            return False
        if next_page_number > 10:
            return False
        if current_page_number > next_page_number:
            return False
        return True
