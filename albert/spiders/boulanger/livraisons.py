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


def create_queries(catalog_entries, products_urls):
    wells = get_data_file('albert', 'data/villes_crawl_boulanger.csv')
    data = get_city_data(wells, stringio=True)
    query_base = lambda: f'https://www.boulanger.com/webapp/wcs/stores/servlet/BLDeliverySlotCalendarScheduleAppointmentTableDisplay?storeId=10001&langId=-2&catalogId=10001&catentryId={catalog_entry}&shipModeId=14551&dailySlot=true&zipcode={zipcode}&city={city}&insee={inseecode}&shipmodeLabel=LVB'
    queries_list = []
    for catalog_entry, product_url in zip(catalog_entries, products_urls):
        for index, location in data.iterrows():
            location = location.to_dict()
            zipcode = location['Code_postal']
            city = location['Nom_commune']
            inseecode = location['INSEE_COM']
            location['product_url'] = [f"https://www.boulanger.com/{product_url}"]
            queries_list.append((query_base(), location.copy()))
    return queries_list


# create_queries(['toto'], ['tata'])


class BoulangerLivraisonSpider(scrapy.Spider):
    name = 'boulanger_livraisons'
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
        'https://www.boulanger.com/c/lave-linge-hublot'
    ]

    def __init__(self, name='boulanger_livraisons', **kwargs):
        super().__init__(name='boulanger_livraisons', **kwargs)
        self.number_of_articles = 4

    def get_catalog_entries(self, response):
        list_entries = response.xpath(
            '//div[@class="productListe"]//div[contains(@class, "product  dynamic-product-data-")]/@class').getall()[
                       :self.number_of_articles]
        new_data = [w.replace("product  dynamic-product-data-", "") for w in list_entries]
        return new_data

    def parse(self, response):
        catalog_entries = self.get_catalog_entries(response)
        products_urls = response.xpath("//div//div//h2//a/@href").getall()[:self.number_of_articles]
        queries = create_queries(catalog_entries, products_urls)
        for query, meta in queries:
            yield scrapy.Request(query, callback=self.parse_date, meta=meta)

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
               'timestamp': '{}'.format(french_hour.isoformat())}
