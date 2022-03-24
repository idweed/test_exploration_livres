import scrapy
import pandas as pd
from fuzzywuzzy import fuzz
from io import StringIO
import pkgutil
from albert.spiders.occasion_main import get_occasions_data, get_darty_data

valid_ratio = True
invalid_ratio = False


def title_converter(input_title: str):
    output_title = input_title.strip().lower().replace('/', ' ').replace('-', ' ')
    return output_title


def ean_converter(ean: str):
    return '{:0>13}'.format(ean)


def compare_strings(str1, str2):
    ratio = fuzz.partial_token_set_ratio(str1, str2)
    return ratio


def validate_ratio(ratio, min_ratio=86):
    if ratio > min_ratio:
        return True
    return False


def backmarket_clean_string(url):
    try:
        if url:
            output_str = url.split('/')[3].replace('+', ' ').replace('-', ' ').lower()
            return output_str
    except Exception:
        pass
    return None


def get_data_file(pkg, path):
    contents = pkgutil.get_data(pkg, path)
    return contents.decode('utf-8')


def get_dataframe():
    converter = {'EAN': ean_converter,
                 'PRODUIT': title_converter}
    wells = get_data_file('albert', 'data/occasions/occasion_ean_clean.csv')
    df = pd.read_csv(StringIO(wells), sep=';', converters=converter)
    df.rename(columns={"LMARQUE": "MARQUE"}, errors='raise', inplace=True)
    df.drop_duplicates(subset='EAN', keep='first', inplace=True, ignore_index=True)
    df.drop(
        columns=['SKU', 'NUM_SERIE', 'ID_FNAC', 'ORIGINE', 'EMBALLAGE', 'ACCESSOIRE', 'MATERIEL', 'STATUT', 'CODE_MARQUE', 'CODE_FAMILLE',
                 'CODE_SOUS_FAMILLE', 'LIBELLE_FAMILLE', 'LIBELLE_SOUS_FAMILLE'], inplace=True)
    return df


def get_dataframe_2():
    converter = {'EAN': ean_converter,
                 'PRODUIT': title_converter}
    wells = get_data_file('albert', 'data/occasions/ean_occasions_2.csv')
    df = pd.read_csv(StringIO(wells), sep=';', converters=converter)
    df.drop_duplicates(subset='EAN', keep='first', inplace=True, ignore_index=True)
    df['PRODUIT'] = df['MARQUE'] + ' ' + df['PRODUIT']
    return df


def get_urls(url_pre, dataframe, url_post):
    dataframe['URL'] = dataframe.apply(lambda row: f'{url_pre}{row.PRODUIT.replace(" ", "+")}{url_post}', axis=1)


def get_backmarket_data():
    url_pre = 'https://www.backmarket.fr/search?q='
    url_post = ''
    df1 = get_occasions_data()
    # df2 = get_dataframe_2()
    get_urls(url_pre, df1, url_post)
    # get_urls(url_pre, df2, url_post)
    # final_df = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
    final_df = df1
    # final_df.drop_duplicates(subset='EAN', keep='first', inplace=True, ignore_index=True)
    return final_df

# get_backmarket_data()


class BackmarketSpider(scrapy.Spider):
    name = 'backmarket_occ'
    allowed_domains = ['backmarket.fr']
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
            'X-Crawlera-Profile': 'desktop',
            'X-Crawlera-Max-Retries': '3',
            'X-Crawlera-Timeout': 180000,
        },
        'RETRY_TIMES': 3,
        'AUTOTHROTTLE_ENABLED': 'False'
    }
    start_urls = [
        'https://www.backmarket.fr/'
    ]

    def __init__(self, name='backmarket_occ', **kwargs):
        super().__init__(name='backmarket_occ', **kwargs)
        self.df = get_backmarket_data()

    def start_requests(self):
        for index, (ean, name, marque, url) in self.df.iterrows():
            yield scrapy.Request(url,
                                 callback=self.parse,
                                 meta={'EAN': ean,
                                       'PRODUIT': name})

    def parse(self, response: scrapy.http.Response):
        name = response.meta['PRODUIT']
        ean = response.meta['EAN']
        backmarket_products = response.xpath('//div[@data-test="product-thumb"]//a')
        valid_product = {'ratio': 0, 'EAN': ean, 'PRODUIT': name, 'PRIX': 0, 'URL': response.request.url}
        prev_ratio = 0
        for product in backmarket_products:
            product_url = 'https://www.backmarket.fr{}'.format(product.xpath('@href').get())
            product_price = product.xpath('.//*[@data-test="price"]/text()').get().strip().replace('&nbsp;€', '')
            backmarket_string = backmarket_clean_string(product_url)
            darty_data = get_darty_data(backmarket_string, ' ')
            if darty_data and darty_data[0] == ean:
                valid_product['EAN'] = ean
                valid_product['PRODUIT'] = name
                valid_product['PRIX'] = product_price
                valid_product['darty_prix'] = darty_data[2]
        yield valid_product
