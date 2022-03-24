import scrapy
import pandas as pd
import pkgutil
from io import StringIO
from albert.spiders.occasion_main import get_occasions_data


def get_dataframe(data_file, stringio=False, columns=None, converter=None):
    '''
    :param converter: converters to use for dataframe
    :param columns: columns to use for dataframe
    :param data_file: either a file or a dictionary we get the data from
    :param stringio: parameter used if the data comes from packaged data
    :return:
    '''
    try:
        if stringio is True:
            try:
                data = pd.read_csv(StringIO(data_file), converters=converter, usecols=columns)
                return data
            except Exception as err:
                print('stringIO did not work, data unavailable, using test data')
                print(f'{err}')
        data = pd.read_csv(data_file, converters=converter)
        return data
    except Exception as err:
        print(f'datafile not available : {data_file}')
        print('using common data in __data variable')
        return None


def get_data_file(pkg, path):
    contents = pkgutil.get_data(pkg, path)
    return contents.decode('utf-8')


def create_queries():
    # wells = get_data_file('albert', 'data/boulanger_PEM/EAN PEM.txt')
    # df = get_occasions_data()
    df = pd.DataFrame({'CODIC': [
'4351673',
'4766369',
'4766377',
'4766385',
'4805674',
'4805682',
'4805690',
'4854667',
'4854675',
'4854683',
'4854691',
'4854705',
'4854713',
'4854721',
'4854730',
'4854748',
'4854756',
'4854764',
'4854772',
'4854780',
'4854799',
'4854802',
'4854810',
'4854829',
'4857321',
'4864450',
'4864476',
'4864492',
'4864506',
'4864565',
'4873467',
'4887611',
'4892224',
'4904370',
'4905245',
'6241107',
'9099305',
'9109456',
'9157462',
'9157463',
'9173375',
'9173378',
'9179176',
'9179177',
'9179230',
'9187103',
'4829328 ']})
    df['URLS'] = df.apply(lambda row: f'https://www.darty.com/nav/recherche?srctype=list&text={row.CODIC}', axis=1)
    df.drop_duplicates(subset='CODIC', keep='first', inplace=True)
    return df


class DartyEANSpider(scrapy.Spider):
    name = 'darty_ean'
    allowed_domains = ['darty.com']
    handle_httpstatus_list = [404, 503, 403, 504]
    AUTOTHROTTLE_ENABLED = False
    RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 404, 403, 429]
    CONCURRENT_REQUESTS = 32
    CONCURRENT_REQUESTS_PER_DOMAIN = 32
    DOWNLOAD_TIMEOUT = 1000
    custom_settings = {
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 403],
    }
    start_urls = [
        'https://www.darty.com/'
    ]

    def __init__(self, name='darty_ean', **kwargs):
        super().__init__(name='darty_ean', **kwargs)

    def start_requests(self):
        df = create_queries()
        for index, (codic, url) in df.iterrows():
            yield scrapy.Request(url,
                                 callback=self.parse,
                                 meta={'CODIC': codic},
                                 headers={'User-Agent': "Scrapinghub-Fnac(crawl-diffusion@fnacdarty.com)"})

    def get_price(self, response: scrapy.http.Response):
        price = response.xpath(
            '//meta[@name="product_unitprice_ttc"][@data-tagcommander]/@content').get()
        if price:
            return price
        return None

    def parse(self, response: scrapy.http.Response):
        codic_initial = response.meta['CODIC']
        ean = response.xpath(
                '//div[@class="page_product store-in-history-trigger"]//meta[@itemprop="gtin13"]/@content').get()
        codic = response.xpath(
                '//div[@class="page_product store-in-history-trigger"]//meta[@itemprop="sku"]/@content').get()
        if codic == codic_initial:
            codic_okay = 'oui'
        else:
            codic_okay = 'non'
        url = response.request.url
        info_dict = {'CODIC': codic,
                     'EAN': ean,
                     'URL': url,
                     'codic_okay': codic_okay}
        yield info_dict
