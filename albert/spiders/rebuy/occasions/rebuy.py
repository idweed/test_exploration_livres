import json
import logging
import pkgutil
from io import StringIO
from time import sleep

import pandas as pd
import scrapy

from albert.spiders.occasion_main import get_darty_data, get_occasions_data


def get_data(data_file, stringio=False):
    '''
    :param data_file: either a file or a dictionary we get the data from
    :param stringio: parameter used if the data comes from packaged data
    :return:
    '''
    columns = ['EAN', 'LMARQUE', 'PRODUIT']
    converter = {'EAN': '{:0>13}'.format}
    if stringio is True:
        try:
            data = pd.read_csv(StringIO(data_file), sep=';', converters=converter, usecols=columns)
            data['URLS'] = data['PRODUIT'].apply(search_url)
            data.drop_duplicates(subset='EAN', inplace=True, ignore_index=True)
            return data
        except Exception as err:
            logging.error('stringIO did not work, data unavailable, using test data')
            logging.error(f'{err}')
            return None
    data = pd.read_csv(data_file, sep=',', converters=converter)
    return data


def get_data_file(pkg, path):
    contents = pkgutil.get_data(pkg, path)
    return contents.decode('utf-8')


def create_queries():
    wells = get_data_file('albert', 'data/occasions/occasion_ean_clean.csv')
    data = get_data(wells, stringio=True)
    return data


def title_converter(input_title: str):
    output_title = input_title.strip().lower().replace('/', ' ').replace('-', ' ')
    return output_title


def ean_converter(ean: str):
    return '{:0>13}'.format(ean)


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


def get_rebuy_data():
    url_pre = 'https://www.rebuy.fr/api/search/smart-suggestions?q='
    url_post = ''
    df1 = get_occasions_data()
    # df2 = get_dataframe_2()
    get_urls(url_pre, df1, url_post)
    # get_urls(url_pre, df2, url_post)
    final_df = df1
    # final_df = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
    # final_df.drop_duplicates(subset='EAN', keep='first', inplace=True, ignore_index=True)
    return final_df


# def search_url(item):
#     return f'https://www.rebuy.fr/api/search/smart-suggestions?q={item.replace(" ", "+")}'


class RebuySpider(scrapy.Spider):
    name = 'rebuy_occ'
    allowed_domains = ['rebuy.fr']
    crawlera_enabled = True
    # crawlera_apikey = 'ca8e16eeca8e4c9da8dc39383670b2da'  # region france
    crawlera_apikey = '5f5637c76d4245f88a631f85f93a3da6'  # region amazon test pricing - france + francophones
    # crawlera_apikey = '57ca6ff8cdeb41c1bd1d4a58261d22bc' # region all - openvalue
    handle_httpstatus_list = [404, 503, 403, 504]
    COOKIES_DEBUG = True
    AUTOTHROTTLE_ENABLED = False
    HTTPCACHE_POLICY = 'scrapy.extensions.httpcache.RFC2616Policy'
    RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 404, 403, 429]
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_DOMAIN': 10,
        'CONCURRENT_REQUESTS': 10,
        'DOWNLOAD_TIMEOUT': 1000,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610,
        },
        'DEFAULT_REQUEST_HEADERS': {
            'X-Crawlera-Profile': 'desktop',
            'X-Crawlera-Cookies': 'disable',
            'X-Crawlera-Timeout': 180000,
            # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            # 'Accept-Encoding': 'gzip, deflate, br',
            # 'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            # 'Upgrade-Insecure-Requests': '1',
            # 'cache-control': 'max-age=0',
            # 'sec-fetch-site': 'cross-site',
        },
        'RETRY_TIMES': 1,
        'AUTOTHROTTLE_ENABLED': 'False'
    }
    # start_urls = create_queries()[:550]

    def __init__(self, name='rebuy_occ', **kwargs):
        super().__init__(name='rebuy_occ', **kwargs)
        self.df = get_rebuy_data()[:500]

    def start_requests(self):
        yield scrapy.Request('https://www.rebuy.fr/', meta={'EAN': 0,
                                                            'PRODUIT': 'TOTO',
                                                            'MARQUE': 'toto',
                                                            'URL': 'toto'})
        headers = {"referer": "https://www.rebuy.fr/",
                   "sec-fetch-dest": "empty",
                   "sec-fetch-mode": "cors",
                   "sec-fetch-site": "same-origin",
                   "x-requested-with": "XMLHttpRequest"}
        for index, (ean, name, marque, url) in self.df.iterrows():
            yield scrapy.http.Request(url,
                                      callback=self.parse,
                                      meta={'EAN': ean,
                                            'MARQUE': marque,
                                            'PRODUIT': name,
                                            'URL': url},
                                      headers=headers)

    def procces_url(self, url):
        if url:
            return url.split('/')[3].replace('-', ' ')
        return None

    # def get_rebuy_product_price(self, current_xpath):
    #     self.logger.warning('getting rebuy price')
    #     price = current_xpath.xpath('.//ry-product-price//span/text()[normalize-space()]').get()
    #     if price:
    #         price = price.replace('&nbsp;€', '').strip()
    #     return price

    def parse(self, response: scrapy.http.Response) -> object:
        self.logger.debug(response.meta)
        product = response.meta['PRODUIT']
        marque = response.meta['MARQUE']
        try:
            json_item = json.loads(response.text)
            page_product = json_item['products'][0]['name']
            darty_data = get_darty_data('', page_product)
            darty_ean = darty_data[0]
            darty_prix = darty_data[2]
            # if darty_ean != response.meta['EAN']:
            #     yield {'EAN': response.meta['EAN'],
            #            'marque': marque}
            #     return
            price = json_item['products'][0]['blue_price']
            if price and price > 0:
                price = price * 0.01
            sleep(0.5)
            yield {'produit': product,
                   'EAN': response.meta['EAN'],
                   'darty_EAN': darty_ean,
                   'url': response.request.url,
                   'prix': price,
                   'marque': marque,
                   'page_product': page_product,
                   'darty_prix': darty_prix,
                   'delta_darty_rebuy': darty_prix - price}
                   # 'fnac_prix': darty_ean,
                   # 'delta_fnac_rebuy': darty_prix - price}
        except Exception as err:
            pass
