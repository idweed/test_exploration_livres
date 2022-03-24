import scrapy
import pandas as pd
from fuzzywuzzy import fuzz
from io import StringIO
import pkgutil
from albert.spiders.occasion_main import get_occasions_data, get_darty_data
import json

valid_ratio = True
invalid_ratio = False


def fnac_requests_url(sku):
    return f'https://www.fnac.com/Nav/API/MpOffersList/{sku}/1?page=1'


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


def fnac_clean_string(url):
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
    df.drop_duplicates(subset='EAN', keep='first', inplace=True, ignore_index=True)
    df.drop(
        columns=['ID_FNAC', 'ORIGINE', 'EMBALLAGE', 'ACCESSOIRE', 'MATERIEL', 'STATUT', 'CODE_MARQUE', 'CODE_FAMILLE',
                 'CODE_SOUS_FAMILLE', 'LIBELLE_FAMILLE', 'LIBELLE_SOUS_FAMILLE', 'SKU', 'NUM_SERIE'], inplace=True)
    df.rename(columns={'LMARQUE': 'MARQUE'}, inplace=True)
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
    dataframe['URL'] = dataframe.apply(lambda row: f'{url_pre}{row.EAN.replace(" ", "+")}{url_post}', axis=1)


def get_fnac_data():
    url_pre = 'https://www.fnac.com/SearchResult/ResultList.aspx?Search='
    url_post = ''
    df1 = get_occasions_data()
    # df2 = get_dataframe_2()
    get_urls(url_pre, df1, url_post)
    # get_urls(url_pre, df2, url_post)
    # final_df = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
    final_df = df1
    final_df.drop_duplicates(subset='EAN', keep='first', inplace=True, ignore_index=True)
    return final_df


class FnacSpider(scrapy.Spider):
    name = 'fnac_occ'
    allowed_domains = ['fnac.com']
    handle_httpstatus_list = [404, 503, 403, 504]
    AUTOTHROTTLE_ENABLED = False
    RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 404, 403, 429]
    CONCURRENT_REQUESTS = 32
    CONCURRENT_REQUESTS_PER_DOMAIN = 32
    DOWNLOAD_TIMEOUT = 1000
    custom_settings = {
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 403],
        "custom_headers": {
            'User-Agent': "Scrapinghub-Fnac(crawl-diffusion@fnacdarty.com)",
        }
    }
    start_urls = [
        'https://www.fnac.com/',
    ]

    def __init__(self, name='fnac_occasions', **kwargs):
        super().__init__(name='fnac_occasions', **kwargs)
        self.df = get_fnac_data()

    current_data = None

    def start_requests(self):
        for index, (ean, marque, name, url) in self.df.iterrows():
            yield scrapy.Request(url,
                                 callback=self.parse,
                                 meta={'EAN': ean,
                                       'PRODUIT': name},
                                 headers={'User-Agent': "Scrapinghub-Fnac(crawl-diffusion@fnacdarty.com)"})

    def parse(self, response: scrapy.http.Response):
        self.current_data = None
        name = response.meta['PRODUIT']
        ean = response.meta['EAN']
        darty_ean = None
        darty_codic = None
        darty_price = None
        try:
            darty_data = get_darty_data(' ', name)
            if darty_data and darty_data[0] == ean:
                darty_ean = darty_data[0]
                darty_codic = darty_data[1]
                darty_price = darty_data[2]
        except Exception as err:
            self.logger.error('error : {}'.format(err))
        if response.xpath('//article'):
            all_articles = response.xpath(
                'normalize-space(//article//div[@class="Article-itemInfo"]//li[@class="Shop-item"]//div//div//strong/text())')
            for article in all_articles:
                url = response.xpath('normalize-space(//article//p[@class="Article-desc"]//a/@href)').get()
                price = response.xpath(
                    'normalize-space(//article//div[@class="Article-price"]//div//a//strong/text())').get()
                etat = response.xpath('normalize-space(//article/div/div/div/div/ul/li/div/div[1]/strong/text())').get()
                current_data = {'titre': name,
                                'EAN': ean,
                                'URL': url,
                                'PRIX': price,
                                'ETAT': etat,
                                'DARTY_ean': darty_ean,
                                'DARTY_codic': darty_codic,
                                'DARTY_PrixTTC': darty_price}
                yield scrapy.http.request.Request(url,
                                                  callback=self.parse_detailed_offer,
                                                  headers={
                                                      'User-Agent': "Scrapinghub-Fnac(crawl-diffusion@fnacdarty.com)"},
                                                  meta={'items': current_data})
                break

    def parse_detailed_offer(self, response):
        current_data = response.meta['items']
        prix_occasion_fnac_tabs = response.xpath(
            'normalize-space(//div//section[contains(@class, "f-productOffers")]//ul[contains(@class, "f-productOffers-tabs")]//li//span)')
        prix_fnac = None
        for tab in prix_occasion_fnac_tabs:
            tab_string = tab.get()
            if 'occasion' in tab.get():
                prix_fnac = tab_string.split(' ')[-1]
                prix_fnac = prix_fnac.replace('€', ',')
                break
        second_price_test = json.loads(response.xpath('//script[9]/text()').get())
        current_data['occasion_second_price_test'] = second_price_test['offers']['price']
        current_data['PRIX'] = prix_fnac
        yield current_data
