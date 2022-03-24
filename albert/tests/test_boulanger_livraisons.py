import unittest
import datetime
import pandas as pd
from random import randint
from albert.launching_scripts.livraisons.boulanger_livraisons_script import treat_data
from albert.spiders.boulanger.livraisons import BoulangerLivraisonSpider, create_queries, get_data_file, get_city_data, convert_cities_insee_code
from pathlib import Path
from scrapy.http import Response, HtmlResponse
from scrapy import Request

startdate = datetime.datetime.strptime('2019,01,01', '%Y,%m,%d')

data = [{
    b'Date de livraison': (startdate + datetime.timedelta(randint(1, 365))).strftime('%d/%m/%Y').encode(),
    b'Code Postal': b'75017',
    b'Code Insee': b'75017',
    b'Ville': b'Paris 17',
    b'_type': b'1',
    b'_key': b'2'
}, {
    b'Date de livraison': (startdate + datetime.timedelta(randint(1, 365))).strftime('%d/%m/%Y').encode(),
    b'Code Postal': b'93200',
    b'Code Insee': b'93200',
    b'Ville': b'Saint-Denis',
    b'_type': b'1',
    b'_key': b'2'
}, {
    b'Date de livraison': (startdate + datetime.timedelta(randint(1, 365))).strftime('%d/%m/%Y').encode(),
    b'Code Postal': b'93400',
    b'Code Insee': b'93400',
    b'Ville': b'Saint-Ouen',
    b'_type': b'1',
    b'_key': b'2'
}, {
    b'Date de livraison': b'Indisponible',
    b'Code Postal': b'93450',
    b'Code Insee': b'93450',
    b'Ville': b'Toto',
    b'_type': b'1',
    b'_key': b'2'
}]

data2 = [
    {
        'Date de livraison': (startdate + datetime.timedelta(randint(1, 365))).strftime('%d/%m/%Y'),
        'Code Postal': '75017',
        'Code Insee': '75017',
        'Ville': 'Paris 17'
    }, {
        'Date de livraison': (startdate + datetime.timedelta(randint(1, 365))).strftime('%d/%m/%Y'),
        'Code Postal': '93200',
        'Code Insee': '93200',
        'Ville': 'Saint-Denis'
    }, {
        'Date de livraison': (startdate + datetime.timedelta(randint(1, 365))).strftime('%d/%m/%Y'),
        'Code Postal': '93400',
        'Code Insee': '93400',
        'Ville': 'Saint-Ouen'
    }, {
        'Date de livraison': 'Indisponible',
        'Code Postal': '93450',
        'Code Insee': '93450',
        'Ville': 'Toto'
    }
]


def fake_response_from_file(file_name, meta):
    """
    Create a Scrapy fake HTTP response from a HTML file
    @param file_name: The relative filename from the responses directory,
                      but absolute paths are also accepted.
    @param url: The URL of the response.
    returns: A scrapy HTTP response which can be used for unittesting.
    """
    url = 'http://www.example.com'

    request = Request(url=url, meta=meta)
    try:
        with open(file_name, 'r') as file_content:
            response = HtmlResponse(url=url,
                                    request=request,
                                    body=file_content.read(),
                                    encoding='utf-8')
            # response.encoding = 'utf-8'
            return response
    except Exception as err:
        raise err


class TestLivraisonsCase(unittest.TestCase):
    def test_treat_data(self):
        treat_data(data, '/tmp/boulanger_test_case.csv')
        df = pd.read_csv('/tmp/boulanger_test_case.csv').to_dict(orient='records')
        assert [i for i in df if i not in data2]

    def test_parse_date(self):
        text_html = Path('./boulanger_livraison_correct.html')
        expected_result = {
            'Date de livraison': '19/06/2020',
            'Code Postal': '06110',
            'Ville': 'LE CANNET',
            'Code Insee': '06030',
            'url article': 'https://www.boulanger.com//ref/1086581',
        }
        meta_test_result = {
            'Date de livraison': '19/06/2020',
            'Code_postal': '06110',
            'Nom_commune': 'LE CANNET',
            'INSEE_COM': '06030',
            'product_url': ['https://www.boulanger.com//ref/1086581'],
        }
        response = fake_response_from_file(text_html, meta_test_result)
        spider = BoulangerLivraisonSpider()
        method_result = next(spider.parse_date(response))
        method_result.pop('timestamp', None)
        assert method_result == expected_result

    def test_parse_date_incorrect(self):
        text_html = Path('./boulanger_livraison_incorrect.html')
        expected_result = {
            'Date de livraison': '19/06/2020',
            'Code Postal': '06110',
            'Ville': 'LE CANNET',
            'Code Insee': '06030',
            'url article': 'https://www.boulanger.com//ref/1086581',
        }
        meta_test_result = {
            'Code_postal': '06110',
            'Nom_commune': 'LE CANNET',
            'INSEE_COM': '06030',
            'product_url': ['https://www.boulanger.com//ref/1086581'],
        }
        response = fake_response_from_file(text_html, meta_test_result)
        spider = BoulangerLivraisonSpider()
        method_result = next(spider.parse_date(response))
        method_result.pop('timestamp', None)
        assert method_result['Date de livraison'] != expected_result['Date de livraison']

    def test_catalog_entries(self):
        text_html = Path('./boulanger_site_lavelinges.html')
        meta = None
        response = fake_response_from_file(text_html, meta)
        expected_result = ['']
        spider = BoulangerLivraisonSpider()
        assert spider.get_catalog_entries(response) == expected_result


if __name__ == '__main__':
    unittest.main()
