import json
import pkgutil
from io import StringIO
import albert.config.ftp_wrapper as ftp_wrapper
import albert.config.config as config

import pandas as pd
import requests
from fuzzywuzzy import fuzz


def get_darty_data(marque, reference_constructeur):
    headers = {
        'User-Agent': 'Scrapinghub-Fnac(crawl-diffusion@fnacdarty.com)'}
    new_url = 'https://z0ypi1plpq-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(3.35.1)%3B%20Browser&x-algolia-application-id=Z0YPI1PLPQ&x-algolia-api-key=7f198efa7726638c56ef6e008dd72cdf'
    request_dict = {
        "requests": [{"indexName": "prod_darty", "params": f"query={marque}%20{reference_constructeur}&hitsPerPage=4"},
                     {"indexName": "prod_autosuggest_terms",
                      "params": f"query={marque}%20{reference_constructeur}&hitsPerPage=5"}]}
    req_1 = requests.post(new_url, headers=headers, json=request_dict)
    try:
        j = json.loads(req_1.text)
        ean = j['results'][0]['hits'][0]['ean'][0]
        codic = j['results'][0]['hits'][0]['codic']
        darty_price = str(j['results'][0]['hits'][0]['priceSort'])
        darty_price = '{},{}'.format(darty_price[:-2], darty_price[-2:])
        return (ean, codic, darty_price)
    except Exception:
        return None


def get_darty_ean(marque, reference_constructeur, check_ean=False):
    headers = {
        'User-Agent': 'Scrapinghub-Fnac(crawl-diffusion@fnacdarty.com)'}
    new_url = 'https://z0ypi1plpq-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(3.35.1)%3B%20Browser&x-algolia-application-id=Z0YPI1PLPQ&x-algolia-api-key=7f198efa7726638c56ef6e008dd72cdf'
    request_dict = {
        "requests": [{"indexName": "prod_darty", "params": f"query={marque}%20{reference_constructeur}&hitsPerPage=4"},
                     {"indexName": "prod_autosuggest_terms",
                      "params": f"query={marque}%20{reference_constructeur}&hitsPerPage=5"}]}
    req_1 = requests.post(new_url, headers=headers, json=request_dict)
    try:
        j = json.loads(req_1.text)
        ean = j['results'][0]['hits'][0]['ean']
        if check_ean is not False:
            for content in j['results'][0]['hits']:
                if content['ean'] == check_ean:
                    ean = check_ean
                    break
                else:
                    ean = None
        return ean
    except Exception:
        return None


def title_converter(input_title: str):
    output_title = input_title.strip().lower().replace('/', ' ').replace('-', ' ').replace(',', ' ').replace('(',
                                                                                                             ' ').replace(
        ')', ' ').replace('  ', ' ')
    return output_title


def ean_converter(ean: str):
    return '{:0>13}'.format(ean)


def compare_strings(str1, str2):
    ratio = fuzz.partial_token_sort_ratio(str1, str2)
    return ratio


def validate_ratio(ratio, min_ratio=86):
    if ratio > min_ratio:
        return True
    return False


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


def get_dataframe_3_old():
    converter = {'EAN': ean_converter,
                 'PRODUIT': title_converter}
    wells = get_data_file('albert', 'data/occasions/occasion_numbers_3_old.csv')
    df = pd.read_csv(StringIO(wells), sep=';', converters=converter)
    df.drop_duplicates(subset='EAN', keep='first', inplace=True, ignore_index=True)
    df['PRODUIT'] = df['MARQUE'] + ' ' + df['PRODUIT']
    return df[['EAN', 'MARQUE', 'PRODUIT']]


def retrieve_dataframe_from_packaged_data():
    converter = {'EAN': ean_converter}
    wells = get_data_file('albert', 'data/occasions/occasions_20201201.csv')
    df = pd.read_csv(StringIO(wells), converters=converter, sep=';')
    if 'EAN' not in df.columns:
        df = pd.read_csv(StringIO(wells), converters=converter, sep=',')
    return df


def retrieve_dataframe_from_ftp():
    converter = {'EAN': ean_converter}
    ftp = ftp_wrapper.establish_connection(config.fnac_deposit_ftp_access)
    df = None
    if ftp is None:
        return
    if ftp_wrapper.change_directory(ftp, '/CRAWL/seconde_vie'):
        local_file = '/tmp/occasions_dataframe.xlsx'
        seconde_vie_filename = ftp_wrapper.get_most_recent_file(ftp)
        if ftp_wrapper.download_file(ftp, local_file, seconde_vie_filename):
            df = pd.read_excel(local_file, converters=converter)
    return df


def get_dataframe_for_crawlers():
    df = retrieve_dataframe_from_ftp()
    df.rename(columns=lambda x: x.strip(), inplace=True)
    df.drop_duplicates(subset='EAN', keep='first', inplace=True, ignore_index=True)
    pos = 3
    column_product_name = df.columns[pos]
    df.rename(columns={'LMARQUE': 'MARQUE'}, inplace=True)
    df.rename(columns={column_product_name: 'PRODUIT'}, inplace=True)
    df['PRODUIT'] = df['MARQUE'] + ' ' + df['PRODUIT']
    return df[['EAN', 'MARQUE', 'PRODUIT']]


def get_dataframe_for_final_file():
    df = retrieve_dataframe_from_ftp()
    df.rename(columns=lambda x: x.strip(), inplace=True)
    df.drop_duplicates(subset='EAN', keep='first', inplace=True, ignore_index=True)
    return df


def get_occasions_data():
    final_df = get_dataframe_for_crawlers()
    return final_df


def fuzzymatch(rows, element):
    element = element.lower().replace('é', '').replace('à', 'a').replace('è', 'e')
    element = title_converter(element)
    current_validated_ratio = 0
    current_validated_ean = None
    for ean, marque, prd in zip(rows['EAN'], rows['MARQUE'], rows['PRODUIT']):
        ratio = compare_strings(prd.lower(), element)
        if validate_ratio(ratio) and current_validated_ratio < ratio:
            current_validated_ratio = ratio
            current_validated_ean = ean
    if current_validated_ratio > 86:
        return current_validated_ean
    return None

