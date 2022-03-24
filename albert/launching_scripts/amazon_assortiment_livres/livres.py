#!/usr/bin/env python3
import keepa
from config import KEEPA_APIKEY, SHUB_APIKEY
import pandas as pd
import openpyxl
import os.path
import requests
import json
from pathlib import Path


class LivresOneShot:
    name = 'livres_one_shot'

    def __init__(self):
        self.api = keepa.Keepa(KEEPA_APIKEY)
        self.excel_file_location = '/Users/openvalue/crawls_FD/albert/data/amazon_exploration_livres_ean/amazon_livres.xlsx'
        self.csv_file_location = '/Users/openvalue/crawls_FD/albert/data/amazon_exploration_livres_ean/amazon_livres_csv_ean.csv'
        self.size = os.path.getsize(self.excel_file_location)
        self.output_file = '/Users/openvalue/crawls_FD/albert/data/toto_ean_asin.csv'
        self.df = self.get_crawl_init_data()
        self.project_id = 436828
        self.response_data = None
        self.response_string = self.list_data_from_frontier()
        self.urls_sample = None

    def get_crawl_init_data(self):
        if Path(self.output_file).is_file():
            return pd.read_csv(self.output_file, sep=';')
        return self.read_excel_in_chunks(excel_file_path=self.excel_file_location)

    def convert_ean_to_asin(self, **kwargs):
        if kwargs and 'ean' in kwargs.keys():
            query = self.api.query(kwargs['ean'], product_code_is_asin=False)
            return '{}'.format(query[0]['asin'])

    def convert_eans_to_asins(self, **kwargs):
        query = self.api.query(kwargs['eans'], product_code_is_asin=False)
        return query

    def read_excel_in_chunks(self, **kwargs):
        # loading data with pandas doesnt work
        # self.df = pd.read_excel(self.excel_file_location, index_col=0, engine="openpyxl")
        if 'excel_file_path' in kwargs.keys():
            excel_file_location = kwargs['excel_file_path']
        else:
            excel_file_location = self.excel_file_location
        wb = openpyxl.load_workbook(filename=excel_file_location, read_only=True)
        ws = wb.active
        rows = ws.rows
        first_row = [cell.value for cell in next(rows)]
        data = []
        # data = [{'EAN': cell.value for key, cell in zip(first_row, row)} for row in rows]
        for row in rows:
            if len(data) > 10:
                break
            record = {key: cell.value for key, cell in zip(first_row, row)}
            # for key, cell in zip(first_row, row):
            #     record[key] = cell.value
            data.append(record)
        df = pd.DataFrame(data)
        return df

    def add_asin_column_to_csv(self):
        self.df.drop_duplicates(inplace=True, ignore_index=True)
        self.df = self.df.assign(ASIN='')
        keepa_rate = 10
        products = self.convert_eans_to_asins(eans=self.df['EAN'].astype(str)[:keepa_rate].to_numpy())
        for product in products:
            for ean in product['eanList']:
                self.df.loc[self.df['EAN'].astype(str) == ean, 'ASIN'] = product['asin']
        self.df.to_csv('/Users/openvalue/crawls_FD/albert/data/toto_ean_asin.csv', sep=';', index_label=False,
                       index=False, mode='a', header=False)

    def upload_data_to_frontier(self):
        data_as_dict = [{'fp': f'dp/{asin}', 'qdata': {'depth': 1, 'ean': ean}} if asin else None for index, (ean, asin)
                        in self.df.iterrows()]
        data_as_json_string = '\n'.join(json.dumps(elem) for elem in data_as_dict)
        response = requests.post(f'https://storage.scrapinghub.com/hcf/{self.project_id}/amazon_livres/s/amazon.fr',
                                 data=data_as_json_string, auth=(SHUB_APIKEY, ''))
        self.response_data = response
        return self.response_data.content

    def list_data_from_frontier(self):
        response = requests.get('https://storage.scrapinghub.com/hcf/436828/amazon_livres/s/amazon.fr/f',
                                auth=(SHUB_APIKEY, ''))
        self.response_string = response.content
        return self.response_string

    def transform_data_from_frontier(self):
        if self.response_string:
            string = self.response_string.decode('utf-8')
            tab = string.split('\n')
            output_tab = [json.loads(elem) for elem in tab if elem]
            self.urls_sample = output_tab
        return self.urls_sample

    # def transform_ean_csv_to_ean_asin_csv(self):
    #     for data in get_data_in_chunk():
    #         asin_data = get_asins(data)
    #         output_df_in_chunk = add_asin_data_to_chunk(asin_data)
    #         append_to_csv(output_df_in_chunk)

    def clean_csv(self):
        for data in pd.read_csv(self.csv_file_location, chunksize=80):
            df = data.drop_duplicates(inplace=True)
            df.to_csv('/tmp/new_data.csv', mode='a', header=False)
            # eans = data['EAN'].astype(str)[:len(data['EAN'])].to_numpy()

            # products = self.convert_eans_to_asins(eans=eans)
            # for product in products:
            #     for ean in product['eanList']:
            #         data.loc[data['EAN'].astype(str) == ean, 'ASIN'] = product['asin']

    def clean_data(self, data):
        data.drop_duplicates(inplace=True)


def clean_csv():
    ean_list = []
    csv_file_location = '/tmp/correct_data.csv'
    for data in pd.read_csv(csv_file_location, chunksize=10000):
        data.drop_duplicates(inplace=True, ignore_index=True)
        ean_list.extend(data['EAN'].values.tolist())
    df = pd.DataFrame(ean_list, columns=['EAN'])
    df.drop_duplicates(subset=['EAN'], keep='first', inplace=True, ignore_index=True)
    df.to_csv('/tmp/correct_data_2.csv', index=False)


def csv_chunks_to_asin():
    api = keepa.Keepa(KEEPA_APIKEY)
    csv_file_location = '/tmp/correct_data.csv'
    # asin_list = []
    asin_dict = {}
    for data in pd.read_csv(csv_file_location, chunksize=120):
        eans = data['EAN'].astype(str).str.zfill(13).to_numpy()
        tmp_dict = dict.fromkeys(eans)
        products = api.query(eans, product_code_is_asin=False)
        for product in products:
            for ean in product['eanList']:
                if ean in eans:
                    # asin_list.append((ean, product['asin']))
                    tmp_dict[ean] = product['asin']
                    break
        asin_dict.update(tmp_dict)
        if len(asin_dict) > 2000:
            print('toto')
        # sleep(60)
        # if len(asin_dict) > 120:
        #     break
    df = pd.DataFrame(asin_dict.items(), columns=['EAN', 'ASIN'])
    # df.drop_duplicates(subset=['ASIN'], keep='first', inplace=True, ignore_index=True)
    df.to_csv('/Users/openvalue/crawls_FD/albert/data/amazon_exploration_livres_ean/livres_ean_asin_final.csv',
              index=False, mode='a')


# csv_chunks_to_asin()
def treat_amazon_ean_asin_csv():
    csv_file_location = '/Users/openvalue/crawls_FD/albert/data/amazon_exploration_livres_ean/livres_ean_asin_final.csv'
    df = pd.read_csv(csv_file_location)
    df.drop_duplicates(subset=['EAN'], inplace=True)
    df.dropna(inplace=True)
    print('toto')


treat_amazon_ean_asin_csv()
