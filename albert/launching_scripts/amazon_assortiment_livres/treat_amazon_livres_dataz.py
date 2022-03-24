import pandas as pd


def drop_useless_columns(df: pd.DataFrame, columns):
    new_df = df.drop(columns=columns)
    return new_df


def drop_useless_rows(df: pd.DataFrame, target_column, target_value):
    new_df = df[df[target_column] != target_value]
    return new_df


def drop_empty_rows(df: pd.DataFrame):
    new_df = df.dropna()
    return new_df


def merge_dataframes_on_ean(df1: pd.DataFrame, df2: pd.DataFrame):
    new_df = df1.merge(df2, on='EAN', how='outer')
    new_df.drop_duplicates(inplace=True)
    return new_df


def append_dataframes(df1: pd.DataFrame, df2: pd.DataFrame):
    new_df = df1.append(df2)
    return new_df


def get_dataframes_from_csv(file_location):
    df = pd.read_csv(file_location)
    return df


# complete_file_list = ['/tmp/aaa.csv', '/tmp/bbb.csv', '/tmp/ccc.csv']
complete_file_list = ['items_amazon_61.csv',
                      'items_amazon_60.csv',
                      'items_amazon_58.csv',
                      'items_amazon_57.csv']

final_df = pd.read_csv(complete_file_list[0])
partial_file_list = complete_file_list[1:]
for file in complete_file_list[1:]:
    file_df = pd.read_csv(file)
    final_df.append(file_df)
