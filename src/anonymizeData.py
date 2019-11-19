import pandas as pd
import numpy as np
import math
import random, string
import json

import textfield_parser.textfield_parser as tfp

EXAMPLE_ROWS = 5


def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()
    warning = ''
    ###### start of doing calculation

    att_dict['config']['to_nan'] = api.config.to_nan
    to_nan = tfp.read_value(api.config.to_nan,test_number=False)
    if to_nan:
        df.replace(to_nan, np.nan, inplace=True)

    ## Replace with Alphanumeric
    att_dict['config']['anonymize_categoricals'] = api.config.replace_categoricals
    anonymize_categoricals = tfp.read_list(api.config.replace_categoricals, list(df.columns))
    if anonymize_categoricals :
        keep_terms = {"Yes": "Y", "yes": "Y", "y": "Y", "Y": "Y", "N": "N", "n": "N", "No": "N", "no": "N"}
        for c in df[anonymize_categoricals].select_dtypes(include='object') :
            unique_list = df[c].unique()
            n = int(math.log10(len(unique_list))) + 2
            rep_map = { x:''.join(random.choices(string.ascii_letters, k = n))  for x in unique_list if isinstance(x,str) }
            for ktk, ktv in keep_terms.items():
                if ktk in rep_map.keys():
                    rep_map[ktk] = ktv
            df[c].replace(rep_map,inplace=True)

    att_dict['config']['anonymize_numbers'] = api.config.linearshift_numbers
    anonymize_numbers = tfp.read_list(api.config.linearshift_numbers, list(df.columns))
    if anonymize_numbers :
        for c in df.select_dtypes(include='int'):
            unique_i = df[c].unique()
            if not (len(unique_i) == 2 and 0 in unique_i and 1 in unique_i) :
                df[c] = df[c] * random.randint(0,100)
            else :
                print("Values 0 and c for {} with {} ".format(c, str(unique_i)))
        for c in df.select_dtypes(include='float'):
            df[c] = df[c] * random.random()

    att_dict['config']['anonymize_ids'] = api.config.replace_with_ids
    anonymize_ids = tfp.read_list(api.config.replace_with_ids, list(df.columns))
    if anonymize_ids :

        for c in anonymize_ids:
            unique_list = df[c].unique()
            len_unique = len(unique_list)
            if  df.shape[0] == len_unique :
                df[c] = random.sample(range(df[c].min()*100,df[c].max()*100), df.shape[0])
            else :
                warning = warning + "Anonymize ID detected  duplicates. Column: " + c + "; "
                if pd.api.types.is_integer_dtype(df[c]) :
                    map_dict = dict(zip(unique_list,random.sample(range(df[c].min() * 100, df[c].max() * 100), len_unique)))
                elif pd.api.types.is_object_dtype(df[c].dtype) :
                    n = len_unique * 100
                    map_dict = dict(zip(unique_list,random.sample(range(n, n*10),k=len_unique)))
                else :
                    raise ValueError("Dtype <{}> cannot be anonymized_id".format(df[c].dtype))
                df[c].replace(map_dict,inplace = True)

    att_dict['config']['enumerate_cols'] = api.config.enumerate_cols
    att_dict['config']['prefix_cols'] = api.config.prefix_cols
    enumerate_cols = tfp.read_list(api.config.enumerate_cols,list(df.columns))
    if enumerate_cols :
        ncols = int(math.log10(len(enumerate_cols)))+1
        prefix_cols = tfp.read_value(api.config.prefix_cols)
        if not prefix_cols :
            prefix_cols = 'Att_'
        cols_map ={ oc : prefix_cols + str(i).zfill(ncols) for i,oc in enumerate(enumerate_cols)}
        df.rename(columns=cols_map,inplace=True)

    ###### end of doing calculation


    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'selectDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict["warnings"] = warning
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return  api.Message(attributes = att_dict,body=df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''

class test :
    SIMPLE = 0

actual_test = test.SIMPLE

try:
    api
except NameError:
    class api:
        def get_default_input():
            df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'col2': ['Cydia','Cydia', None, 'Dani', 'Liza'],\
                 'col3': ['Frank', 'Stephen', 'Hubert', 'Hubert', 'Sue'],
                 'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 6.7, 8.2, None, 10.1]})
            attributes = {'format': 'csv', 'name': 'DF_name'}
            return api.Message(attributes=attributes, body=df)


        def set_config(test_scenario) :
            pass

        class config:
            to_nan = '0'
            replace_categoricals = 'col2, col3'
            linearshift_numbers = 'col2, col4,col5'
            replace_with_ids = 'icol'
            enumerate_cols = "Not icol"
            prefix_cols = 'Att'

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if not isinstance(msg,str) :
                print(msg.body.head(10))
            #else :
            #    print(msg)
            pass

        def set_port_callback(port, callback):
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            msg = api.get_default_input()
            callback(msg)

        def call(msg,config):
            api.config = config
            result = process(msg)
            return result, json.dumps(result.attributes, indent=4)


def interface(msg):
    result = process(msg)
    api.send("outDataFrameMsg", result)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
#api.set_port_callback("inDataFrameMsg", interface)

