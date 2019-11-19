import pandas as pd
import re
import json

from fuzzywuzzy import fuzz


def process(df_msg,value_dict_list):

    prev_att = df_msg.attributes
    df = df_msg.body

    att_dict = dict()
    att_dict['config'] = dict()
    att_dict['memory'] = dict()
    att_dict['operator'] = 'fuzzySelectDataFrame'
    att_dict['name'] = prev_att['name']


    for v_dict in value_dict_list :
        # get matching for values in dict
        num_cols = len(v_dict)
        cols_list = list(v_dict.keys())
        def get_ratio(row):
            sc = 0
            for col,value in v_dict.items() :
                sc = sc + fuzz.token_sort_ratio(row[col], value)
            return sc/num_cols
        df['score']= df.apply(get_ratio,axis = 1)
        # get best matching and store index in v_dict
        v_dict['score'] = df['score'].max()
        v_dict['match'] = df.loc[df['score']== v_dict['score'],cols_list].squeeze().to_dict()
        print(v_dict)

    df.drop(columns=['score'],inplace=True)

    att_dict['memory']['mem_usage'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = list(df.columns)
    att_dict['number_columns'] = len(att_dict['columns'])
    att_dict['number_rows'] = len(df.index)

    if df.empty :
        raise ValueError('DataFrame is empty')

    att_dict['example_row_1'] = str(df.iloc[0,:].tolist())

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

        def set_test(test_scenario):
            df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'col2': ['Am Rhein','Wagnerstr','Rheinbad','Am Hang', 'Rheinstr.'], \
                 'col3': ['ABC','ABC Company','B&C Ltd','Bang & Cool GmbH','Achdu']})

            search_values = [{'col2':'Am Rheinbad','col3':'ABC'},{'col2':'Rheingaustr','col3':'B & C'}]

            attributes = {'format': 'df','name':'DF_name'}

            return api.Message(attributes=attributes,body=df),search_values

        def set_config(test_scenario) :
            api.config.fuzzy_logic = 'ratio'  # partial_ratio, token_sort_ratio,token_set_ratio
            api.config.fuzzy_limit = 50  # match criteria

        class config:
            fuzzy_logic= 'ratio'  # partial_ratio, token_sort_ratio,token_set_ratio
            fuzzy_limit = 90  # match criteria

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if not isinstance(msg,str) :
                print(msg.body.head(100))
            #else :
            #    print(msg)
            pass

        def set_port_callback(port, callback):
            msg, search_values = api.set_test(actual_test)
            api.set_config(actual_test)
            print("Call \"" + callback.__name__ + "\"  messages port \"" + str(port) + "\"..")
            callback(msg,search_values)

        def call(msg,config):
            api.config = config
            result = process(msg)
            return result, json.dumps(result.attributes, indent=4)


def interface(msg,value_dict):
    result = process(msg,value_dict)
    api.send("outDataFrameMsg", result)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
api.set_port_callback(["inDataFrameMsg","searchValues"], interface)

