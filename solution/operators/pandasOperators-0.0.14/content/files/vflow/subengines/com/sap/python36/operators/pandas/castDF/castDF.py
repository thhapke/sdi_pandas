import pandas as pd
import re
import json

EXAMPLE_ROWS =5


def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body

    att_dict = dict()
    att_dict['config'] = dict()

    # dtypes mapping
    castmap = None
    if api.config.cast and not api.config.cast == 'None':
        # mapping
        colmaps = [x.strip() for x in api.config.cast.split(',')]
        castmap = {cm.split(':')[0].strip().strip("'").strip('"'): \
                       cm.split(':')[1].strip().strip("'").strip('"') for cm in colmaps}

    for col, casttype in castmap.items() :
        if api.config.round :
            df[col] = df[col].round()
        df[col] = df[col].astype(casttype)

    ##############################################
    #  final infos to attributes and info message
    ##############################################
    att_dict['operator'] = 'castDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]
    if 'id' in prev_att.keys() :
        att_dict['id'] = prev_att['id'] + '; ' + att_dict['operator'] + ': ' + str(id(df))
    else :
        att_dict['id'] = att_dict['operator'] + ': ' + str(id(df))

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return  api.Message(attributes = att_dict,body=df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''

class test :
    ORDER_HEADERS = 2
    PORTAL2 = 3
    PORTAL2_1 = 4
    PORTAL2_2 = 5
    ADDRESS = 6
    SIMPLE = 0

actual_test = test.ADDRESS

try:
    api
except NameError:
    class api:

        def set_test(test_scenario):
            df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100,200,300,400,500]})

            attributes = {'format': 'csv','name':'DF_name'}

            return api.Message(attributes=attributes,body=df)

        def set_config(test_scenario) :
            api.config.cast = "'col 2' : 'float32', 'col3' : 'uint8'"  # operators comparisons: <,>,=,!=
            api.config.round = True

        class config:
            cast =  "'col 2' : 'float32', 'col3' : 'uint8'"
            round = False


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
            msg = api.set_test(actual_test)
            api.set_config(actual_test)
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
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
api.set_port_callback("inDataFrameMsg", interface)

