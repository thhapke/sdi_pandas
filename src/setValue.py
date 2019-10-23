import pandas as pd
import numpy as np
import re
import json

EXAMPLE_ROWS = 5



def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation

    # map_values : column1: {from_value: to_value}, column2: {from_value: to_value}
    if api.config.map_values and not api.config.map_values.upper() == 'NONE' :
        # Read parameter value
        map_dict = dict()
        map_list = [x.strip() for x in api.config.map_values.split(',')]
        for map_col_value in map_list :
            col = map_col_value.split(':')[0].strip().strip("'").strip('"')
            from_val = map_col_value.split(':')[1].strip().strip("{").strip('"').strip("'")
            to_val = map_col_value.split(':')[2].strip().strip("}").strip('"').strip("'")
            if to_val.upper() == 'NAN' or to_val.upper() == 'NULL' :
                to_val = np.nan
            map_dict[col] = {from_val:to_val}

        att_dict['config']['set_value'] = str(map_dict)

        # set parameter value
        df.replace(map_dict,inplace=True)

    # Fill NaN value : column1: value, column2: value,
    if api.config.fill_nan_values and not api.config.fill_nan_values.upper() == 'NONE':
        # Read parameter value
        map_dict = dict()
        map_list = [x.strip() for x in api.config.fill_nan_values.split(',')]
        for map_col_value in map_list:
            col = map_col_value.split(':')[0].strip().strip("'").strip('"')
            val = map_col_value.split(':')[1].strip().strip("{").strip('}')
            map_dict[col] = val
        att_dict['config']['fill_nan_values'] = str(map_dict)


        # set parameter value
        df.fillna(map_dict,inplace=True)

    ###### end of doing calculation


    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'selectDataFrame'
    att_dict['name'] = prev_att['name']
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

        def set_test(test_scenario):
            df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100,200,300,400,500]})

            attributes = {'format': 'csv','name':'DF_name'}

            return api.Message(attributes=attributes,body=df)

        def set_config(test_scenario) :
            pass

        class config:
            map_values = 'None'
            fill_nan_values = 'None'

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
#api.set_port_callback("inDataFrameMsg", interface)

