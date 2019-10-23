import pandas as pd
import re
import json

EXAMPLE_ROWS = 5

def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body

    att_dict = dict()
    att_dict['config'] = dict()


    # groupby list
    cols = [x.strip().replace("'",'').replace('"','') for x in api.config.groupby.split(',')]

    # mapping
    colmaps = [x.strip() for x in api.config.aggregation.split(',')]
    colagg = {cm.split(':')[0].strip().replace("'", "").replace('"', ''): \
                   cm.split(':')[1].strip().replace("'", "").replace('"', '') for cm in colmaps}

    # groupby
    df = df.groupby(cols, as_index=api.config.index).agg(colagg)

    # drop col
    if api.config.drop_columns and not api.config.drop_columns.upper() == 'NONE':
        dropcols = [x.strip().replace("'", '').replace('"', '') for x in api.config.drop_columns.split(',')]
        df.drop(columns=dropcols,inplace=True)


    ##############################################
    #  final infos to attributes and info message
    ##############################################
    att_dict['operator'] = 'groupbyDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = list(df.columns)
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0, example_rows):
        att_dict['row_' + str(i)] = str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return  api.Message(attributes = att_dict,body=df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''

class test :
    PORTAL_1 = 6
    SIMPLE = 0

actual_test = test.PORTAL_1

try:
    api
except NameError:
    class api:

        def set_test(test_scenario):
            print('TEST SCENARIO: ' + str(test_scenario))
            if test_scenario == test.PORTAL_1:
                df = pd.read_csv("/Users/d051079/OneDrive - SAP SE/GitHub/OptRanking/data/verivox/verivox-sample.csv", sep=';')
            else : #test_scenario == test.SIMPLE
                df = pd.DataFrame(
                    {'icol': [1, 1, 1, 1, 2], 'xcol 2': ['A', 'A', 'B', 'B', 'C'], 'xcol 3': [1, 1,2,2,3],'xcol4': ['a', 'a','b','a','b']})
            attributes = {'format': 'csv','name':'DF_name'}

            return api.Message(attributes=attributes,body=df)

        def set_config(test_scenario) :
            if test_scenario == test.PORTAL_1:
                api.config.groupby = "'Exportdatum', 'Postleitzahl', 'Ort', 'Verbrauchsstufe','Rang'" # list
                api.config.aggregation = "'Gesamtpreis':'mean','Ortsteil':'count'"  # map key:value
                api.config.index = True
                api.config.drop_columns = "'Ortsteil'"
            else : # SIMPLE'
                api.config.groupby = "'icol', 'xcol 2'"  # list
                api.config.aggregation = "'xcol 3':'sum','xcol4':'count'"  # map key:value
                api.config.index = False
                api.config.drop_columns = "'xcol4'"

        class config:
            groupby = "'icol'"  # list
            aggregation = "'xcol 3':'sum','xcol4':'count'"  # map key:value
            index = True
            drop_columns = "'xcol4'"

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

