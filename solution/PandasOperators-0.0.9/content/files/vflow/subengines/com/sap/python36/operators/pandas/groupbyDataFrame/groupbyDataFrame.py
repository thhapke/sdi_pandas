import pandas as pd
import re
import json


def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body

    att_dict = dict()
    att_dict['config'] = dict()
    att_dict['memory'] = dict()
    att_dict['operator'] = 'groupbyDataFrame'
    att_dict['name'] = prev_att['name']

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

    att_dict['memory']['mem_usage'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = list(df.columns)
    att_dict['number_columns'] = len(att_dict['columns'])
    att_dict['number_rows'] = len(df.index)
    att_dict['example_row_1'] = str(df.iloc[0,:].tolist())

    return  api.Message(attributes = att_dict,body=df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''

class test :
    VERIVOX_1 = 6
    SIMPLE = 0

actual_test = test.VERIVOX_1

try:
    api
except NameError:
    class api:

        def set_test(test_scenario):
            print('TEST SCENARIO: ' + str(test_scenario))
            if test_scenario == test.VERIVOX_1:
                df = pd.read_csv("/Users/d051079/OneDrive - SAP SE/GitHub/OptRanking/data/verivox/verivox-sample.csv", sep=';')
            else : #test_scenario == test.SIMPLE
                df = pd.DataFrame(
                    {'icol': [1, 1, 1, 1, 2], 'xcol 2': ['A', 'A', 'B', 'B', 'C'], 'xcol 3': [1, 1,2,2,3],'xcol4': ['a', 'a','b','a','b']})
            attributes = {'format': 'csv','name':'DF_name'}

            return api.Message(attributes=attributes,body=df)

        def set_config(test_scenario) :
            if test_scenario == test.VERIVOX_1:
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

            # called by 'integrated/pipeline-test simulation

        def test_call(msg):
            print('EXTERNAL CALL of module:' + __name__)
            api.set_config(actual_test)
            result = process(msg)
            api.send("outDataFrame", result)
            return result



def interface(msg):
    result = process(msg)
    api.send("outDataFrameMsg", result)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
api.set_port_callback("inDataFrameMsg", interface)

