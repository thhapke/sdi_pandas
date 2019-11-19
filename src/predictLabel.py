import pandas as pd
import logging

import json

import textfield_parser.textfield_parser as tfp

from sklearn.metrics import accuracy_score

EXAMPLE_ROWS = 5


def process(df_msg,model_msg):

    prev_att = df_msg.attributes
    df = df_msg.body
    model = model_msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    exclude_cols = []
    ###### start of doing calculation
    att_dict['config']['prediction_cols'] = api.config.prediction_cols
    prediction_cols = tfp.read_list(api.config.prediction_cols,list(df.columns))
    if not prediction_cols :
        raise ValueError('Prediction needs prediction columns')

    print('Predict with features: {}'.format(len(prediction_cols)))
    print(prediction_cols)
    df['prediction'] = model.predict(df[prediction_cols])

    if api.config.round :
        df['prediction'] = df['prediction'].round().astype('int64')
    att_dict['config']['round'] = api.config.round

    att_dict['config']['label'] = api.config.label
    label = tfp.read_value(api.config.label)
    if label :
        att_dict['accuracy'] =  accuracy_score(df[label], df['prediction']) * 100
        logging('Accurracy: ',att_dict['accuracy'])
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

    return  api.Message(attributes = att_dict,body=df),api.Message(attributes = att_dict,body=None)


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
                {'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100,200,300,400,500]})

            attributes = {'format': 'csv','name':'DF_name'}

            return api.Message(attributes=attributes,body=df),api.Message(attributes=attributes,body=None)

        class config:
            prediction_cols = 'None'
            round = False
            label = 'None'

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
            msg, model = api.get_default_input()
            if isinstance(port,list) :
                port = str(list)
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            callback(msg,model)

        def call(data_msg,model_msg,config):
            api.config = config
            result = process(data_msg,model_msg)
            return result, json.dumps(result.attributes, indent=4)


def interface(data_msg,model_msg):
    msg = process(data_msg, model_msg)
    api.send("outDataFrame", msg)
    info_str = json.dumps(msg.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
#api.set_port_callback(["inData","Model"], interface)