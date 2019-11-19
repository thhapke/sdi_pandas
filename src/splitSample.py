import pandas as pd
import numpy as np
import logging
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

    ###### start of doing calculation
    warning = ''
    att_dict['config']['split'] = api.config.split
    if api.config.split > df.shape[0] :
        warning = 'Split larger than whole sample'
        split = 1
    elif api.config.split > 1 :
        split = api.config.split / df.shape[0]
    else :
        split = api.config.split

    att_dict['config']['to_category'] = api.config.to_category
    if api.config.to_category :
        for col in df.select_dtypes(include=np.object).columns:
            unique_num = len(df[col].unique())
            nan_num = df[col].isna().count()
            logging.debug('Cast to category - {}: unique {}, nan: {} of {}'.format(col, unique_num, nan_num, df.shape[0]))
            df[col] = df[col].astype('category')

    att_dict['config']['label'] = api.config.label
    label = tfp.read_value(api.config.label)
    if label :
        label_vals = list(df[label].unique())
        tdf = list()
        for lab in label_vals :
            tdf.append(df.loc[df[label] == lab].sample(frac=split, random_state=api.config.seed))
        train_df = pd.concat(tdf)
    else :
        train_df = df.sample(frac=split, random_state=api.config.seed)  # random state is a seed value

    test_df = df.drop(train_df.index)
    ###### end of doing calculation

    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'sampleDataFrame'
    att_dict['warning'] = warning
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return  api.Message(attributes = att_dict,body=train_df), api.Message(attributes = att_dict,body=test_df)


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

        def set_default_input():
            df = pd.DataFrame(
                {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, None, 4, 5], 'col3': [2, 3, 4, 5, 6],
                 'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 6.7, 8.2, None, 10.1]})
            attributes = {'format': 'csv','name':'DF_name'}

            return api.Message(attributes=attributes,body=df)

        class config:
            label = 'icol'
            split = 0.4
            seed = 1
            to_category = False

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
            msg = api.set_default_input()
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            callback(msg)

        def call(msg,config):
            api.config = config
            msg_train, msg_test = process(msg)
            return msg_train, msg_test, json.dumps(msg_train.attributes, indent=4)


def interface(msg):
    train_msg, test_msg = process(msg)
    api.send("outTrain", train_msg)
    api.send("outTest", test_msg)
    info_str = json.dumps(train_msg.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
#api.set_port_callback("inDataFrameMsg", interface)

