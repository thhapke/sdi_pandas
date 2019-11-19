import pandas as pd
import json
import logging

from lightgbm import LGBMRegressor

import textfield_parser.textfield_parser as tfp

EXAMPLE_ROWS = 5


def process(df_msg):
    prev_att = df_msg.attributes
    df = df_msg.body
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation

    model = LGBMRegressor(
        n_estimators=200,
        learning_rate=0.03,
        num_leaves=32,
        colsample_bytree=0.9497036,
        subsample=0.8715623,
        max_depth=8,
        reg_alpha=0.04,
        reg_lambda=0.073,
        min_split_gain=0.0222415,
        min_child_weight=40)

    att_dict['config']['train columns'] = api.config.train_cols
    train_cols = tfp.read_list(api.config.train_cols, df.columns)

    att_dict['config']['label'] = api.config.label
    label = tfp.read_value(api.config.label)
    if not label :
        raise ValueError('Label is mandatory')

    # cast to categorical dtype
    for c in df[train_cols].select_dtypes(include='category').columns :
        unique_num = len(df[c].unique())
        nan_num = df[c].isna().count()
        logging.debug('Cast to category - {}: unique {}, nan: {} of {}'.format(c,unique_num,nan_num,df.shape[0]))
        df[c] = df[c].cat.codes
        df[c] = df[c].astype('int32')

    if pd.api.types.is_categorical(df[label]) :
        df[label] = df[label].astype('category')
        logging.debug('Cast label to <category>')
        df[label] = df[label].cat.codes
        df[label] = df[label].astype('int32')

    print(df.select_dtypes(include='category').head(10))
    logging.debug('Train with {} features'.format(len(train_cols)))
    print(train_cols)
    model.fit(df[train_cols], df[label], eval_metric='auc')

    ###### end of doing calculation

    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty:
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'TrainLGBClassifier'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    return api.Message(attributes=att_dict, body=model)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''


class test:
    SIMPLE = 0


actual_test = test.SIMPLE

try:
    api
except NameError:
    class api:

        def get_default_input():
            df = pd.DataFrame(
                {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, 3, 4, 5], 'col3': [2, 3, 4, 5, 6],
                 'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 6.7, 8.2, 9, 10.1]})

            attributes = {'format': 'csv', 'name': 'DF_name'}

            return api.Message(attributes=attributes, body=df)

        def set_config(test_scenario):
            pass

        class config:
            train_cols = 'Not col5'
            label = 'col5'


        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if not isinstance(msg, str):
                print(msg.body.head(10))
            # else :
            #    print(msg)
            pass

        def set_port_callback(port, callback):
            msg = api.get_default_input()
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            callback(msg)

        def call(msg, config):
            api.config = config
            result = process(msg)
            return result, json.dumps(result.attributes, indent=4)


def interface(msg):
    result = process(msg)
    api.send("outModel", result)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
#api.set_port_callback("inDataFrameMsg", interface)

