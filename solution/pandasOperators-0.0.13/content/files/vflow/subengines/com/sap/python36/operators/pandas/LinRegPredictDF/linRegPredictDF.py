import pandas as pd
import numpy as np
import re
import json

from sklearn.linear_model import LinearRegression

EXAMPLE_ROWS = 5

def process(msg,coef_msg):

    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation
    # segment columns
    segment_cols = coef_msg.attributes['segmentation_columns']

    # regression columns
    regression_cols = coef_msg.attributes['regression_columns']

    # prediction column
    prediction_col = coef_msg.attributes['prediction_column']

    # merge data and coef df
    df = pd.merge(df,coef_msg.body,how='inner',left_on=segment_cols,right_on=segment_cols)

    pcol = 'p_' + prediction_col
    def predict(x) :
        x[pcol] = np.dot(x['coef'],x[regression_cols].values) + x['intercept']
        return x

    df = df.apply(predict,axis = 1,result_type = None)
    df.drop(columns=['coef', 'intercept'], inplace=True)

    # cast type of prediction col from prediction variable
    if df[prediction_col].dtype == np.integer :
        df[pcol] = df[pcol].round().astype(df[prediction_col].dtype)

    if api.config.prediction_col_only :
        df = df[segment_cols + [prediction_col] + [pcol]]
    att_dict['config']['prediction_col_only'] = api.config.prediction_col_only

    #print(df[[pcol, prediction_col]].head(5))
    #print(df.head(10))

    ###### end of doing calculation


    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'regressionTrainingDataFrame'
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
            api.config.prediction_col_only = True

        class config:
            prediction_col_only = True

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

        def call(data_msg,coef_msg,config):
            api.config = config
            msg = process(data_msg,coef_msg)
            return msg, json.dumps(msg.attributes, indent=4)


def interface(msg,coef_df):
    msg = process(msg,coef_df)
    api.send("outDataMsg", msg)
    info_str = json.dumps(msg.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
api.set_port_callback(["inDataMsg","inCoefMsg"], interface)

