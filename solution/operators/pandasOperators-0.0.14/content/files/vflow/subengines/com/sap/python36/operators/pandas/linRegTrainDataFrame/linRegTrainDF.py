import pandas as pd
import re
import json

from sklearn.linear_model import LinearRegression

EXAMPLE_ROWS = 5

def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation

    # segment columns
    segment_cols = None
    if api.config.segment_cols and not api.config.segment_cols == 'None':
        segment_cols = [x.strip().strip("'").strip('"') for x in api.config.segment_cols.split(',')]
        att_dict['config']['segment_cols'] = str(segment_cols)

    # regression columns
    regression_cols = None
    if api.config.regression_cols and not api.config.regression_cols == 'None':
        regression_cols = [x.strip().strip("'").strip('"') for x in api.config.regression_cols.split(',')]
        att_dict['config']['regression_cols'] = str(regression_cols)
    else:
        raise ValueError('No Regression Columns - mandatory data')

    # prediction column
    prediction_col = None
    if api.config.prediction_col and not api.config.prediction_col == 'None':
        prediction_col = api.config.prediction_col.strip("'").strip('"')
    else:
        raise ValueError('No Predicition Column - mandatory data')


    training_cols = regression_cols + [prediction_col]
    model = LinearRegression(fit_intercept=True)
    def fit(x) :
        model.fit(x[regression_cols], x[prediction_col])
        return pd.Series([model.coef_, model.intercept_],index=['coef','intercept'])
    coef_df = df.groupby(segment_cols)[training_cols].apply(fit).reset_index()

    coef_att = {'segmentation_columns':segment_cols,'regression_columns':regression_cols, 'prediction_column': prediction_col}

    coef_msg = api.Message(attributes=coef_att,body=coef_df)

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

    return  api.Message(attributes = att_dict,body=df), coef_msg


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
            api.config.segment_cols = 'City, Zipcode'
            api.config.regression_cols = 'col1,col2,col3,col4'
            api.config.prediction_col = 'col5'

        class config:
            segment_cols = 'City, Zipcode'
            regression_cols = 'col1,col2,col3,col4'
            prediction_col = 'col5'

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port, msg,coef_msg):
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
            msg, coef_msg = process(msg)
            return msg, coef_msg, json.dumps(msg.attributes, indent=4)


def interface(msg):
    msg, coef_df = process(msg)
    api.send("outDataMsg", msg)
    api.send('outCoefMsg',coef_df)
    info_str = json.dumps(msg.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
api.set_port_callback("inDataFrameMsg", interface)

