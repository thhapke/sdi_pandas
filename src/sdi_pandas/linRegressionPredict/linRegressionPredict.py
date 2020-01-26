import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp

import pandas as pd
import numpy as np


try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            if isinstance(msg,api.Message) :
                print('Port: ', port)
                print('Attributes: ', msg.attributes)
                print('Body: ', str(msg.body))
            else :
                print(str(msg))
            return msg
    
        def call(config,msg_coef,msg_data):
            api.config = config
            return process(msg_coef,msg_data)
            
        def set_port_callback(port, callback) :
            data_df = pd.DataFrame(
                {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, 3, 4, 5], 'col3': [2, 3, 4, 5, 6], 'col4': [5, 6.5, 7.5, 8, 9], \
                 'col5': [6, 6.7, 8.2, 9, 10.1]})
            msg_data = api.Message(attributes= {'format': 'panda', 'name': 'DF_name'},body = data_df)
            # without segmentation
            coef_df = pd.Series({'coef': [0.57, 0.57, -0.09], 'intercept': 4.67})
            att_coef = {'format': 'panda', 'name': 'Coeff', 'regression_columns': ['col2', 'col3', 'col4'], \
                        'prediction_column': 'col5'}
            msg_coef = api.Message(attributes=att_coef,body=coef_df)
            # with segmentation
            coef_df = pd.DataFrame(
                {'icol': [1, 3], 'coef': [[0.165, 0.165, 0.25], [0.25, 0.25, 0.6]], 'intercept': [4.27, 1.95]})
            att_coef = {'format': 'panda', 'name': 'Coeff', 'regression_columns': ['col2', 'col3', 'col4'], \
                        'prediction_column': 'col5', 'segmentation_columns': ['icol']}
            msg_coef = api.Message(attributes=att_coef, body=coef_df)
            callback(msg_coef,msg_data)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': '', 'sklearn': ''}
            operator_description = "Linear Regression Predict"
            operator_description_long = "Using the model calculated with the Scikit Learn module to predict values."
            add_readme = dict()
            add_readme["References"] = '[ScitLearn Linear Regression](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html)'
            prediction_col_only = False
            config_params['prediction_col_only'] = {'title': 'Prediction Columns only', 'description': 'The output only contains the prediction columns', 'type': 'boolean'}
            regresssion_cols_value = 'None'
            config_params['regresssion_cols_value'] = {'title': 'Set value for some regression columns', 'type': 'string'}
            config_params['regresssion_cols_value']['description'] = "list of comma-separated maps with columns and values that overrides the prediction data of the *inData* message. Only applicable for fixe values. Otherwise the *inData* message needs to be used. "
            prediction_prefix = 'p_'
            config_params['prediction_prefix'] = {'title': 'Prefix for prediction columns', 'description': 'Prefix for prediction columns', 'type': 'string'}



def process(msg_coef, msg_data) :

    logger, log_stream = slog.set_logging('DEBUG')

    # start custom process definition
    prev_att = msg_data.attributes
    df = msg_data.body
    coef_df = msg_coef.body
    if not isinstance(df, pd.DataFrame):
        logger.error('Message body does not contain a pandas DataFrame')
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation
    # segment columns
    segment_cols = None
    if 'segmentation_columns' in msg_coef.attributes:
        segment_cols = msg_coef.attributes['segmentation_columns']

    # regression columns
    regression_cols = msg_coef.attributes['regression_columns']

    # prediction column
    prediction_col = msg_coef.attributes['prediction_column']

    # setting values of regression column values (if not in the dataMsg already done
    att_dict['config']['regresssion_cols_value'] = api.config.regresssion_cols_value
    valmap = tfp.read_dict(api.config.regresssion_cols_value)
    if valmap:
        for col, val in valmap.items():
            if np.issubdtype(df[col].dtype, np.integer):
                val = int(val)
            elif np.issubdtype(df[col].dtype, np.float):
                val = float(val)
            else:
                raise ValueError('Regression value needs to be numeric')
            df[col] = val

    # merge data and coef df
    if segment_cols:
        df = pd.merge(df, coef_df, how='inner', left_on=segment_cols, right_on=segment_cols)

    prefix = tfp.read_value(api.config.prediction_prefix)
    if prefix == None:
        prefix = ''
    pcol = prefix + prediction_col

    if segment_cols:
        def predict(x):
            x[pcol] = np.dot(x['coef'], x[regression_cols].values) + x['intercept']
            return x

        df = df.apply(predict, axis=1, result_type=None)
        df.drop(columns=['coef', 'intercept'], inplace=True)
    else:
        def predict(x):
            x[pcol] = np.dot(coef_df['coef'], x[regression_cols].values) + coef_df['intercept']
            return x

        df = df.apply(predict, axis=1, result_type=None)

    # cast type of prediction col from prediction variable
    if df[prediction_col].dtype == np.integer:
        logger.debug('Cast prediction column to <int>')
        df[pcol] = df[pcol].round().astype(df[prediction_col].dtype)

    if api.config.prediction_col_only:
        logger.debug('Output only prediction columns')
        if segment_cols:
            df[prediction_col] = df[pcol]
            df = df[segment_cols + [prediction_col]]
        else:
            df = df[prediction_col]
    att_dict['config']['prediction_col_only'] = api.config.prediction_col_only

    ###### end of doing calculation

    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty:
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'regressionTrainingDataFrame'
    att_dict['name'] = prev_att['name']

    # end custom process definition

    log = log_stream.getvalue()
    msg = api.Message(attributes=att_dict,body=df)
    return log, msg


inports = [{'name': 'inData', 'type': 'message.DataFrame'}, {'name': 'inCoef', 'type': 'message.DataFrame'}]
outports = [{'name': 'log', 'type': 'string'}, {'name': 'output', 'type': 'message.DataFrame'}]

def call_on_input(msg, msg1) :
    log, msg = process(msg, msg1)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], msg)

#api.set_port_callback([inports[0]['name'], inports[1]['name']], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback([inports[0]['name'], inports[1]['name']], call_on_input)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
