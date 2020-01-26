import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp

from sklearn.linear_model import LinearRegression
import pandas as pd

EXAMPLE_ROWS =  5

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
    
        def call(config,msg):
            api.config = config
            return process(msg)
            
        def set_port_callback(port, callback) :
            df = pd.DataFrame(
                {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, 3, 4, 5], 'col3': [2, 3, 4, 5, 6],
                 'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 6.7, 8.2, 9, 10.1]})
            default_msg = api.Message(attributes={'format': 'pandas', 'name': 'DF_name'},body = df)
            api.config.regression_cols = "col2,col3,col4"
            api.config.prediction_col = "col5"
            return callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'': '', 'pandas': ''}
            operator_description = "Train Linear Regression"
            operator_description_long = "Using Scikit Learn module to train a linear regression model."
            add_readme = dict()
            add_readme["References"] = r"""[ScitLearn Linear Regression](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html)"""
            prediction_col = 'None'
            config_params['prediction_col'] = {'title': 'Prediction Column', 'description': 'Prediction column', 'type': 'string'}
            regression_cols = 'None'
            config_params['regression_cols'] = {'title': 'Regression Columns', 'description': 'Regression columns', 'type': 'string'}
            segment_cols = 'None'
            config_params['segment_cols'] = {'title': 'Segment Columns', 'description': 'Segment Columns', 'type': 'string'}


def process(msg) :

    logger, log_stream = slog.set_logging('DEBUG')

    # start custom process definition
    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df, pd.DataFrame):
        logger.error('Message body does not contain a pandas DataFrame')
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation

    # segment columns
    att_dict['config']['segment_cols'] = api.config.segment_cols
    segment_cols = tfp.read_list(api.config.segment_cols)

    # regression columns
    att_dict['config']['regression_cols'] = api.config.regression_cols
    regression_cols = tfp.read_list(api.config.regression_cols)
    if not regression_cols:
        logger.error('No Regression Columns - mandatory data')
        raise ValueError('No Regression Columns - mandatory data')

    # prediction column
    att_dict['config']['prediction_col'] = api.config.prediction_col
    prediction_col = tfp.read_value(api.config.prediction_col)
    if not prediction_col:
        raise ValueError('No Predicition Column - mandatory data')

    training_cols = regression_cols + [prediction_col]
    model = LinearRegression(fit_intercept=True)

    def fit(x):
        model.fit(x[regression_cols], x[prediction_col])
        return pd.Series([model.coef_, model.intercept_], index=['coef', 'intercept'])

    if segment_cols:
        coef_df = df.groupby(segment_cols)[training_cols].apply(fit).reset_index()
    else:
        model.fit(df[regression_cols], df[prediction_col])
        coef_df = pd.Series([model.coef_, model.intercept_], index=['coef', 'intercept'])

    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty:
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'regressionTrainingDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0, example_rows):
        att_dict['row_' + str(i)] = str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    # end custom process definition

    log = log_stream.getvalue()
    coef_att = {'segmentation_columns': segment_cols, 'regression_columns': regression_cols,
                'prediction_column': prediction_col}

    msg_coef = api.Message(attributes=coef_att, body=coef_df)
    msg_data = api.Message(attributes=att_dict,body=df)

    return log, msg_coef, msg_data


inports = [{'name': 'input', 'type': 'message.DataFrame'}]
outports = [{'name': 'log', 'type': 'string'}, {'name': 'outData', 'type': 'message.DataFrame'}, {'name': 'outCoef', 'type': 'message.DataFrame'}]

def call_on_input(msg) :
    log, msg_coef, msg_data = process(msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], msg_coef)
    api.send(outports[2]['name'], msg_data)

#api.set_port_callback([inports[0]['name']], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback([inports[0]['name']], call_on_input)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
