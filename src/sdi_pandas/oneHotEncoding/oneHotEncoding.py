import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog

import sdi_utils.textfield_parser as tfp
import pandas as pd

EXAMPLE_ROWS = 5

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
                {'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100, 200, 300, 400, 500]})

            attributes = {'format': 'csv', 'name': 'DF_name'}

            default_msg = api.Message(attributes={'name':'doit'},body = 'message')
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': ''}
            operator_description = "One Hot Encoding"
            operator_description_long = "Transforms string(object) columns to categoricals by using 'pandas.get_dummies()"
            add_readme = dict()
            add_readme["References"] = r"""[pandas doc: get_dummies](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.get_dummies.html)"""
            training_cols = 'None'
            config_params['training_cols'] = {'title': 'Training Columns', 'description': 'Training Columns', 'type': 'string'}


def process(msg) :

    logger, log_stream = slog.set_logging('DEBUG')

    # start custom process definition
    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df,pd.DataFrame) :
        logger.error('Message body does not contain a pandas DataFrame')
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    df = pd.get_dummies(df,prefix_sep='_',drop_first=True)

    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        logger.error('DataFrame is empty')
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

    # end custom process definition

    log = log_stream.getvalue()

    msg = api.Message(attributes=att_dict,body=df)
    return log, msg


inports = [{'name': 'input', 'type': 'message.DataFrame'}]
outports = [{'name': 'log', 'type': 'string'}, {'name': 'output', 'type': 'message.DataFrame'}]

def call_on_input(msg) :
    log, msg = process(msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], msg)

#api.set_port_callback([inports[0]['name']], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback([inports[0]['name']], call_on_input)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
