
import os
import pandas as pd

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.tprogress as tp
import sdi_utils.textfield_parser as tfp

try:
    api
except NameError:
    class api:
        class config:

            ## Meta data
            tags = {'python36': '','sdi_utils':''}  # tags that helps to select the appropriate container
            operator_description = 'Drop Duplicates'
            operator_description_long='Operator that removes duplicate rows in a DataFrame. '
            version = "0.0.1"  # for creating the manifest.json
            add_readme = dict()
            add_readme["References"] ="[pandas.DataFrame.drop_duplicates](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html)"

            config_params = dict()
            ## config paramter
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            columns = 'All'
            config_params['columns'] = {'title': 'Columns', 'description': 'Columns to check for duplicates', 'type': 'string'}
            keep = 'first'
            config_params['keep'] = {'title': 'Keep rule', 'description': 'Rule which values to keep', 'type': 'string'}
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

        def set_port_callback(port, callback) :
            df = pd.DataFrame(
                {'icol': [1, 1, 1, 1, 2], 'xcol2': ['A', 'A', 'B', 'B', 'C'], 'xcol3': ['A', 'A', 'C', 'D', 'E'],
                 'xcol4': ['a', 'A', 'b', 'a', 'c'],'xcol5': ['X', 'A', 'B', 'B', 'C']})
            default_msg = api.Message(attributes={'format': 'csv', 'name': 'DF_name'}, body=df)
            callback(default_msg)

        def call(config,msg):
            api.config = config
            return process(msg)


def process(msg):
    att_dict = dict()
    att_dict['config'] = dict()

    att_dict['operator'] = 'drop_duplicates'
    if api.config.debug_mode == True:
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='DEBUG')
    else :
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='INFO')
    logger.info("Process started")
    time_monitor = tp.progress()

    df = msg.body
    prev_shape = df.shape

    drop_cols_test = tfp.read_list(api.config.columns,df.columns)
    keep = tfp.read_value(api.config.keep,test_number=False)
    df.drop_duplicates(subset=drop_cols_test, keep=keep, inplace=True)

    logger.info('End of Process: {}'.format(time_monitor.elapsed_time()))

    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['shape'] = df.shape
    att_dict['id'] = str(id(df))

    logger.debug('Columns: {}'.format(str(df.columns)))
    logger.debug('Shape (#rows - #columns): {} - {}'.format(df.shape[0],df.shape[1]))
    logger.debug('Memory: {} kB'.format(att_dict['memory']))

    logger.debug('Dropped duplicates: {}'.format(prev_shape[0] - df.shape[0]))
    logger.info('Dropped duplicates: {}'.format(prev_shape[0]-df.shape[0]))

    return log_stream.getvalue(), api.Message(attributes={'name':'drop_duplicates','type':'DataFrame'},body=df),

inports = [{"name":"data","type":"message.DataFrame","description":"Input data"}]
outports = [{"name":"log","type":"string","description":"Logging"},\
            {"name":"data","type":"message.DataFrame","description":"Output data"}]

def call_on_input(msg) :
    log, new_msg = process(msg)
    api.send(outports[0]['name'],log)
    api.send(outports[1]['name'],new_msg)

api.set_port_callback(inports[0]['name'], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: config')
    config = api.config
    config.columns = 'Not icol'
    df = pd.DataFrame(
        {'icol': [1, 1, 1, 1, 2], 'xcol2': ['A', 'A', 'B', 'B', 'C'], 'xcol3': ['A', 'A', 'C', 'D', 'E'],
         'xcol4': ['A', 'A', 'b', 'a', 'c'], 'xcol5': ['A', 'A', 'B', 'B', 'C']})
    test_msg = api.Message(attributes={'name':'test1'},body =df)
    log, new_msg = api.call(config,test_msg)
    print('Attributes: ', new_msg.attributes)
    print('Body: ', str(new_msg.body))
    print('Logging: ')
    print(log)
    gs.gensolution(os.path.realpath(__file__), config, inports, outports,override_readme=True)



