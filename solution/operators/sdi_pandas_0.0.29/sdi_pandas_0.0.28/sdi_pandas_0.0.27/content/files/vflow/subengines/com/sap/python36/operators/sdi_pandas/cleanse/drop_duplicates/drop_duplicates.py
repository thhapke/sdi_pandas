
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
            tags = {'python36': ''}  # tags that helps to select the appropriate container
            operator_description = 'Drop Duplicates'
            operator_description_long='Operator that removes duplicate rows in a DataFrame. '
            version = "0.0.1"  # for creating the manifest.json
            add_readme = dict()
            add_readme["References"] ="[pandas.DataFrame.drop_duplicates](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html)"

            config_params = dict()
            ## config paramter
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
    logger, log_stream = slog.set_logging('DEBUG')
    time_monitor = tp.progress()

    logger.debug('Start Process Function')
    logger.debug('Start time: ' + time_monitor.get_start_time())

    df = msg.body
    before_num_rows = df.shape[0]
    drop_cols_test = tfp.read_list(api.config.columns,df.columns)
    keep = tfp.read_value(api.config.keep,test_number=False)
    df.drop_duplicates(subset=drop_cols_test, keep=keep, inplace=True)
    logger.debug('Duplicate Rows: {}'.format(before_num_rows - df.shape[0]))

    logger.debug('End of Process Function')
    logger.debug('End time: ' + time_monitor.elapsed_time())
    return log_stream.getvalue(), api.Message(attributes={'name':'drop_duplicates','type':'DataFrame'},body=df),

inports = [{"name":"input","type":"message","description":"Input data"}]
outports = [{"name":"output","type":"message","description":"Output data"},{"name":"log","type":"string","description":"Logging"}]

def call_on_input(msg) :
    log, new_msg = process(msg)
    api.send(outports[0]['name'],new_msg)
    api.send(outports[1]['name'],log)

api.set_port_callback('input', call_on_input)

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



