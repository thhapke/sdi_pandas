
import os
import pandas as pd
import numpy as np

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
            operator_description = 'Drop Highly Unique Columns'
            operator_description_long='Drop columns with number of unique values (string) close to number of rows.\nWARNING: \
exclude dtype=DateTime columns.'
            version = "0.0.1"  # for creating the manifest.json
            add_readme = dict()
            add_readme["References"] =""

            config_params = dict()
            ## config paramter
            columns = 'All'
            config_params['columns'] = {'title': 'Columns', 'description': 'Columns to check for 1 unique value', 'type': 'string'}
            info_only = 'True'
            config_params['info_only'] = {'title': 'Info only', 'description': 'Only check without data modification.', 'type': 'boolean'}
            threshold = 0.0001
            config_params['threshold'] = {'title': 'Threshold', 'description': 'Threshold by with column is droped.',
                                          'type': 'number'}

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
                {'icol': [1, 2, 3, 4, 5], 'xcol2': ['A', 'A', 'A', 'B', 'B'], 'xcol3': ['A', 'B', 'C', 'D', 'E'],
                 'xcol4': ['A', 'A', 'b', 'a', 'c'], 'xcol5': [None, 'B', 'A', None, None]})
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

    columns = tfp.read_list(api.config.columns,df.columns,test_number=False)
    info_only = api.config.info_only
    threshold = api.config.threshold

    transform_data = {'column': [], 'dtype': [], 'unique_values': [],'action': []}
    for col in df[columns].select_dtypes(np.object):
        unique_vals_num = len(df[col].unique())
        frac_unique_vals = unique_vals_num / df.shape[0]
        if frac_unique_vals > threshold:
            transform_data['column'].append(col)
            transform_data['dtype'].append(df[col].dtype)
            transform_data['unique_values'].append(frac_unique_vals)
            transform_data['action'].append('drop')
            if info_only == False:
                df.drop(columns=[col], inplace=True)

    logger.debug('End of Process Function')
    logger.debug('End time: ' + time_monitor.elapsed_time())
    return log_stream.getvalue(), api.Message(attributes={'name':'filter_by_population','type':'DataFrame'},body=df),\
            api.Message(attributes={'name':'transformation','type':'DataFrame'},body=pd.DataFrame(transform_data))

inports = [{"name":"input","type":"message","description":"Input data"}]
outports = [{"name":"output","type":"message","description":"Output data"},{"name":"log","type":"string","description":"Logging"},
            {"name":"transformation","type":"string","description":"transformation"}]

def call_on_input(msg) :
    log, data, transformation_data = process(msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], data)
    api.send(outports[2]['name'], transformation_data)

api.set_port_callback('input', call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: config')
    config = api.config
    config.columns = 'All'
    config.info_only = False
    config.threshold = 0.9
    df = pd.DataFrame(
        {'icol': [1, 2, 3, 4, 5], 'xcol2': ['A', 'A', 'A', 'B', 'B'], 'xcol3': ['A', 'B', 'C', 'D', 'E'],
         'xcol4': ['A', 'A', 'b', 'a', 'c'], 'xcol5': [None, 'B', 'A', None, None]})
    test_msg = api.Message(attributes={'name':'test1'},body =df)
    log, data, trans = api.call(config,test_msg)
    print('Attributes: ', data.attributes)
    print('Body: ', str(data.body))
    print('Attributes: ', trans.attributes)
    print('Body: ', str(trans.body))
    print('Logging: ')
    print(log)
    gs.gensolution(os.path.realpath(__file__), config, inports, outports,override_readme=True)



