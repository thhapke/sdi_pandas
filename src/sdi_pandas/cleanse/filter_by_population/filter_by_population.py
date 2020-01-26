
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
            tags = {'python36': '','sdi_utils':''}  # tags that helps to select the appropriate container
            operator_description = 'Filter by Population'
            operator_description_long='Filter out columns with less population than a threshold.'
            version = "0.0.1"  # for creating the manifest.json
            add_readme = dict()
            add_readme["References"] =""

            config_params = dict()
            ## config paramter
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
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

    att_dict['operator'] = 'filter_by_population'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')


    time_monitor = tp.progress()

    logger.debug('Start Process Function')
    logger.debug('Start time: ' + time_monitor.get_start_time())

    df = msg.body

    columns = tfp.read_list(api.config.columns,df.columns,test_number=False)
    info_only = api.config.info_only
    threshold = api.config.threshold

    transform_data = {'column': [], 'dtype': [], 'unique_vals': [],'action': []}
    for col in columns:
        population = df[col].count() / df.shape[0]
        unique_vals = df[col].unique()
        if population < threshold and not (len(unique_vals) == 1 and np.isnan(unique_vals[0])):
            unique_vals = df[col].unique()
            transform_data['column'].append(col)
            transform_data['dtype'].append(df[col].dtype)
            transform_data['unique_vals'].append(unique_vals)
            transform_data['action'].append('drop')
            if not info_only:
                df.drop(columns=[col], inplace=True)

    logger.debug('End of Process Function')
    logger.debug('End time: ' + time_monitor.elapsed_time())

    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['shape'] = df.shape
    att_dict['id'] = str(id(df))

    logger.debug('Columns: {}'.format(str(df.columns)))
    logger.debug('Shape (#rows - #columns): {} - {}'.format(df.shape[0], df.shape[1]))
    logger.debug('Memory: {} kB'.format(att_dict['memory']))

    return log_stream.getvalue(), api.Message(attributes={'name':'filter_by_population','type':'DataFrame'},body=df),\
            api.Message(attributes={'name':'transformation','type':'DataFrame'},body=pd.DataFrame(transform_data))

inports = [{"name":"data","type":"message.DataFrame","description":"Input data"}]
outports = [{"name":"log","type":"string","description":"Logging"},\
            {"name":"data","type":"message.DataFrame","description":"Output data"},\
            {"name":"transformation","type":"message.DataFrame","description":"Transformation data"}]

def call_on_input(msg) :
    log, data, transformation_data = process(msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], data)
    api.send(outports[2]['name'], transformation_data)

#api.set_port_callback(inports[0]['name'], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: config')
    config = api.config
    config.columns = 'All'
    config.info_only = False
    config.threshold = 0.25
    df = pd.DataFrame(
        {'icol': [1, 1, 1, 1, 1], 'xcol2': ['A', None,None,None, None], 'xcol3': ['A', 'A', 'C', 'D', 'E'],
         'xcol4': ['A', 'A', 'b', 'a', 'c'], 'xcol5': [None, 'B',None,None, None]})
    test_msg = api.Message(attributes={'name':'test1'},body =df)
    log, data, trans = api.call(config,test_msg)
    print('Attributes: ', data.attributes)
    print('Body: ', str(data.body))
    print('Attributes: ', trans.attributes)
    print('Body: ', str(trans.body))
    print('Logging: ')
    print(log)
    gs.gensolution(os.path.realpath(__file__), config, inports, outports,override_readme=True)



if __name__ == '__main__':
    main()
