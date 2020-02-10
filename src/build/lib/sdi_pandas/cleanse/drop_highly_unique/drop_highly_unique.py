
import os
import pandas as pd
import numpy as np

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.tprogress as tp
import sdi_utils.textfield_parser as tfp

EXAMPLE_ROWS = 5

try:
    api
except NameError:
    class api:
        class config:

            ## Meta data
            tags = {'python36': '','sdi_utils':''}  # tags that helps to select the appropriate container
            operator_description = 'Drop Highly Unique Columns'
            operator_description_long='Drop columns with number of unique values (string) close to number of rows.\nWARNING: \
exclude dtype=DateTime columns.'
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
            threshold = 0.99
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
            default_msg = api.Message(attributes={'format': 'csv', 'name': 'DF_name','process_list':[]}, body=df)
            callback(default_msg)

        def call(config,msg):
            api.config = config
            return process(msg)


def process(msg):
    att_dict = msg.attributes
    att_dict['operator'] = 'drop_highly_unique'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

    time_monitor = tp.progress()
    logger.debug('Start time: ' + time_monitor.get_start_time())

    df = msg.body

    columns = tfp.read_list(api.config.columns,df.columns,test_number=False)
    info_only = api.config.info_only
    threshold = api.config.threshold

    transform_data = {'column': [], 'dtype': [], 'unique_values': [],'action': []}
    for col in df[columns].select_dtypes(np.object).columns:
        unique_vals_num = len(df[col].unique())
        frac_unique_vals = unique_vals_num / df.shape[0]
        if frac_unique_vals > threshold:
            transform_data['column'].append(col)
            transform_data['dtype'].append(df[col].dtype)
            transform_data['unique_values'].append(frac_unique_vals)
            transform_data['action'].append('drop')
            if info_only == False:
                df.drop(columns=[col], inplace=True)

    # end custom process definition
    if df.empty :
        raise ValueError('DataFrame is empty')
    logger.debug('Columns: {}'.format(str(df.columns)))
    logger.debug('Shape (#rows - #columns): {} - {}'.format(df.shape[0],df.shape[1]))
    logger.debug('Memory: {} kB'.format(df.memory_usage(deep=True).sum() / 1024 ** 2))
    example_rows = EXAMPLE_ROWS if df.shape[0] > EXAMPLE_ROWS else df.shape[0]
    for i in range(0, example_rows):
        logger.debug('Row {}: {}'.format(i,str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])))

    progress_str = '>BATCH ENDED<'
    if 'storage.fileIndex' in att_dict and 'storage.fileCount' in att_dict and 'storage.endOfSequence' in att_dict :
        if not att_dict['storage.fileIndex'] + 1 == att_dict['storage.fileCount'] :
            progress_str = '{}/{}'.format(att_dict['storage.fileIndex'],att_dict['storage.fileCount'])
    logger.debug('Past process steps: {}'.format(att_dict['process_list']))
    logger.debug('Process ended: {}  - {}  '.format(progress_str,time_monitor.elapsed_time()))

    return log_stream.getvalue(), api.Message(attributes=att_dict,body=df),\
            api.Message(attributes={'name':'transformation','type':'DataFrame'},body=pd.DataFrame(transform_data))


inports = [{"name":"data","type":"message.DataFrame","description":"Input data"}]
outports = [{"name":"log","type":"string","description":"Logging"},\
            {"name":"transformation","type":"message.DataFrame","description":"Transformation data"},\
            {"name":"data","type":"message.DataFrame","description":"Output data"}]

def call_on_input(msg) :
    log, data, transformation_data = process(msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], transformation_data)
    api.send(outports[2]['name'], data)

#api.set_port_callback(inports[0]['name'], call_on_input)

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
    test_msg = api.Message(attributes={'name':'test1','process_list':[]},body =df)
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
