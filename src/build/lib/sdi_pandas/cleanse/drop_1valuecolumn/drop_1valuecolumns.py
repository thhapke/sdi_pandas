
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
            operator_description = 'Drop Single Value Columns'
            operator_description_long='Drops columns of DataFrame with only one unique value.'
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

    att_dict['operator'] = 'drop_1valuecolumns'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

    time_monitor = tp.progress()

    logger.debug('Start Process Function')
    logger.debug('Start time: ' + time_monitor.get_start_time())

    df = msg.body
    # Columns with 1 unique value
    columns = tfp.read_list(api.config.columns,df.columns)
    col1val_data = {'column': [], 'type': [], 'unique_vals': [], 'action': []}
    for col in columns:
        vals = df[col].unique()
        if len(vals) == 1:
            col1val_data['column'].append(col)
            col1val_data['type'].append(str(df[col].dtype))
            col1val_data['unique_vals'].append(vals)
            col1val_data['action'].append('drop')
            if not api.config.info_only:
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

    return log_stream.getvalue(), api.Message(attributes={'name':'drop_duplicates','type':'DataFrame'},body=df),\
            api.Message(attributes={'name':'transformation','type':'DataFrame'},body=pd.DataFrame(col1val_data))

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
    config.info_only = True
    df = pd.DataFrame(
        {'icol': [1, 1, 1, 1, 1], 'xcol2': ['A', 'A', 'B', 'B', 'C'], 'xcol3': ['A', 'A', 'C', 'D', 'E'],
         'xcol4': ['A', 'A', 'b', 'a', 'c'], 'xcol5': ['A', 'A', 'A', 'A', 'A']})
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
