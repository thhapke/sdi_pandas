import os
import numpy as np
import pandas as pd

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

EXAMPLE_ROWS = 5

try:
    api
except NameError:
    class api:
        class config:

            ## Meta data
            tags = {'python36': '','sdi_utils':''}  # tags that helps to select the appropriate container
            operator_description = 'Split Sample'
            operator_description_long='Splits a sample by factor. If the \'lable\' is defined the split is according to \
the frequency of the label to ensure that even for labels with far less frequency that the split factor still taken into \
account properly. '
            version = "0.0.1"  # for creating the manifest.json
            add_readme = dict()
            add_readme["References"] ="[Download template](https://raw.githubusercontent.com/thhapke/gensolution/master/diutil/customOperatorTemplate.py)"

            config_params = dict()
            ## config paramter
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            label = 'None'
            config_params['label'] = {'title': 'Label', 'description': 'Label to split', 'type': 'string'}
            split = 0.8
            config_params['split'] = {'title': 'Split Factor', 'description': 'Split Factor', 'type': 'float'}
            seed = 1
            config_params['seed'] = {'title': 'Seed', 'description': 'Seed for random number generator', 'type': 'int'}
            to_category = False
            config_params['to_category'] = {'title': 'To Categorgy', 'description': 'Cast <object> data type to categorical.', 'type': 'Boolean'}

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
                {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, None, 4, 5], 'col3': [2, 3, 4, 5, 6],
                 'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 6.7, 8.2, None, 10.1]})
            attributes = {'format': 'csv', 'name': 'DF_name'}
            callback(api.Message(attributes=attributes,body=df))

        def call(config,msg):
            api.config = config
            return process(msg)


def process(msg):
    att_dict = dict()
    att_dict['config'] = dict()

    att_dict['operator'] = 'splitSample'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

    time_monitor = tp.progress()

    logger.debug('Start Process Function')
    logger.debug('Start time: ' + time_monitor.get_start_time())
    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Message body does not contain a pandas DataFrame')

    ###### start of doing calculation
    att_dict['config']['split'] = api.config.split
    if api.config.split > df.shape[0]:
        warning = 'Split larger than whole sample'
        split = 1
    elif api.config.split > 1:
        split = api.config.split / df.shape[0]
    else:
        split = api.config.split

    att_dict['config']['to_category'] = api.config.to_category
    if api.config.to_category:
        for col in df.select_dtypes(include=np.object).columns:
            unique_num = len(df[col].unique())
            nan_num = df[col].isna().count()
            logger.debug(
                'Cast to category - {}: unique {}, nan: {} of {}'.format(col, unique_num, nan_num, df.shape[0]))
            df[col] = df[col].astype('category')

    att_dict['config']['label'] = api.config.label
    label = tfp.read_value(api.config.label)
    if label:
        label_vals = list(df[label].unique())
        tdf = list()
        for lab in label_vals:
            tdf.append(df.loc[df[label] == lab].sample(frac=split, random_state=api.config.seed))
        train_df = pd.concat(tdf)
    else:
        train_df = df.sample(frac=split, random_state=api.config.seed)  # random state is a seed value

    test_df = df.drop(train_df.index)
    ###### end of doing calculation

    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty:
        raise ValueError('DataFrame is empty')

    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['shape'] = df.shape
    att_dict['id'] = str(id(df))

    logger.debug('Columns: {}'.format(str(df.columns)))
    logger.debug('Shape (#rows - #columns): {} - {}'.format(df.shape[0], df.shape[1]))
    logger.debug('Memory: {} kB'.format(att_dict['memory']))
    example_rows = EXAMPLE_ROWS if df.shape[0] > EXAMPLE_ROWS else df.shape[0]
    for i in range(0, example_rows):
        att_dict['row_' + str(i)] = str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])
        logger.debug('Head data: {}'.format(att_dict['row_' + str(i)]))

    train_msg =  api.Message(attributes=att_dict, body=train_df)
    test_msg = api.Message(attributes=att_dict, body=test_df)
    logger.debug('End time: ' + time_monitor.elapsed_time())

    return log_stream.getvalue(), train_msg, test_msg

inports = [{"name":"data","type":"message.DataFrame","description":"Input data"}]
outports = [{"name":"log","type":"string","description":"Logging"},\
            {"name":"train","type":"message.DataFrame","description":"train sample"},\
            {"name":"test","type":"message.DataFrame","description":"test sample"}]

def call_on_input(msg) :
    log, train_msg, test_msg = process(msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], train_msg)
    api.send(outports[2]['name'], test_msg)

#api.set_port_callback(inports[0]['name'], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: config')
    config = api.config
    config.label = 'icol'
    config.split = 0.2
    config.to_category = True
    df = pd.DataFrame(
        {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, None, 4, 5], 'col3': [2, 3, 4, 5, 6],
         'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 6.7, 8.2, None, 10.1],'col6':['A','A','B','B','C']})
    attributes = {'format': 'csv', 'name': 'DF_name'}
    input_msg = api.Message(attributes=attributes,body = df)
    log, train_msg, test_msg = api.call(config,input_msg)
    print('Input')
    print(input_msg.body)
    print('Train')
    print(train_msg.body)
    print('Test')
    print(test_msg.body)
    print('Logging: ')
    print(log)
    gs.gensolution(os.path.realpath(__file__), config, inports, outports,override_readme=True)



if __name__ == '__main__':
    main()
