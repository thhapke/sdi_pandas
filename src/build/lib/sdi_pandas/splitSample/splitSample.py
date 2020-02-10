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
            label_col = 'None'
            config_params['label_col'] = {'title': 'Column of Labels', 'description': 'Label to split', 'type': 'string'}
            split = 0.8
            config_params['split'] = {'title': 'Split Factor', 'description': 'Split Factor', 'type': 'number'}
            seed = 1
            config_params['seed'] = {'title': 'Seed', 'description': 'Seed for random number generator', 'type': 'integer'}
            to_category = False
            config_params['to_category'] = {'title': 'To Categorgy', 'description': 'Cast <object> data type to categorical.', 'type': 'boolean'}

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
            attributes = {'format': 'csv', 'name': 'DF_name','process_list':[]}
            callback(api.Message(attributes=attributes,body=df))

        def call(config,msg):
            api.config = config
            return process(msg)


def process(msg):
    att_dict = msg.attributes
    att_dict['operator'] = 'splitSample'
    if api.config.debug_mode == True:
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='DEBUG')
    else :
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='INFO')
    logger.info("Process started")
    time_monitor = tp.progress()

    time_monitor = tp.progress()

    df = msg.body
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Message body does not contain a pandas DataFrame')

    ###### start of doing calculation
    if api.config.split > df.shape[0]:
        warning = 'Split larger than whole sample'
        split = 1
    elif api.config.split > 1:
        split = api.config.split / df.shape[0]
    else:
        split = api.config.split
    logger.info('Split DataFrame: {}'.format(split))

    if api.config.to_category:
        cast_cols = df.select_dtypes(include=np.object).columns
        for col in cast_cols:
            unique_num = len(df[col].unique())
            nan_num = df[col].isna().count()
            logger.debug(
                'Cast to category - {}: unique {}, nan: {} of {}'.format(col, unique_num, nan_num, df.shape[0]))
            df[col] = df[col].astype('category')
        logger.info('Cast to category type: {}'.format(cast_cols))

    label = tfp.read_value(api.config.label_col)
    if label:
        label_vals = list(df[label].unique())
        tdf = list()
        for lab in label_vals:
            tdf.append(df.loc[df[label] == lab].sample(frac=split, random_state=api.config.seed))
        train_df = pd.concat(tdf)
        logger.info('Consider label ratio for splitting: {}'.format(label))
    else:
        train_df = df.sample(frac=split, random_state=api.config.seed)  # random state is a seed value

    test_df = df.drop(train_df.index)
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
    att_dict['process_list'].append(att_dict['operator'])
    logger.debug('Past process steps: {}'.format(att_dict['process_list']))
    logger.debug('Process ended: {}  - {}  '.format(progress_str,time_monitor.elapsed_time()))

    return log_stream.getvalue(), api.Message(attributes=att_dict,body=train_df), api.Message(attributes=att_dict,body=test_df)

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
    config.label_col = 'icol'
    config.split = 0.2
    config.to_category = True
    df = pd.DataFrame(
        {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, None, 4, 5], 'col3': [2, 3, 4, 5, 6],
         'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 6.7, 8.2, None, 10.1],'col6':['A','A','B','B','C']})
    attributes = {'format': 'csv', 'name': 'DF_name','process_list':[]}
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
