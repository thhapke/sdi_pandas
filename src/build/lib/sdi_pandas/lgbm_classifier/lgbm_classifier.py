
import os
import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor

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
            tags = {'python36': '','sdi_utils':'','lightgbm':''}  # tags that helps to select the appropriate container
            operator_description = 'LGBM Classifier'
            operator_description_long='Classifies the data by using the light gbm classifier. '
            version = "0.0.1"  # for creating the manifest.json
            add_readme = dict()
            add_readme["References"] ="[Download template](https://raw.githubusercontent.com/thhapke/gensolution/master/diutil/customOperatorTemplate.py)"

            config_params = dict()
            ## config paramter
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            train_cols = 'None'
            config_params['train_cols'] = {'title': 'Training Columns', 'description': 'Columns of DataFrame that are used for training', 'type': 'string'}
            label_col = 'None'
            config_params['label_col'] = {'title': 'Column of Label', 'description': 'Label of DataFrame that needs to be predict', 'type': 'string'}

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
                {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, 3, 4, 5], 'col3': [2, 3, 4, 5, 6],
                 'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 6.7, 8.2, 9, 10.1]})
            attributes = {'format': 'csv', 'name': 'DF_name','process_list':[]}
            default_msg = api.Message(attributes=attributes,body = df)
            callback(default_msg)

        def call(config,msg):
            api.config = config
            return process(msg)


def process(msg):
    att_dict = msg.attributes
    att_dict['operator'] = 'lgbm_classifier'
    if api.config.debug_mode == True:
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='DEBUG')
    else :
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='INFO')
    logger.info("Process started")
    time_monitor = tp.progress()

    df = msg.body
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Message body does not contain a pandas DataFrame')

    ###### start of doing calculation

    model = LGBMRegressor(
        n_estimators=200,
        learning_rate=0.03,
        num_leaves=32,
        colsample_bytree=0.9497036,
        subsample=0.8715623,
        max_depth=8,
        reg_alpha=0.04,
        reg_lambda=0.073,
        min_split_gain=0.0222415,
        min_child_weight=40)

    train_cols = tfp.read_list(api.config.train_cols, df.columns)
    logger.info('Train columns: {}'.format(train_cols))

    label = tfp.read_value(api.config.label_col)
    logger.info('Label column: {}'.format(label))
    if not label:
        raise ValueError('Label is mandatory')

    # cast to categorical dtype
    for c in df[train_cols].select_dtypes(include='category').columns:
        unique_num = len(df[c].unique())
        nan_num = df[c].isna().count()
        logger.debug('Cast to category - {}: unique {}, nan: {} of {}'.format(c, unique_num, nan_num, df.shape[0]))
        df[c] = df[c].cat.codes
        df[c] = df[c].astype('int32')

    if pd.api.types.is_categorical(df[label]):
        df[label] = df[label].astype('category')
        logger.debug('Cast label to <category>')
        df[label] = df[label].cat.codes
        df[label] = df[label].astype('int32')

    print(df.select_dtypes(include='category').head(10))
    logger.debug('Train with {} features'.format(len(train_cols)))
    print(train_cols)
    model.fit(df[train_cols], df[label], eval_metric='auc')

    ###### end of doing calculation

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

    return log_stream.getvalue(), api.Message(attributes=att_dict,body=df)

inports = [{"name":"data","type":"message.DataFrame","description":"Input data"}]
outports = [{"name":"log","type":"string","description":"Logging"},\
            {"name":"data","type":"message.DataFrame","description":"Output model"}]

def call_on_input(msg) :
    log, model_msg = process(msg)
    api.send(outports[0]['name'],log)
    api.send(outports[1]['name'],model_msg)

#api.set_port_callback(inports[0]['name'], call_on_input)

def main() :
    print('Test: Default')
    #api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: config')
    config = api.config
    config.train_cols = 'icol, col2, col3, col4'
    config.label_col = 'col5'
    df = pd.DataFrame(
        {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, 3, 4, 5], 'col3': [2, 3, 4, 5, 6],
         'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 6.7, 8.2, 9, 10.1]})
    attributes = {'format': 'csv', 'name': 'DF_name','process_list':[]}
    train_msg = api.Message(attributes=attributes, body=df)
    log, new_msg = api.call(config,train_msg)
    print('Attributes: ', new_msg.attributes)
    print('Body: ', str(new_msg.body))
    print('Logging: ')
    print(log)
    gs.gensolution(os.path.realpath(__file__), config, inports, outports,override_readme=True)



if __name__ == '__main__':
    main()
