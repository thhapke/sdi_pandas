
import os
import numpy as np
import pandas as pd
from lightgbm import LGBMRegressor

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp



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
            label = 'None'
            config_params['label'] = {'title': 'Label', 'description': 'Label of DataFrame that needs to be predict', 'type': 'string'}

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
            attributes = {'format': 'csv', 'name': 'DF_name'}
            default_msg = api.Message(attributes=attributes,body = df)
            callback(default_msg)

        def call(config,msg):
            api.config = config
            return process(msg)


def process(msg):
    att_dict = dict()
    att_dict['config'] = dict()

    att_dict['operator'] = 'anonymizeData'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

    time_monitor = tp.progress()

    result = ''
    logger.debug('Start Process Function')
    logger.debug('Start time: ' + time_monitor.get_start_time())
    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

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

    att_dict['config']['train columns'] = api.config.train_cols
    train_cols = tfp.read_list(api.config.train_cols, df.columns)

    att_dict['config']['label'] = api.config.label
    label = tfp.read_value(api.config.label)
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
    logger.debug('End of Process Function')
    logger.debug('End time: ' + time_monitor.elapsed_time())
    return log_stream.getvalue(), api.Message(attributes=att_dict,body=model)

inports = [{"name":"data","type":"message","description":"Input data"}]
outports = [{"name":"data","type":"message","description":"Output data"},{"name":"log","type":"string","description":"Logging"}]

def call_on_input(msg) :
    new_msg, log = process(msg)
    api.send(outports[0]['name'],new_msg)
    api.send(outports[1]['name'],log)

#api.set_port_callback('input', call_on_input)

def main() :
    print('Test: Default')
    #api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: config')
    config = api.config
    config.var1 = 'own foo'
    config.var12 = 'own bar'
    test_msg = api.Message(attributes={'name':'test1'},body =4)
    new_msg, log = api.call(config,test_msg)
    print('Attributes: ', new_msg.attributes)
    print('Body: ', str(new_msg.body))
    print('Logging: ')
    print(log)
    gs.gensolution(os.path.realpath(__file__), config, inports, outports,override_readme=True)



if __name__ == '__main__':
    main()
