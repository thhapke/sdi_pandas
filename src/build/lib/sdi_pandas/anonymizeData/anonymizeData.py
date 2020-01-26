
import pandas as pd
import numpy as np
import random
import math
import string
import os

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.tprogress as tp
import sdi_utils.textfield_parser as tfp


EXAMPLE_ROWS = 5
keep_terms = {"Yes": "Y", "yes": "Y", "y": "Y", "Y": "Y", "N": "N", "n": "N", "No": "N", "no": "N"}


try:
    api
except NameError:
    class api:
        class config:

            ## Meta data
            tags = {'python36': '','sdi_utils':''}  # tags that helps to select the appropriate container
            operator_description = 'Anonymize Data'
            operator_description_long='Anonymizes the dataset.'
            version = "0.0.1"  # for creating the manifest.json
            add_readme = dict()
            add_readme["References"] ="[Download template](https://raw.githubusercontent.com/thhapke/gensolution/master/diutil/customOperatorTemplate.py)"

            config_params = dict()
            ## config paramter
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            to_nan = 'None'
            config_params['to_nan'] = {'title': 'To NaN', 'description': 'Character to be replaced by NaN', 'type': 'string'}
            anonymize_cols = 'None'
            config_params['anonymize_cols'] = {'title': 'Anonymize Columns', \
                                                     'description': 'Anonymize columns for replacing with random strings or'
                                                                    'linear transformed numbers', \
                                                     'type': 'string'}
            anonymize_to_int_cols = 'None'
            config_params['anonymize_to_int_cols'] = {'title': 'Anonymize to Integer Columns', \
                                                     'description': 'Anonymize columns for replacing with random integers, e.g. IDs', \
                                                     'type': 'string'}

            enumerate_cols = "None"
            config_params['enumerate_cols'] = {'title': 'Enumerate Columns',
                                                  'description': 'Replace column name with enumerated column name',
                                                  'type': 'string'}
            prefix_cols = 'Att'
            config_params['enumerate_cols'] = {'title': 'Prefix of enumerated columns',
                                               'description': 'Prefix of enumerated columns',
                                               'type': 'string'}


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
                {'icol': [1, 2, 3, 4, 5], 'col2': ['Cydia','Cydia', None, 'Dani', 'Liza'],\
                 'col3': ['Frank', 'Stephen', 'Hubert', 'Hubert', 'Sue'],
                 'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 6.7, 8.2, None, 10.1]})
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

    logger.debug("Process started")
    time_monitor = tp.progress()

    result = ''
    logger.debug('Start Process Function')
    logger.debug('Start time: ' + time_monitor.get_start_time())

    prev_att = msg.attributes
    df = msg.body

    ###### start of doing calculation

    att_dict['config']['to_nan'] = api.config.to_nan
    to_nan = tfp.read_value(api.config.to_nan,test_number=False)
    if to_nan:
        df.replace(to_nan, np.nan, inplace=True)


    att_dict['config']['anonymize_to_int_cols'] = api.config.anonymize_to_int_cols
    anonymize_to_int_cols = tfp.read_list(api.config.anonymize_to_int_cols, list(df.columns))
    att_dict['config']['anonymize_cols'] = api.config.anonymize_cols
    anonymize_cols = tfp.read_list(api.config.anonymize_cols, list(df.columns))

    ## Anonymize columns
    if anonymize_cols :
        logger.debug('Anonymize Columns: {}'.format(str(anonymize_cols)))
        # ensure that ids are not anonymized in the section but exclusively in the id-section
        anonymize_cols = [c for c in anonymize_cols if not c in anonymize_to_int_cols]

        # replaceing string with a random string
        for c in df[anonymize_cols].select_dtypes(include='object') :
            unique_list = df[c].unique()
            n = int(math.log10(len(unique_list))) + 2
            # create random map first then check if keys have the values of the keep_list and replace the random values
            rep_map = { x:''.join(random.choices(string.ascii_letters, k = n))  for x in unique_list if isinstance(x,str) }
            for ktk, ktv in keep_terms.items():
                if ktk in rep_map.keys():
                    rep_map[ktk] = ktv
            df[c].replace(rep_map,inplace=True)

        # linear shift of integer
        for c in df[anonymize_cols].select_dtypes(include='int'):
            unique_i = df[c].unique()
            max_i = max(unique_i)
            min_i = min(unique_i)
            length = max_i - min_i
            rand_int1  = random.randint(0,100)
            rand_int2 = random.randint(0,100)
            # preserves existing/binary values 0 and 1
            if not (len(unique_i) == 2 and 0 in unique_i and 1 in unique_i) :
                df[c] = ((df[c] - min_i)/length * rand_int1 +  rand_int2).astype('int')

        # linear shift of float
        for c in df[anonymize_cols].select_dtypes(include='float'):
            unique_f = df[c].unique()
            max_f = max(unique_f)
            min_f = min(unique_f)
            length = max_f - min_f
            rand_float1  = random.random()
            rand_float2 = random.random()
            df[c] = ((df[c] - min_f)/length * rand_float1 + rand_float2)/2.0


    if anonymize_to_int_cols :
        logger.debug('Anonymize to Integer Columns: {}'.format(str(anonymize_to_int_cols)))
        # replaceing string with a random string
        for c in df[anonymize_to_int_cols]:
            unique_list = df[c].unique()
            rand_list = list(np.random.choice(1000*len(unique_list), len(unique_list), replace=False))
            # create random map first then check if keys have the values of the keep_list and replace the random values
            rep_map = dict(zip(unique_list, rand_list))
            df[c].replace(rep_map, inplace=True)

    att_dict['config']['enumerate_cols'] = api.config.enumerate_cols
    att_dict['config']['prefix_cols'] = api.config.prefix_cols
    enumerate_cols = tfp.read_list(api.config.enumerate_cols,list(df.columns))
    if enumerate_cols :
        ncols = int(math.log10(len(enumerate_cols)))+1
        prefix_cols = tfp.read_value(api.config.prefix_cols)
        if not prefix_cols :
            prefix_cols = 'Att_'
        cols_map ={ oc : prefix_cols + str(i).zfill(ncols) for i,oc in enumerate(enumerate_cols)}
        df.rename(columns=cols_map,inplace=True)

    ###### end of doing calculation


    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        raise ValueError('DataFrame is empty')

    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['shape'] = df.shape
    att_dict['id'] = str(id(df))

    logger.debug('Columns: {}'.format(str(df.columns)))
    logger.debug('Shape (#rows - #columns): {} - {}'.format(df.shape[0],df.shape[1]))
    logger.debug('Memory: {} kB'.format(att_dict['memory']))
    example_rows = EXAMPLE_ROWS if df.shape[0] > EXAMPLE_ROWS else df.shape[0]
    for i in range(0, example_rows):
        att_dict['row_' + str(i)] = str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])
        logger.debug('Head data: {}'.format(att_dict['row_' + str(i)]))

    logger.debug('End of Process Function')
    logger.debug('End time: ' + time_monitor.elapsed_time())

    return log_stream.getvalue(), api.Message(attributes=att_dict,body=df)

inports = [{"name":"data","type":"message.DataFrame","description":"Input data"}]
outports = [{"name":"log","type":"string","description":"Logging"},\
            {"name":"data","type":"message.DataFrame","description":"Output data"}]

def call_on_input(msg) :
    log, msg = process(msg)
    api.send(outports[0]['name'],log)
    api.send(outports[1]['name'],msg)

#api.set_port_callback('data', call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: config')
    config = api.config
    config.to_nan = '0'
    config.anonymize_cols = 'col3,col4,col5'
    config.anonymize_to_int_cols = 'icol, col2'
    config.enumerate_cols = 'col3, col4, col5'
    config.prefix_cols = 'Att'

    df = pd.DataFrame(
        {'icol': [1, 2, 3, 4, 5], 'col2': ['Cydia', 'Cydia', None, 'Dani', 'Liza'], \
         'col3': ['Frank', 'Stephen', 'Hubert', 'Hubert', 'Sue'],
         'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 7, 8, 9, 10]})
    attributes = {'format': 'csv', 'name': 'DF_name'}
    #df['col5'] = df['col5'].astype('int')
    print(df.dtypes)
    test_msg = api.Message(attributes={'name':'test1'},body =df)
    log, new_msg = api.call(config,test_msg)
    print('Attributes: ', new_msg.attributes)
    print('Body: ', str(new_msg.body))
    print('Logging: ')
    print(log)
    gs.gensolution(os.path.realpath(__file__), config, inports, outports,override_readme=True)



if __name__ == '__main__':
    main()
