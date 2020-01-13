
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
            tags = {'python36': ''}  # tags that helps to select the appropriate container
            operator_description = 'Custom Operator Template'
            operator_description_long='Template Operator that provides the framework for a custom operator that includes ' \
                                      'all the information needed for generating the descriptive json-files and the ' \
                                      'README.md.'
            version = "0.0.1"  # for creating the manifest.json
            add_readme = dict()
            add_readme["References"] ="[Download template](https://raw.githubusercontent.com/thhapke/gensolution/master/diutil/customOperatorTemplate.py)"

            config_params = dict()
            ## config paramter
            to_nan = 'None'
            config_params['to_nan'] = {'title': 'To NaN', 'description': 'Character to be replaced by NaN', 'type': 'string'}
            anonymize_cols = 'None'
            config_params['anonymize_cols'] = {'title': 'Anonymize Columns', \
                                                     'description': 'Anonymize columns for replacing with random strings or'
                                                                    'linear transformed numbers', \
                                                     'type': 'string'}
            anonymize_id_cols = 'None'
            config_params['anonymize_id_cols'] = {'title': 'Anonymize ID Columns',
                                                     'description': 'Anonymize ID Columns (numeric columns only)',
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
    logger, log_stream = slog.set_logging('DEBUG')
    time_monitor = tp.progress()

    result = ''
    logger.debug('Start Process Function')
    logger.debug('Start time: ' + time_monitor.get_start_time())

    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()
    warning = ''
    ###### start of doing calculation

    att_dict['config']['to_nan'] = api.config.to_nan
    to_nan = tfp.read_value(api.config.to_nan,test_number=False)
    if to_nan:
        df.replace(to_nan, np.nan, inplace=True)


    att_dict['config']['anonymize_id_cols'] = api.config.anonymize_id_cols
    anonymize_id_cols = tfp.read_list(api.config.anonymize_id_cols, list(df.columns))
    att_dict['config']['anonymize_cols'] = api.config.anonymize_cols
    anonymize_cols = tfp.read_list(api.config.anonymize_cols, list(df.columns))

    ## Anonymize columns
    if anonymize_cols :
        logger.debug('Anonymize Columns: {}'.format(str(anonymize_cols)))
        # ensure that ids are not anonymized in the section but exclusively in the id-section
        anonymize_cols = [c for c in anonymize_cols if not c in anonymize_id_cols]

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
        for c in df.select_dtypes(include='int'):
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
        for c in df.select_dtypes(include='float'):
            unique_f = df[c].unique()
            max_f = max(unique_f)
            min_f = min(unique_f)
            length = max_f - min_f
            rand_float1  = random.random()
            rand_float2 = random.random()
            df[c] = ((df[c] - min_f)/length * rand_float1 + rand_float2)/2.0

    if anonymize_id_cols :
        logger.debug('Anonymize ID Columns: {}'.format(str(anonymize_id_cols)))
        for c in anonymize_id_cols:
            unique_list = df[c].unique()
            len_unique = len(unique_list)
            if  df.shape[0] == len_unique :
                df[c] = random.sample(range(df[c].min()*100,df[c].max()*100), df.shape[0])
            else :
                logger.warning("Duplicates detected when anonymize IDs: {}".format(c))
                if pd.api.types.is_integer_dtype(df[c]) :
                    map_dict = dict(zip(unique_list,random.sample(range(df[c].min() * 100, df[c].max() * 100), len_unique)))
                elif pd.api.types.is_object_dtype(df[c].dtype) :
                    n = len_unique * 100
                    map_dict = dict(zip(unique_list,random.sample(range(n, n*10),k=len_unique)))
                else :
                    raise ValueError("Dtype <{}> cannot be anonymized_id".format(df[c].dtype))
                df[c].replace(map_dict,inplace = True)

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

    att_dict['operator'] = 'selectDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict["warnings"] = warning
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    logger.debug('End of Process Function')
    logger.debug('End time: ' + time_monitor.elapsed_time())

    return api.Message(attributes=att_dict,body=df),log_stream.getvalue()

inports = [{"name":"input","type":"message","description":"Input data"}]
outports = [{"name":"output","type":"message","description":"Output data"},{"name":"log","type":"string","description":"Logging"}]

def call_on_input(msg) :
    new_msg, log = process(msg)
    api.send(outports[0]['name'],new_msg)
    api.send(outports[1]['name'],log)

#api.set_port_callback('input', call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: config')
    config = api.config
    config.to_nan = '0'
    config.anonymize_cols = 'col2,col3,col4,col5'
    config.anonymize_id_cols = 'icol'
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
    new_msg, log = api.call(config,test_msg)
    print('Attributes: ', new_msg.attributes)
    print('Body: ', str(new_msg.body))
    print('Logging: ')
    print(log)
    gs.gensolution(os.path.realpath(__file__), config, inports, outports,override_readme=True)



if __name__ == '__main__':
    main()
