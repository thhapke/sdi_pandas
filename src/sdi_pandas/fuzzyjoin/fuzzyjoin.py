import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
from fuzzywuzzy import fuzz
import pandas as pd
import numpy as np

EXAMPLE_ROWS = 5

try:
    api
except NameError:
    class api:
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
    
        def call(config,test_msg,base_msg):
            api.config = config
            return process(test_msg,base_msg)
            
        def set_port_callback(port1, port2, callback) :
            l_df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'Name': ['Hans', 'Peter', 'Carl', 'Justin', 'Eric'], \
                 'City': ['Berlin', 'Berlin', 'Paris', 'London', 'London'],\
                 'Profession': ['Accountant', 'Accountant', 'Designer', 'Developer', 'Developer'], \
                 'Age': [25, 30, 45, 55, 65]})
            l_msg = api.Message(attributes={'format': 'pandas', 'name': 'leftDF'}, body= l_df)
            r_df = pd.DataFrame(
                {'icol': [10, 20, 30, 40, 50], 'First Name': ['Hansi', 'Claire', 'Karl', 'Justice', 'Erik'], \
                 'city': ['Berlin', 'Berlin', 'Paris', 'London', 'London'],\
                 'Profession': ['Accountant', 'Accountant', 'Web-Designer', 'Developer', 'Developer'], \
                 'Age': [25, 30, 45, 55, 65]})
            r_msg = api.Message(attributes={'format': 'pandas', 'name': 'rightDF'},body=r_df)

            callback(l_msg,r_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'fuzzywuzzy': '', 'pandas': '','sdi_utils':''}
            operator_description = "Fuzzy Join"
            operator_description_long ="A test datasets (testDataFrame) are checked if they (string-) match with a\
             base dataset (baseDataFrame). If more than one column are provided for checking then the average is \
             calculated of all columns."
            add_readme = dict()
            add_readme["References"] = r"""
[pandas doc: groupby](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html)
[fuzzywuzzy](https://github.com/seatgeek/fuzzywuzzy)"""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            check_columns = 'None'
            config_params['check_columns'] = {'title': 'Columns to check', 'description': 'Columns to check', 'type': 'string'}
            limit = 90
            config_params['limit'] = {'title': 'Matching Limit', 'description': 'Matching Limit', 'type': 'integer'}
            test_index = 'test'
            config_params['test_index'] = {'title': 'Index of Test DataFrame', 'description': 'Index of Test DataFrame', 'type': 'string'}
            only_matching_rows = False
            config_params['only_matching_rows'] = {'title': 'Matching Rows only', 'description': 'Matching Rows only', 'type': 'boolean'}
            only_index = False
            config_params['only_index'] = {'title': 'Add only Index to DataFrame', 'description': 'Add only Index to DataFrame', 'type': 'boolean'}
            joint_id = False
            config_params['joint_id'] = {'title': 'Add joint id', 'description': 'Add joint id', 'type': 'boolean'}
            base_index = 'None'
            config_params['base_index'] = {'title': 'Index of Base Dataset', 'description': 'Index of Base Dataset', 'type': 'string'}
            add_non_matching = False
            config_params['add_non_matching'] = {'title': 'Add non matching datasets', 'description': 'Add non matching datasets', 'type': 'boolean'}


def process(test_msg, base_msg) :
    att_dict = dict()
    att_dict['config'] = dict()

    att_dict['operator'] = 'fuzzyjoin'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

    # start custom process definition
    test_att = test_msg.attributes
    base_att = base_msg.attributes

    if test_att['name'] == base_att['name']:
        att_dict['name'] = test_att['name']
    else:
        att_dict['name'] = test_att['name'] + '-' + base_att['name']
    att_dict['config'] = dict()

    att_dict['config']['test_index'] = api.config.test_index
    testdf_index = tfp.read_value(api.config.test_index)
    if not testdf_index:
        logger.error('Index of test data is mandatory')
        raise ValueError('Index of test data is mandatory')

    att_dict['number_rows'] = str(base_msg.body.shape[0])

    # get the columns to check

    mapping = tfp.read_dict(api.config.check_columns)
    df = pd.DataFrame()

    if mapping:

        att_dict['config']['check_columns'] = str(mapping)
        att_dict['config']['limit'] = api.config.limit

        # read stream from memory
        test_df = test_msg.body

        # test if all mapping cols in testdf
        checkcols = [elem in list(test_df.columns) for elem in list(mapping.keys())]
        if not all(checkcols):
            error_txt = 'Elements in mapping are not contained in columns of test df : ' + \
                        str(list(mapping.keys())) + '-' + str(list(test_df.columns)) + ' - ' + str(checkcols)
            logger.error(error_txt)
            raise ValueError(error_txt)

        if not testdf_index in test_df.columns:
            logger.error('Test index needs to be column')
            raise ValueError('Test index needs to be column')

        tcols = ['t_' + c for c in list(mapping.keys())]
        tdf = pd.DataFrame(columns=tcols)

        df = base_msg.body
        df = pd.concat([df, tdf], axis=1)

        num_cols = len(mapping)
        # run over all left df rows to test in right_df
        for index, test_row in test_df.iterrows():
            # apply function
            def get_ratio(row):
                sc = 0
                for tcol, bcol in mapping.items():
                    sc = sc + fuzz.token_sort_ratio(test_row[tcol], row[bcol])
                return sc / num_cols

            df['tscore'] = df.apply(get_ratio, axis=1)
            # get best matching and store index in v_dict
            max_score = df['tscore'].max()
            if max_score >= api.config.limit:
                mask = (df['tscore'] == max_score)
                df.loc[mask, 'score'] = max_score
                df.loc[mask, 'external_id'] = test_row[testdf_index]
                for coli in mapping:
                    df.loc[mask, 't_' + coli] = test_row[coli]

            df.drop(columns=['tscore'], inplace=True)

        # remove external_id when test column value has none

        t_cols = ['t_' + t for t in mapping.keys()] + ['external_id', 'score']
        for bcol in mapping.values():
            mask = df[bcol].isna()
            df.loc[mask, t_cols] = np.nan

        if api.config.only_index:
            df = df[list(base_msg.body.columns) + ['external_id']]
        att_dict['config']['only_index'] = api.config.only_index

        if api.config.only_matching_rows:
            df = df.loc[~df['score'].isna()]
        att_dict['config']['only_matching_rows'] = api.config.only_matching_rows

        basedf_index = tfp.read_value(api.config.base_index)
        att_dict['config']['base_index'] = basedf_index

        if api.config.joint_id:
            if not basedf_index:
                raise ValueError("For <joint_id> a value for <base_index> is necessary ")
            df.loc[~df['external_id'].isna(), 'joint_id'] = df.loc[~df['external_id'].isna(), 'external_id']
            df.loc[df['external_id'].isna(), 'joint_id'] = df.loc[df['external_id'].isna(), basedf_index]
        att_dict['config']['joint_id'] = api.config.joint_id

        if api.config.add_non_matching:
            # test if same columns
            if not all([elem in test_df.columns for elem in base_msg.body.columns]):
                raise ValueError("Adding test dataframe only possible when having same columns " + str(test_df.columns) \
                                 + ' vs. ' + str(base_msg.body.columns))
            matched_ids = df['external_id'].unique()
            addto_df = test_df.loc[~test_df[testdf_index].isin(matched_ids)].copy()
            addto_df['joint_id'] = addto_df[testdf_index]
            df = pd.concat([df, addto_df], axis=0, sort=False)
        att_dict['config']['add_non_matching'] = api.config.add_non_matching

    else:
        logger.warning('No columns to check')

    ##############################################
    #  final infos to attributes and info message
    ##############################################
    if df.empty:
        logger.warning('DataFrame is empty')
    else :
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

    # end custom process definition

    log = log_stream.getvalue()
    msg = api.Message(attributes=att_dict,body = df)
    return log, msg


inports = [{'name': 'testdata', 'type': 'message.DataFrame',"description":"Input test data"},\
           {'name': 'basedata', 'type': 'message.DataFrame',"description":"Input base data"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame',"description":"Output data"}]


def call_on_input(test_msg, base_msg) :
    log, msg = process(test_msg, base_msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], msg)

#api.set_port_callback(inports[0]['name'], inports[1]['name'], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], inports[1]['name'], call_on_input)

    config = api.config
    config.limit = 90
    config.base_index = 'icol'
    config.joint_id = 'icol'
    config.test_index = 'icol'
    config.check_columns = "'Name':'First Name', City:city"
    test_df = pd.DataFrame(
        {'icol': [1, 2, 3, 4, 5], 'Name': ['Hans', 'Peter', 'Carl', 'Justin', 'Eric'], \
         'City': ['Berlin', 'Berlin', 'Paris', 'London', 'London'], \
         'Profession': ['Accountant', 'Accountant', 'Designer', 'Developer', 'Developer'], \
         'Age': [25, 30, 45, 55, 65]})
    test_msg = api.Message(attributes={'format': 'pandas', 'name': 'leftDF'}, body=test_df)
    base_df = pd.DataFrame(
        {'icol': [10, 20, 30, 40, 50], 'First Name': ['Hansi', 'Claire', 'Karl', 'Justice', 'Erik'], \
         'city': ['Berlin', 'Berlin', 'Paris', 'London', 'London'], \
         'Profession': ['Accountant', 'Accountant', 'Web-Designer', 'Developer', 'Developer'], \
         'Age': [25, 30, 45, 55, 65]})
    base_msg = api.Message(attributes={'format': 'pandas', 'name': 'rightDF'}, body=base_df)
    log, msg = api.call(config,test_msg,base_msg)

    api.send(outports[0]['name'],log)
    api.send(outports[1]['name'],msg)


if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
