import pandas as pd
import numpy as np
import json

from fuzzywuzzy import fuzz

EXAMPLE_ROWS = 5


def process(test_msg, base_msg):
    test_att = test_msg.attributes
    base_att = base_msg.attributes

    att_dict = dict()

    if test_att['name'] == base_att['name']:
        att_dict['name'] = test_att['name']
    else:
        att_dict['name'] = test_att['name'] + '-' + base_att['name']
    att_dict['config'] = dict()

    if api.config.test_index and not api.config.test_index.upper() == 'NONE':
        testdf_index = api.config.test_index.strip().replace("'", "").replace('"', '')
        att_dict['config']['test_index'] = testdf_index
    else:
        raise ValueError('Index of test data is mandatory')

    att_dict['number_rows'] = str(base_msg.body.shape[0])

    # get the columns to check
    if api.config.check_columns and not api.config.check_columns.upper() == 'NONE':

        colmaps = [x.strip() for x in api.config.check_columns.split(',')]
        mapping = {cm.split(':')[0].strip().replace("'", "").replace('"', ''): \
                       cm.split(':')[1].strip().replace("'", "").replace('"', '') for cm in colmaps}

        att_dict['config']['check_columns'] = str(mapping)
        att_dict['config']['limit'] = api.config.limit
        # read stream from memory
        test_df = test_msg.body

        # test if all mapping cols in testdf
        checkcols = [elem in list(test_df.columns) for elem in list(mapping.keys())]
        if not all(checkcols):
            raise ValueError('Elements in mapping are not contained in columns of test df : ' + str(
                list(mapping.keys())) + '-' + str(list(test_df.columns)) + ' - ' + str(checkcols))

        if not testdf_index in test_df.columns:
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

        t_cols = [ 't_' + t for t in mapping.keys()] + ['external_id','score']
        for  bcol in mapping.values():
            mask =  df[bcol].isna()
            df.loc[mask,t_cols] = np.nan

        if api.config.only_index:
            df = df[list(base_msg.body.columns) + ['external_id']]
        att_dict['config']['only_index'] = api.config.only_index

        if api.config.only_matching_rows:
            df = df.loc[~df['score'].isna()]
        att_dict['config']['only_matching_rows'] = api.config.only_matching_rows

        if api.config.base_index and not api.config.base_index.upper() == 'NONE':
            basedf_index = api.config.base_index.strip().replace("'", "").replace('"', '')
            att_dict['config']['base_index'] = basedf_index

        if api.config.joint_id:
            if not basedf_index :
                raise ValueError("For <joint_id> a value for <base_index> is necessary ")
            df.loc[~df['external_id'].isna(), 'joint_id'] = df.loc[~df['external_id'].isna(), 'external_id']
            df.loc[df['external_id'].isna(), 'joint_id'] = df.loc[df['external_id'].isna(), basedf_index]
        att_dict['config']['joint_id'] = api.config.joint_id

        if api.config.add_non_matching :
            # test if same columns
            if not all([elem in test_df.columns for elem in base_msg.body.columns]) :
                raise ValueError("Adding test dataframe only possible when having same columns " + str(test_df.columns) \
                                 + ' vs. ' + str(base_msg.body.columns))
            matched_ids = df['external_id'].unique()
            addto_df = test_df.loc[~test_df[testdf_index].isin(matched_ids)].copy()
            addto_df['joint_id'] = addto_df[testdf_index]
            df = pd.concat([df,addto_df],axis = 0,sort=False)
        att_dict['config']['add_non_matching'] = api.config.add_non_matching


    else:
        att_dict['warning'] = att_dict['warning'] + 'No columns to check'

    ##############################################
    #  final infos to attributes and info message
    ##############################################
    if df.empty:
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'fuzzyjoinDataFrames'
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]
    if 'id' in base_att.keys() :
        att_dict['id'] = base_att['id'] + '; ' + att_dict['operator'] + ': ' + str(id(df))
    else :
        att_dict['id'] = att_dict['operator'] + ': ' + str(id(df))

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0, example_rows):
        att_dict['row_' + str(i)] = str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])


    return api.Message(attributes=att_dict, body=df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''


class test:
    READ_BIG = 1
    SIMPLE = 0


test_scenario = test.SIMPLE

try:
    api
except NameError:
    class api:

        # input data
        def set_test(test_scenario):
            l_df = pd.read_csv("/Users/d051079/data/Addresses/uk-500-modified.csv", sep=';')
            r_df = pd.read_csv("/Users/d051079/data/Addresses/uk-500.csv", sep=',',
                               usecols=['company_name', 'address', 'city', 'county', 'postal'])

            # input data
            att1 = {'format': 'pandas', 'name': 'leftDF'}
            att2 = {'format': 'pandas', 'name': 'rightDF'}

            return api.Message(attributes=att1, body=l_df), api.Message(attributes=att2, body=r_df)

        class config:
            check_columns = 'None'
            limit = 90
            test_index = 'None'
            only_index = False
            only_matching_rows = False
            joint_id = False
            base_index = 'None'
            add_non_matching = False


        def set_config(test_scenario):
            api.config.check_columns = "'company_name':'company_name', 'address':'address','city':'city','county':'county','postal':'postal'"
            api.config.limit = 93
            api.config.test_index = 'id'
            api.config.only_index = True
            api.config.only_matching_rows = False
            api.config.joint_id = False
            api.config.base_index = 'None'
            api.config.add_non_matching = False

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if isinstance(msg, str):
                # print(msg)
                pass
            else:
                print(msg.body.head(50))
                print(msg.body.columns)
            pass

        # called by 'isolated'-test simulation
        def set_port_callback(port, callback):
            if isinstance(port, list):
                port = str(port)
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            l_msg, r_msg = api.set_test(test_scenario)
            api.set_config(test_scenario)
            callback(l_msg, r_msg)

        def call(msg1, msg2, config):
            api.config = config
            result = process(msg1, msg2)
            return result, json.dumps(result.attributes, indent=4)


def interface(test_msg, base_msg):
    result_df = process(test_msg, base_msg)
    api.send("outDataFrameMsg", result_df)
    info_str = json.dumps(result_df.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
# to be commented when imported for external 'integration' call
api.set_port_callback(["testDataFrameMsg", "baseDataFrameMsg"], interface)

