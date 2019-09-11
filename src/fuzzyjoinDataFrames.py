import pandas as pd
import json

from fuzzywuzzy import fuzz

# setting display options for df
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 15)

def process(test_msg,base_msg):

    test_att = test_msg.attributes
    base_att = base_msg.attributes

    att_dict = dict()
    att_dict['operator'] = 'fuzzyjoinDataFrames'
    if test_att['name'] == base_att['name'] :
        att_dict['name'] = test_att['name']
    else :
        att_dict['name'] = test_att['name'] + '-' + base_att['name']
    att_dict['config'] = dict()
    att_dict['memory'] = dict()

    # read stream from memory
    test_df = test_msg.body
    df = base_msg.body
    tcols = ['t_' + c for c in test_df.columns]
    tdf = pd.DataFrame(columns=tcols)
    df['score'] = None
    df = pd.concat([df,tdf],axis=1)

    # get the columns to check
    if api.config.check_columns :
        colmaps = [x.strip() for x in api.config.check_columns.split(',')]
        mapping = {cm.split(':')[0].strip().replace("'", "").replace('"', ''): \
                       cm.split(':')[1].strip().replace("'", "").replace('"', '') for cm in colmaps}
        num_cols = len(mapping)
        # run over all left df rows to test in right_df
        for index, test_row in test_df.iterrows() :
            # apply function
            def get_ratio(row):
                sc = 0
                for tcol,bcol in mapping.items() :
                    sc = sc + fuzz.token_sort_ratio(test_row[tcol],row[bcol])
                return sc/num_cols

            df['tscore']= df.apply(get_ratio,axis = 1)
            # get best matching and store index in v_dict
            max_score  = df['tscore'].max()
            if max_score >= api.config.limit :
                mask = (df['tscore'] == max_score)
                df.loc[mask,'score'] = max_score
                for coli in mapping :
                    df.loc[mask,'t_'+coli] = test_row[coli]
            df.drop(columns=['tscore'],inplace=True)

        df = df.loc[~df['score'].isna()]

    else:
        att_dict['warning'] = att_dict['warning'] + 'No columns to check'

    #print(right_df.dtypes)

    att_dict['memory']['mem_usage'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = list(df.columns)
    att_dict['number_columns'] = len(att_dict['columns'])
    att_dict['number_rows'] = len(df.index)
    att_dict['example_row_1'] = str(df.iloc[0, :].tolist())

    # Serialize df, former pickle versions are restricted by 4GB
    body = df

    return api.Message(attributes=att_dict,body = body)



'''
Mock pipeline engine api to allow testing outside pipeline engine
'''
class test :
    READ_BIG = 1
    SIMPLE = 0
test_scenario = test.SIMPLE

try:
    api
except NameError:
    class api:

        # input data
        def set_test(test_scenario):
            l_df = pd.read_csv("/Users/d051079/data/Addresses/uk-500-modified.csv",sep=';')
            r_df = pd.read_csv("/Users/d051079/data/Addresses/uk-500.csv",sep=',',usecols=['company_name','address','city','county','postal'])

            # input data
            att1 = {'format': 'pandas','name':'leftDF'}
            att2 = {'format': 'pandas','name':'rightDF'}

            return api.Message(attributes=att1,body=l_df), api.Message(attributes=att2,body=r_df)

        class config:
            check_columns = 'None'
            limit = 90

        def set_config (test_scenario) :
            api.config.check_columns = "'company_name':'company_name', 'address':'address','city':'city','county':'county','postal':'postal'"
            api.config.limit = 93

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if isinstance(msg,str) :
                #print(msg)
                pass
            else :
                print(msg.body.head(50))
                print(msg.body.columns)
            pass

        # called by 'isolated'-test simulation
        def set_port_callback(port, callback):
            if isinstance(port,list) :
                port = str(port)
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            l_msg, r_msg = api.set_test(test_scenario)
            api.set_config(test_scenario)
            callback(l_msg,r_msg)

        # called by 'integrated/pipeline-test simulation
        def test_call(l_msg, r_msg,scenario):
            print('EXTERNAL CALL of module:' + __name__)
            api.set_config(scenario)
            result = process(l_msg,r_msg)
            api.send("outDataFrame",result)
            return result

        def call(msg1,msg2,config):
            api.config = config
            result = process(msg1,msg2)
            return result, json.dumps(result.attributes, indent=4)

def interface(test_msg,base_msg):
    result_df = process(test_msg,base_msg)
    api.send("outDataFrameMsg", result_df)
    info_str = json.dumps(result_df.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
# to be commented when imported for external 'integration' call
api.set_port_callback(["testDFMsg","baseDFMsg"], interface)

