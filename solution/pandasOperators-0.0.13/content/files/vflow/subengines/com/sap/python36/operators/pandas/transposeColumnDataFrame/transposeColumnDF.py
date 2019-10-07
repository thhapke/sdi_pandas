import pandas as pd
import json

MAX_COLUMNS = 2

def process(msg):

    # test if body refers to a DataFrame type
    df = msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    #####################
    #  pd operations
    #####################

    if api.config.reset_index:
        df.reset_index(inplace=True)

    if api.config.groupby :
        if not api.config.groupby.upper()  == 'NONE' :
            gbcols = [x.strip().replace("'",'').replace('"','') for x in api.config.groupby.split(',')]

    # create DataFrame with numbered columns add concat it to df
    trans_col = api.config.transpose_column.strip().replace("'","").replace('"','')
    val_col = api.config.value_column.strip().replace("'","").replace('"','')

    tvals = list(df[trans_col].unique())
    new_cols = {trans_col + '_' + str(v): v for v in tvals}
    t_df = pd.DataFrame(columns=new_cols.keys(), index=df.index)
    df = pd.concat([df, t_df], axis=1)

    # setting the corresponding column to the value of the value column
    for col, val in new_cols.items():
        df.loc[df[trans_col] == val, col] = df.loc[df[trans_col] == val, val_col]
    df.drop(columns=[trans_col,val_col], inplace=True)

    # group df
    if api.config.groupby :
        if not api.config.groupby.upper() == 'NONE' :
            gbcols = [x.strip().replace("'", '').replace('"', '') for x in api.config.groupby.split(',')]

            aggr_trans = api.config.aggr_trans.strip()
            aggr_default = api.config.aggr_default.strip()

            aggregation = dict()
            for col in df.columns:
                aggregation[col] = aggr_trans if col in new_cols else aggr_default
            aggregation = { c:a for c,a in aggregation.items() if c not in gbcols }

            df = df.groupby(gbcols,as_index = api.config.as_index).agg(aggregation)


    #####################
    #  final infos to attributes and info message
    #####################
    prev_att = msg.attributes
    att_dict = dict()
    att_dict['config'] = dict()

    # df from body
    att_dict['operator'] = 'transposeColumnDataFrame' # name of operator
    att_dict['mem_usage'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['name'] = prev_att['name']
    att_dict['columns'] = list(df.columns)
    att_dict['number_columns'] = len(att_dict['columns'])
    att_dict['number_rows'] = len(df.index)
    att_dict['example_row_1'] = str(df.iloc[0, :].tolist())

    return api.Message(attributes=att_dict,body = df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''
class test :
    BIGDATA = 1
    SIMPLE = 0

test_scenario = test.SIMPLE

try:
    api
except NameError:
    class api:

        # input data - only used for isolated testing
        def set_test(test_scenario):
            if test_scenario == test.BIGDATA :
                df = pd.read_csv("/Users/madmax/big_data/test1.csv",sep=';')
                df.set_index(keys='index', inplace=True)
            else :
                df = pd.DataFrame(
                    {'icol': [1, 2, 3, 4, 5], 'xcol2': [1, 2, 2, 2, 3], 'xcol 3': ['A', 'B', 'B', 'B', 'C'], \
                     'xcol4': ['L', 'L', 'K', 'N', 'C']})

            # input data
            att = {'format': 'pandas','name':'test'}

            return api.Message(attributes=att,body=df)

        # setting test config data
        def set_config (test_scenario) :
            if test_scenario == test.BIGDATA :
                api.config.transpose_column = ''
                api.config.value_column = ''
                api.config.aggr_trans = 'sum'
                api.config.aggr_default = 'first'
                api.config.restet_index = False
                api.config.as_index = False
                api.config.groupby = ''
            else :  # test_scenario == test.SIMPLE:
                api.config.transpose_column = 'icol'
                api.config.value_column = 'xcol2'
                api.config.aggr_trans = 'sum'
                api.config.aggr_default = 'first'
                api.config.groupby = "'xcol 3'"
                api.config.reset_index = False
                api.config.as_index = True

        # definition of api.config - variable names should be same as in DI implementation
        class config:
            transpose_column = ''
            value_column = ''
            groupby = ''  # integer
            aggr_trans = 'sum'
            aggr_default = 'first'
            reset_index = False
            as_index = False

        # fake definition of api.Message
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        # fake definition - can be used of asserting test results
        def send(port, msg):
            if isinstance(msg,str) :
                print(msg)
            else :
                print(api.set_test(test_scenario).body)
                print(msg.body)
            pass

        # fake definition - called by 'isolated'-test simulation
        def set_port_callback(port, callback):
            if isinstance(port,list) :
                port = str(port)
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            # creates the message "send" to the inport based on the test
            msg= api.set_test(test_scenario)
            # sets the configuration based on the test
            api.set_config(test_scenario)
            # calls the "process" function
            callback(msg)

        # called by 'integrated/pipeline-test simulation
        def test_call(msg):
            print('EXTERNAL CALL of module:' + __name__)
            api.set_config(test_scenario)
            result = process(msg)
            # because when called locally via this function, 'api.set_port_callback' and 'interface' are not called
            api.send("DataFrame",result)
            return result

        def call(msg,config):
            api.config = config
            result = process(msg)
            return result, json.dumps(result.attributes, indent=4)


# gateway that gets the data from the inports and sends the result to the outports
def interface(msg):
    result= process(msg)
    api.send("outDataFrameMsg", result)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)

# Triggers the request for every message
api.set_port_callback("inDataFrameMsg", interface)
