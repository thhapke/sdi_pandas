import pandas as pd
import re
import json

EXAMPLE_ROWS =5

def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body

    att_dict = dict()
    att_dict['config'] = dict()

    # save and reset indices
    index_names = df.index.names
    if index_names[0]  :
        df.reset_index(inplace=True)

    # prepare selection for numbers
    if api.config.selection_num  :
        if  not api.config.selection_num.upper() == 'NONE':
            selection_num_str = api.config.selection_num.replace("!=", "!")
            selection_num_str = selection_num_str.replace("==", "=")
            att_dict['config']['selection_num'] = selection_num_str
            sel_num_list = selection_num_str.split(',')

            selected_cols = dict()
            for selection in sel_num_list:

                m = re.match(u'(.+)(<=)(.+)', selection)
                if m :
                    column = m.group(1).strip().replace('"', "").replace("'", '')
                    value = float(m.group(3).strip())
                    df = df.loc[df[column] <= value]
                else :
                    m = re.match(u'(.+)(>=)(.+)', selection)
                    if m:
                        column = m.group(1).strip().replace('"', "").replace("'", '')
                        value = float(m.group(3).strip())
                        df = df.loc[df[column] >= value]
                    else :
                        m = re.match(u'(.+)([<>=!])(.+)', selection)
                        column = m.group(1).strip().replace('"',"").replace("'",'')
                        comp = m.group(2).strip()
                        value = float(m.group(3))
                        if comp == '=':
                            df = df.loc[df[column] == value]
                            selected_cols[column] = ['=',value]
                        elif comp == '>':
                            df = df.loc[df[column] > value]
                            selected_cols[column] = ['>', value]
                        elif comp == '<':
                            df = df.loc[df[column] < value]
                            selected_cols[column] = ['<', value]
                        elif comp == '!':
                            df = df.loc[df[column] != value]
                            selected_cols[column] = ['!=', value]

        # prepare selection statement
    if api.config.selection_list and not api.config.selection_list.upper() == 'NONE':
        if api.config.selection_list :
            sel_str_str = api.config.selection_list.replace("!=", "!").replace("==", "=")
            att_dict['config']['selection_list'] = sel_str_str

            m = re.match(u'(.+)([=!])(.+)', sel_str_str)
            if m :
                column = m.group(1).strip().replace('"',"").replace("'",'')
                comp = m.group(2)
                values = m.group(3).strip()
                value_list = [v.strip().replace('"', "").replace("'", '') for v in m.group(3).split(',')]
                if comp == '=':
                    df = df.loc[df[column].isin(value_list)]
                elif comp == '!':
                    df = df.loc[~df[column].isin(value_list)]
            else :
                att_dict['warning'] = "parsing error with: " + sel_str_str




    # set  index again
    if index_names[0]  :
        att_dict['indices'] = index_names
        df.set_index(keys = index_names,inplace=True)

    if df.empty :
        raise ValueError('DataFrame is empty')


    att_dict['operator'] = 'selectDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]
    if 'id' in prev_att.keys() :
        att_dict['id'] = prev_att['id'] + '; ' + att_dict['operator'] + ': ' + str(id(df))
    else :
        att_dict['id'] = att_dict['operator'] + ': ' + str(id(df))

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return  api.Message(attributes = att_dict,body=df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''

class test :
    ORDER_HEADERS = 2
    PORTAL2 = 3
    PORTAL2_1 = 4
    PORTAL2_2 = 5
    ADDRESS = 6
    SIMPLE = 0

actual_test = test.ADDRESS

try:
    api
except NameError:
    class api:

        def set_test(test_scenario):
            print('TEST SCENARIO: ' + str(test_scenario))
            if test_scenario == test.ORDER_HEADERS:
                df = pd.read_csv("/Users/d051079/OneDrive - SAP SE/Datahub-Dev/data/order_headers.csv", sep=';')
                df.set_index(keys='order_id', inplace=True)
            elif test_scenario == test.PORTAL2 or test_scenario == test.PORTAL2_1 or test_scenario == test.PORTAL2_2:
                df = pd.read_csv("/Users/d051079/data/OptRanking/portal2/portal2.csv", sep=';', nrows = 10000)
            elif test_scenario == test.ADDRESS:
                df = pd.read_csv("/Users/d051079/data/Addresses/mapped_accounts_cleansed.csv", sep=',')
            else : #test_scenario == test.SIMPLE
                df = pd.DataFrame(
                    {'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100,200,300,400,500]})

            attributes = {'format': 'csv','name':'DF_name'}

            return api.Message(attributes=attributes,body=df)

        def set_config(test_scenario) :
            if test_scenario == test.ORDER_HEADERS:
                api.config.selection_num = 'Year = 2016, Month = 5'  # operators comparisons: <,>,=,!=
                api.config.selection_list = ''  # operators comparisons: <,>,=,!=
            elif test_scenario == test.PORTAL2 :
                api.config.selection_num = "'Tarif ID' = 137360, \"Verbrauch\" = 2000"  # operators comparisons: <,>,=,!=
                api.config.selection_list = ''  #
            elif test_scenario == test.PORTAL2_1:
                api.config.selection_num = " \"Verbrauch\" > 2000, Verbrauch <5000 "  # operators comparisons: <,>,=,!=
                api.config.selection_list = ''  #
            elif test_scenario == test.PORTAL2_2:
                api.config.selection_num = "None"  # operators comparisons: <,>,=,!=
                api.config.selection_list = '\"Verbrauch\" = 2000, 3500'  #
            elif test_scenario == test.ADDRESS:
                api.config.selection_num = "None"  # operators comparisons: <,>,=,!=
                api.config.selection_list = " Source = SFDC"  #
            else : # SIMPLE
                api.config.selection_num = 'col 2 < 3, col3 >= 200 '  # operators comparisons: <,>,=,!=
                api.config.selection_list = '4'  # operators comparisons: <,>,=,!=

        class config:
            selection_num = 'order_id < 100000'  # operators comparisons: <,>,=,!=
            selection_list = 'trans_date = 2016-03-03, 2016-02-04 '  # operators comparisons: <,>,=,!=

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if not isinstance(msg,str) :
                print(msg.body.head(100))
            #else :
            #    print(msg)
            pass

        def set_port_callback(port, callback):
            msg = api.set_test(actual_test)
            api.set_config(actual_test)
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            callback(msg)

        def call(msg,config):
            api.config = config
            result = process(msg)
            return result, json.dumps(result.attributes, indent=4)


def interface(msg):
    result = process(msg)
    api.send("outDataFrameMsg", result)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
#api.set_port_callback("inDataFrameMsg", interface)

