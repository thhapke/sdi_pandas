import pandas as pd
import os
import io
import json
import re
import logging

EXAMPLE_ROWS = 5


def downcast(df, data_type, to_type):
    cols = list(df.select_dtypes(include=[data_type]).columns)
    if len(cols) == 0:
        return df, None

    downcast_dict = dict()
    downcast_dict['data_type'] = data_type
    cdtypes = df[cols].dtypes.to_dict()
    downcast_dict['previous_subtypes'] = {col: str(itype) for col, itype in cdtypes.items()}
    downcast_dict['previous_mem_usage'] = df[cols].memory_usage(deep=True).sum() / 1024 ** 2

    df[cols] = df[cols].apply(pd.to_numeric, downcast=to_type)

    int_dtypes2 = df[cols].dtypes.to_dict()
    downcast_dict['subtypes'] = {col: str(itype) for col, itype in int_dtypes2.items()}
    downcast_dict['mem_usage'] = df[cols].memory_usage(deep=True).sum() / 1024 ** 2

    return df, downcast_dict


def process(msg):
    att_dict = dict()
    att_dict['config'] = dict()
    att_dict['warning'] = ''


    global result_df

    # json string of attributes already converted to dict
    # att_dict['prev_attributes'] = msg.attributes
    att_dict['filename'] = msg.attributes["storage.filename"]

    # using file name from attributes of ReadFile
    if not api.config.df_name or api.config.df_name == "DataFrame":
        att_dict['name'] = att_dict['filename'].split(".")[0]

    if isinstance(msg.body, str):
        csv_io = io.StringIO(msg.body)
        att_dict['input_format'] = 'StringIO'
    elif isinstance(msg.body, bytes):
        csv_io = io.BytesIO(msg.body)
        att_dict['input_format'] = 'BytesIO'
    elif isinstance(msg.body, io.BytesIO):
        att_dict['input_format'] = 'string'
        csv_io = msg.body
    else:
        raise TypeError('Message body has unsupported type' + str(type(msg.body)))

    # thousands set api.config to none if '' or 'None'
    if not api.config.thousands or api.config.thousands.upper() == 'NONE' :
        api.config.thousands = None

    # nrows
    nrows = None
    if not api.config.limit_rows == 0:
        nrows = api.config.limit_rows

    # usecols
    use_cols = None
    if api.config.use_columns and not api.config.use_columns == 'None':
        use_cols = [x.strip().replace("'",'').replace('"','') for x in api.config.use_columns.split(',')]
        att_dict['config']['use_columns'] = str(use_cols)

    # dtypes mapping
    typemap = None
    if api.config.dtypes and not api.config.dtypes == 'None':
        # mapping
        colmaps = [x.strip() for x in api.config.dtypes.split(',')]
        typemap = {cm.split(':')[0].strip().replace("'", "").replace('"', ''): \
                       cm.split(':')[1].strip().replace("'", "").replace('"', '') for cm in colmaps}
        #print(typemap)

    # compressed
    compression = None
    if api.config.compression and not api.config.compression == 'None':
        compression = api.config.compression

    ##### Read string from buffer
    if not compression :
        df = pd.read_csv(csv_io, api.config.separator, usecols=use_cols, error_bad_lines=False,dtype=typemap,\
                         warn_bad_lines=api.config.error_bad_lines,\
                         thousands = api.config.thousands,decimal = api.config.decimal,nrows=nrows)
    else :
        df = pd.read_csv(csv_io, api.config.separator,usecols=use_cols, error_bad_lines=False, \
                         warn_bad_lines=api.config.error_bad_lines, dtype=typemap,\
                         thousands=api.config.thousands, decimal=api.config.decimal, \
                         compression=compression, encoding='latin-1',nrows=nrows)

    # Data from filename
    if api.config.data_from_filename and not api.config.data_from_filename == 'None':
        col = api.config.data_from_filename.split(':')[0].strip().strip("'").strip('"')
        pat = api.config.data_from_filename.split(':')[1].strip().strip("'").strip('"')
        logging.debug('Filename: {}  pattern: {}'.format(att_dict['filename'],pat))
        try :
            dataff = re.match('.*(\d{4}-\d+-\d+).*',att_dict['filename'])
            df[col] = dataff.group(1)
        except AttributeError :
            raise ValueError('Pattern not found - Filename: {}  pattern: {}'.format(att_dict['filename'],pat))


    # To Datetime
    if api.config.todatetime and not api.config.todatetime == 'None':
        coldate = api.config.todatetime.split(':')[0].strip().strip("'").strip('"')
        dformat = api.config.todatetime.split(':')[1].strip().strip("'").strip('"')
        df[coldate] = pd.to_datetime(df[coldate], format=dformat)

    ###### Downcasting
    # save memory footprint for calculating the savings of the downcast
    att_dict['previous_memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    if api.config.downcast_int:
        df, dci = downcast(df, 'int', 'unsigned')
    if api.config.downcast_float:
        df, dcf = downcast(df, 'float', 'float')

    # check if index is provided and set
    if api.config.index_cols and not api.config.index_cols == 'None':
        index_list = [x.strip() for x in api.config.index_cols.split(',')]
        att_dict['config']['index_cols'] = str(index_list)
        att_dict['index_cols'] = str(index_list)
        df.set_index(index_list, inplace=True)

    # stores the result in global variable result_df
    if  msg.attributes['storage.fileIndex'] == 0 :
        result_df = df
    else :
        result_df = pd.concat([result_df,df],axis=0)

    ##############################################
    #  final infos to attributes and info message
    ##############################################
    att_dict['operator'] = 'fromCSVDataFrame'
    att_dict['memory'] = result_df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = list(result_df.columns)
    att_dict['dtypes'] = {col:str(ty) for col,ty in df.dtypes.to_dict().items()}
    att_dict['number_columns'] = result_df.shape[1]
    att_dict['number_rows'] = result_df.shape[0]
    att_dict['id'] =  str(id(result_df))
    att_dict['storage.fileIndex'] = msg.attributes['storage.fileIndex']
    att_dict['storage.fileCount'] = msg.attributes['storage.fileCount']

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in result_df.iloc[i, :].tolist()])

    return api.Message(attributes=att_dict, body=result_df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''


class test:
    ORDER_HEADERS = 2
    ORDER_DETAILS = 3
    PRODUCTS_MD = 6
    SIMPLE_BINARY = 0
    SIMPLE_STRING = 1
    ORDER_HEADERS_LIMITED = 4
    ORDER_DETAILS_LIMITED = 5
    PRODUCTS_MD_LIMITED = 7
    PORTAL2 = 8

actual_test = test.SIMPLE_BINARY

try:
    api
except NameError:
    class api:

        def set_test(test_scenario):
            print('TEST SCENARIO: ' + str(test_scenario))
            if test_scenario == test.ORDER_HEADERS or test_scenario == test.ORDER_HEADERS_LIMITED:
                f = open("/Users/d051079/OneDrive - SAP SE/Datahub-Dev/data/order_headers.csv", mode='r')
                csv = ''
                for line in f.readlines():
                    csv += line
            elif test_scenario == test.ORDER_DETAILS or test_scenario == test.ORDER_DETAILS_LIMITED:
                f = open("/Users/d051079/OneDrive - SAP SE/Datahub-Dev/data/order_details.csv", mode='r')
                csv = ''
                for line in f.readlines():
                    csv += line
            elif test_scenario == test.PRODUCTS_MD or test_scenario == test.PRODUCTS_MD_LIMITED:
                f = open("/Users/d051079/OneDrive - SAP SE/Datahub-Dev/data/products_md.csv", mode='r')
                csv = ''
                for line in f.readlines():
                    csv += line
            elif test_scenario == test.PORTAL2 :
                f = open("/Users/d051079/data/OptRanking/portal2/portal2.csv", mode='r')
                csv = ''
                size = 10000
                for line in f.readlines(size):
                    csv += line

            elif test_scenario == test.SIMPLE_STRING:
                csv = """col1;col2;col3
                     1;4.4;99
                     2;4.5;200
                     3;4.7;65
                     4;3.2;140
                     """
            else:  # testdata.BINARY_STRING
                csv = b"""col1;col2;col3
                         1;4.4;99
                         2;4.5;200
                         3;4.7;65
                         4;3.2;140
                         """
            attributes = {'format': 'csv',"storage.filename" : 'filename','storage.endOfSequence': True, \
                          'storage.fileIndex': 0 ,'storage.fileCount':1 }

            return api.Message(attributes=attributes, body=csv)

        def set_config(test_scenario):
            if test_scenario == test.ORDER_DETAILS:
                api.config.index_cols = "order_id, product_id"
                api.config.separator = ';'
                api.config.error_bad_lines = False
                api.config.use_columns = 'order_id, product_id'
                api.config.limit_rows = 0
                api.config.downcast_int = True
                api.config.downcast_float = True
                api.config.df_name = 'order_details'
                api.config.compression = 'None'
            elif test_scenario == test.ORDER_HEADERS:
                api.config.index_cols = "order_id"
                api.config.separator = ';'
                api.config.error_bad_lines = False
                api.config.use_columns = 'order_id, shopper_id,num_products, w_day, hour,location_id, Year,Month, Week'
                api.config.limit_rows = 0
                api.config.downcast_int = True
                api.config.downcast_float = True
                api.config.df_name = 'order_headers'
                api.config.compression = 'None'
            elif test_scenario == test.ORDER_DETAILS_LIMITED:
                api.config.index_cols = "order_id, product_id"
                api.config.separator = ';'
                api.config.error_bad_lines = False
                api.config.use_columns = 'order_id, product_id'
                api.config.limit_rows = 100000
                api.config.downcast_int = True
                api.config.downcast_float = True
                api.config.df_name = 'order_details'
                api.config.compression = 'None'
            elif test_scenario == test.ORDER_HEADERS_LIMITED:
                api.config.index_cols = "order_id"
                api.config.separator = ';'
                api.config.error_bad_lines = False
                api.config.use_columns = 'order_id, num_products, total_basket, w_day, hour,location_id, Year,Month, Week'
                api.config.limit_rows = 100000
                api.config.downcast_int = True
                api.config.downcast_float = True
                api.config.df_name = 'order_headers'
                api.config.compression = 'None'
            elif test_scenario == test.PRODUCTS_MD:
                api.config.index_cols = "product_id"
                api.config.separator = ';'
                api.config.error_bad_lines = False
                api.config.use_columns = 'product_id,category_id'
                api.config.limit_rows = 0
                api.config.downcast_int = True
                api.config.downcast_float = False
                api.config.df_name = 'products_md'
                api.config.compression = 'None'
            elif test_scenario == test.PORTAL2:
                api.config.index_cols = "None"
                api.config.separator = ';'
                api.config.error_bad_lines = False
                api.config.use_columns = "'Verbrauch',\"PLZ\",'ort','Tarif ID','Rank','Gesamtpreis inkl Bonus'"
                api.config.limit_rows = 0
                api.config.downcast_int = True
                api.config.downcast_float = False
                api.config.df_name = 'portal2'
                api.config.compression = 'None'
            else:  # testconfig.NO_INDEX:
                api.config.index_cols = "None"
                api.config.separator = ';'
                api.config.error_bad_lines = False
                api.config.use_columns = ''
                api.config.limit_rows = 0
                api.config.downcast_int = False
                api.config.downcast_float = False
                api.config.df_name = 'test_df'
                api.config.compression = 'None'
                api.config.data_from_filename = 'None'

        class config:
            index_cols = "None"
            separator = ';'
            error_bad_lines = False
            use_columns = ''
            limit_rows = 0
            dtypes = 'None'
            df_name = 'DataFrame'
            downcast_float = False
            downcast_int = False
            thousands = None
            decimal = '.'
            compression = 'None'
            data_from_filename = 'None'
            todatetime = 'None' # "'Date' : '%Y-%m-%d'"

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if isinstance(msg, str):
                print(msg)
            else :
                print(msg.body.head(2))
            pass

        # called by 'isolated'-test simulation
        def set_port_callback(port, callback):
            msg = api.set_test(actual_test)
            api.set_config(actual_test)
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            callback(msg)

        def call(msg,config):
            api.config = config
            commit_token = "0"
            if msg.attributes["storage.endOfSequence"]:
                commit_token = "1"
            out_msg = process(msg)
            out_msg.attributes['commit.token'] = commit_token
            info = out_msg.attributes

            if commit_token == "1":
                return out_msg, json.dumps(out_msg.attributes, indent=4)
            else :
                return None, json.dumps(out_msg.attributes, indent=4)

def interface(msg):
    # inform downstream operators about last file:
    # set message.commit.token = 1 for last file

    commit_token = "0"
    if msg.attributes["storage.endOfSequence"]:
        commit_token = "1"

    out_msg = process(msg)
    out_msg.attributes['commit.token'] = commit_token

    if commit_token == "1" :
        api.send("outDataFrameMsg", out_msg)
    info_str = json.dumps(out_msg.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message
# to be commented when imported for external 'integration' call
api.set_port_callback("inCSVMsg", interface)

