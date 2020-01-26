import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp

import pandas as pd

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
    
        def call(config,msg):
            api.config = config
            return process(msg)
            
        def set_port_callback(port, callback) :
            df = pd.DataFrame({'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100, 200, 300, 400, 500],\
                               'col4': ['A', 'A', "A", "B", 'B']})
            attributes = {'format': 'csv', 'name': 'DF_name'}
            default_msg = api.Message(attributes=attributes, body=df)
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            tags = {'pandas': '','sdi_utils':''}
            version = "0.0.17"
            operator_description = "Drop Columns"
            operator_description_long = "Drops or/and renames DataFrame columns"
            add_readme = dict()
            add_readme["References"] = r"""
[pandas doc: drop](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop.html)
[pandas doc: rename](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rename.html)"""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            drop_columns = 'None'
            config_params['drop_columns'] = {'title': 'Columns to drop', 'type': 'string'}
            config_params['drop_columns']['description'] =r"""* *comma separated list of columns*: columns to drop
* *NOT: comma separated list of columns*: drop all columns except columns in the list
* *ALL* : drop all columns and reset index - same as *NOT*"""
            rename_columns = 'None'
            config_params['rename_columns'] = {'title': 'Rename Columns',  'type': 'string'}
            config_params['rename_columns']['description'] = r"""*  *comma separated list of mappings*: columns to be
renamed, e.g. Col1:col_1, Col2:col_2"""



def process(msg) :

    att_dict = dict()
    att_dict['config'] = dict()

    att_dict['operator'] = 'dropColumns'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

    # start custom process definition
    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Message body does not contain a pandas DataFrame')

    ###### start of doing calculation
    att_dict['config']['drop_columns'] = api.config.drop_columns
    drop_cols = tfp.read_list(api.config.drop_columns, df.columns)
    if drop_cols:
        logger.debug("Drops columns: {}".format(str(drop_cols)))
        df = df.drop(columns=drop_cols)

    att_dict['config']['rename_columns'] = api.config.rename_columns
    map_names = tfp.read_dict(api.config.rename_columns)
    if map_names:

        df.rename(columns=map_names, inplace=True)
    ###### end of doing calculation

    ##############################################
    #  final infos to attributes and info message
    ##############################################

    # df from body
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
    msg = api.Message(attributes=att_dict,body=df)
    return log, msg


inports = [{"name":"data","type":"message.DataFrame","description":"Input data"}]
outports = [{"name":"log","type":"string","description":"Logging"},\
            {"name":"data","type":"message.DataFrame","description":"Output data"}]

def call_on_input(msg) :
    log, msg = process(msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], msg)

#api.set_port_callback(inports[0]['name'], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

    config = api.config
    config.drop_columns = "'col 2', col3"

    df = pd.DataFrame({'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100, 200, 300, 400, 500], \
                       'col4': ['A', 'A', "A", "B", 'B']})
    attributes = {'format': 'csv', 'name': 'DF_name'}
    msg = api.Message(attributes=attributes, body=df)
    log, msg = api.call(config,msg)
    api.send(outports[1]['name'],msg)
    api.send(outports[0]['name'], log)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
