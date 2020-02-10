import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp
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
            df = pd.DataFrame(
                {'icol': [1, 1, 1, 1, 2], 'xcol 2': ['A', 'A', 'B', 'B', 'C'], 'xcol 3': [1, 1, 2, 2, 3],
                 'xcol4': ['a', 'a', 'b', 'a', 'b']})
            default_msg = api.Message(attributes = {'format': 'csv', 'name': 'DF_name','process_list':[]}, body=df)
            api.config.groupby = "'icol', 'xcol 2'"
            api.config.aggregation = "'xcol 3':'sum', 'xcol4':'count'"
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': '','sdi_utils':''}
            operator_description = "Group by"
            operator_description_long = "Groups the named columns by using the given aggregations."
            add_readme = dict()
            add_readme["References"] = "[pandas doc: grouby](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html)"

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            groupby = 'None'
            config_params['groupby'] = {'title': 'Groupby Columns', 'description': 'List of comma separated columns to group', 'type': 'string'}
            aggregation = 'None'
            config_params['aggregation'] = {'title': 'Aggregation Mapping', 'description': 'List of comma separated mappings of columns with the type of aggregation, e.g. price:mean,city:count', 'type': "string"}
            index = False
            config_params['index'] = {'title': 'Set Index', 'description': 'Set Index', 'type': 'boolean'}
            drop_columns = 'None'
            config_params['drop_columns'] = {'title': 'Drop Columns', 'description': "List of columns of the joined DataFrame that could be dropped.", 'type': 'string'}


def process(msg) :
    att_dict = msg.attributes
    att_dict['operator'] = 'groupby'
    if api.config.debug_mode == True:
        logger, log_stream = slog.set_logging(att_dict['operator'], loglevel='DEBUG')
    else:
        logger, log_stream = slog.set_logging(att_dict['operator'], loglevel='INFO')
    logger.info("Process started")
    time_monitor = tp.progress()

    prev_att = msg.attributes
    df = msg.body
    prev_shape = df.shape

    ###### start of doing calculation

    # groupby list
    cols = tfp.read_list(api.config.groupby)

    # mapping aggregation
    try :
        colagg = tfp.read_dict(api.config.aggregation)
    except IndexError :
        logger.info('Aggregation is not a map, try to parse a value instead')
        colagg = tfp.read_value(api.config.aggregation)

    # groupby
    logger.info('Group columns: {}'.format(cols))
    logger.info('Aggregation: {}'.format(colagg))
    logger.info('Index: {}'.format(api.config.index))
    df = df.groupby(cols, as_index=api.config.index).agg(colagg)

    # drop col
    dropcols = tfp.read_list(api.config.drop_columns)
    if dropcols :
        logger.info('Drop columns: {}'.format(dropcols))
        df.drop(columns=dropcols,inplace=True)

    # end custom process definition
    if df.empty:
        raise ValueError('DataFrame is empty')
    logger.debug('Columns: {}'.format(str(df.columns)))
    logger.debug('Shape (#rows - #columns): {} - {}'.format(df.shape[0], df.shape[1]))
    logger.debug('Memory: {} kB'.format(df.memory_usage(deep=True).sum() / 1024 ** 2))
    example_rows = EXAMPLE_ROWS if df.shape[0] > EXAMPLE_ROWS else df.shape[0]
    for i in range(0, example_rows):
        logger.debug('Row {}: {}'.format(i, str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])))

    progress_str = '>BATCH ENDED<'
    if 'storage.fileIndex' in att_dict and 'storage.fileCount' in att_dict and 'storage.endOfSequence' in att_dict:
        if not att_dict['storage.fileIndex'] + 1 == att_dict['storage.fileCount']:
            progress_str = '{}/{}'.format(att_dict['storage.fileIndex'], att_dict['storage.fileCount'])
    att_dict['process_list'].append(att_dict['operator'])
    logger.debug('Past process steps: {}'.format(att_dict['process_list']))
    logger.debug('Process ended: {}  - {}  '.format(progress_str, time_monitor.elapsed_time()))

    return log_stream.getvalue(), api.Message(attributes=att_dict, body=df)


inports = [{'name': 'data', 'type': 'message.DataFrame',"description":"Input data"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame',"description":"Output data"}]


def call_on_input(msg) :
    log, msg = process(msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], msg)

#api.set_port_callback(inports[0]['name'], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: config')
    config = api.config
    config.groupby = 'xcol2'
    config.aggregation = 'first'
    config.index = False
    config.drop_columns = 'None'
    df = pd.DataFrame(
        {'icol': [1, 1, 1, 1, 2], 'xcol2': ['A', 'A', 'B', 'B', 'C'], 'xcol 3': [1, 1, 2, 2, 3],
         'xcol4': ['a', 'a', 'b', 'a', 'b']})
    attributes = {'format': 'csv', 'name': 'DF_name','process_list':[]}
    input_msg = api.Message(attributes=attributes, body=df)
    log, msg = api.call(config, input_msg)
    print('Input')
    print(input_msg.body)
    print('Output')
    print(msg.body)




if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
