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
                {'icol': [1, 2, 2, 5, 5], 'xcol2': [1, 2, 2, 2, 3], 'xcol3': ['A', 'B', 'B', 'B', 'C'], \
                 'xcol4': ['L', 'L', 'K', 'N', 'C']})
            default_msg = api.Message(attributes={'format': 'pandas', 'name': 'test','process_list':[]},body = df)
            api.config.transpose_column = 'xcol3'
            api.config.value_column = 'xcol2'
            api.config.aggr_default = 'first'
            api.config.aggr_trans = 'count'

            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': '','sdi_utils':''}
            operator_description = "Transpose Column"
            operator_description_long = "Transposes the values of a column to new columns with the name of the values. \
            The values are taken from the value_column. The labels of the new columns are a concatination ot the\
             *transpose_column* and the value. *transpose_column* and *value_column*  are dropped."
            add_readme = dict()
            add_readme["References"] = "[pandas doc: groupby](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html)"
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            transpose_column = 'None'
            config_params['transpose_column'] = {'title': 'Transpose Column', 'description': 'Transpose Column', 'type': 'string'}
            value_column = 'None'
            config_params['value_column'] = {'title': 'Value Column', 'description': 'Value Column', 'type': 'string'}
            aggr_trans = 'None'
            config_params['aggr_trans'] = {'title': 'Aggregation of transposed column', 'description': 'Aggregation of transposed column', 'type': 'string'}
            aggr_default = 'None'
            config_params['aggr_default'] = {'title': 'Default aggregation ', 'description': 'Default aggregation', 'type': 'string'}
            groupby = 'None'
            config_params['groupby'] = {'title': 'Group by columns', 'description': 'Group by columns', 'type': 'string'}
            as_index = False
            config_params['as_index'] = {'title': 'Groupby as index', 'description': 'Groupby as index', 'type': 'boolean'}
            reset_index = False
            config_params['reset_index'] = {'title': 'Reset index', 'description': 'Reset Index', 'type': 'boolean'}
            prefix = 't_'
            config_params['prefix'] = {'title': 'Prefix of transposed values', 'description': 'Prefix of transposed values', 'type': 'string'}

def process(msg) :
    att_dict = msg.attributes
    att_dict['operator'] = 'transposeColumn'
    if api.config.debug_mode == True:
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='DEBUG')
    else :
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='INFO')
    logger.info("Process started")
    time_monitor = tp.progress()

    # start custom process definition
    df = msg.body
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Message body does not contain a pandas DataFrame')

    ###### start of doing calculation
    if api.config.reset_index:
        df.reset_index(inplace=True)
        logger.info('Reset index')

    # create DataFrame with numbered columns add concat it to df
    trans_col = tfp.read_value(api.config.transpose_column)
    logger.info('Transpose column: {}'.format(trans_col))

    val_col = tfp.read_value(api.config.value_column)
    logger.info('Value column: {}'.format(val_col))

    # new columns
    tvals = list(df[trans_col].unique())
    if api.config.prefix:
        new_cols = {trans_col + '_' + str(v): v for v in tvals}
    else:
        new_cols = {str(v): v for v in tvals}
    t_df = pd.DataFrame(columns=new_cols.keys(), index=df.index)
    df = pd.concat([df, t_df], axis=1)

    # setting the corresponding column to the value of the value column
    for col, val in new_cols.items():
        df.loc[df[trans_col] == val, col] = df.loc[df[trans_col] == val, val_col]
    df.drop(columns=[trans_col, val_col], inplace=True)

    gbcols = tfp.read_list(api.config.groupby, df.columns)
    # group df
    if gbcols:
        aggr_trans = api.config.aggr_trans.strip()
        aggr_default = api.config.aggr_default.strip()

        aggregation = dict()
        for col in df.columns:
            aggregation[col] = aggr_trans if col in new_cols else aggr_default
        aggregation = {c: a for c, a in aggregation.items() if c not in gbcols}

        df = df.groupby(gbcols, as_index=api.config.as_index).agg(aggregation)
        logger.info('Groupby: {}'.format(gbcols))


    # end custom process definition
    if df.empty :
        raise ValueError('DataFrame is empty')
    logger.debug('Columns: {}'.format(str(df.columns)))
    logger.debug('Shape (#rows - #columns): {} - {}'.format(df.shape[0],df.shape[1]))
    logger.debug('Memory: {} kB'.format(df.memory_usage(deep=True).sum() / 1024 ** 2))
    example_rows = EXAMPLE_ROWS if df.shape[0] > EXAMPLE_ROWS else df.shape[0]
    for i in range(0, example_rows):
        logger.debug('Row {}: {}'.format(i,str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])))

    progress_str = '>BATCH ENDED<'
    if 'storage.fileIndex' in att_dict and 'storage.fileCount' in att_dict and 'storage.endOfSequence' in att_dict :
        if not att_dict['storage.fileIndex'] + 1 == att_dict['storage.fileCount'] :
            progress_str = '{}/{}'.format(att_dict['storage.fileIndex'],att_dict['storage.fileCount'])
    att_dict['process_list'].append(att_dict['operator'])
    logger.debug('Past process steps: {}'.format(att_dict['process_list']))
    logger.debug('Process ended: {}  - {}  '.format(progress_str,time_monitor.elapsed_time()))

    return log_stream.getvalue(), api.Message(attributes=att_dict,body=df)

inports = [{'name': 'data', 'type': 'message.DataFrame',"description":"Input data"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame',"description":"Output data"}]

def call_on_input(msg) :
    log, msg = process(msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], msg)

#api.set_port_callback([inports[0]['name']], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback([inports[0]['name']], call_on_input)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
