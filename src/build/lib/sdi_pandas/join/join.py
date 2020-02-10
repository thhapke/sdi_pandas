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
    
        def call(config,left_msg,right_msg):
            api.config = config
            return process(left_msg,right_msg)
            
        def set_port_callback(port, callback) :
            l_df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'xcol2': ['A', 'B', 'C', 'D', 'E'], 'xcol3': ['K', 'L', 'M', 'N', 'O']})
            l_df.set_index(keys='icol', inplace=True)
            l_msg = api.Message(attributes={'format': 'pandas','name':'leftDF','process_list':[]},body = l_df)
            r_df = pd.DataFrame(
                {'icol': [3, 4, 5, 6, 7], 'ycol2': ['C', 'D', 'E', 'F', 'G'], 'ycol3': ['M', 'N', 'O', 'P', 'Q']})
            r_df.set_index(keys='icol', inplace=True)
            r_msg = api.Message(attributes={'format': 'pandas', 'name': 'rightDF','process_list':[]}, body=r_df)
            api.config.left_on='icol'
            api.config.right_on='icol'
            callback(l_msg,r_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': '','sdi_utils':''}
            operator_description = "Join Dataframes"
            operator_description_long = "Joining 2 DataFrames using either the indices of both or on specified columns. Setting the new index ist necessary."
            add_readme = dict()
            add_readme["References"] = r"""[pandas doc: .merge](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.merge.html)"""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            how = 'inner'
            config_params['how'] = {'title': 'How to join', 'description': 'How to join 2 DataFrames', 'type': 'string', 'enum': ['inner', 'outer', 'left', 'right']}
            on_index = False
            config_params['on_index'] = {'title': 'On index', 'description': 'Join on indices of both DataFrames', 'type': 'boolean'}
            left_on = 'None'
            config_params['left_on'] = {'title': 'Left df column to join', 'description': 'Left DataFrame column to join', 'type': 'string'}
            right_on = 'None'
            config_params['right_on'] = {'title': 'Right df column to join', 'description': 'Right DataFrame column to join', 'type': 'string'}
            drop_columns = 'None'
            config_params['drop_columns'] = {'title': 'Drop columns', 'description': 'List of columns to drop after join', 'type': 'string'}
            new_indices = ''
            config_params['new_indices'] = {'title': 'New index or index list', 'description': 'New index/list after merge', 'type': 'string'}


def process(left_msg, right_msg) :

    att_dict = left_msg.attributes
    att_dict['operator'] = 'join'
    if api.config.debug_mode == True:
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='DEBUG')
    else :
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='INFO')
    logger.info("Process started")
    time_monitor = tp.progress()

    # start custom process definition

    l_att = left_msg.attributes
    r_att = right_msg.attributes

    # read stream from memory
    left_df = left_msg.body
    right_df = right_msg.body

    ###### start of doing calculation
    how = tfp.read_value(api.config.how)

    # merge according to config
    if api.config.on_index:
        df = pd.merge(left_df, right_df, how=how, left_index=True, right_index=True)
    elif api.config.left_on and api.config.right_on:
        left_on_list = tfp.read_list(api.config.left_on)
        right_on_list = tfp.read_list(api.config.right_on)
        logger.info('Join DataFrames on {} - {}'.format(left_on_list,right_on_list))
        left_df.reset_index(inplace=True)
        right_df.reset_index(inplace=True)

        df = pd.merge(left_df, right_df, how=how, left_on=left_on_list, right_on=right_on_list)

        # removing second index - might be a more elegant solution
        if 'index_x' in df.columns:
            df.drop(columns=['index_x'], inplace=True)
    else:
        raise ValueError(
            "Config setting: Either <on> or both <left_on> and <right_on> has to be set in order to join the dataframes")

    index_list = tfp.read_list(api.config.new_indices)
    if index_list:
        df.set_index(keys=index_list, inplace=True)
        logger.info('Set index: {}'.format(index_list))

    col_list = tfp.read_list(api.config.drop_columns)
    if col_list:
        df.drop(labels=col_list, axis=1, inplace=True)
        logger.info('Drop columns: {}'.format(col_list))

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


inports = [{'name': 'left_input', 'type': 'message.DataFrame',"description":"Left input data"}, \
           {'name': 'right_input', 'type': 'message.DataFrame',"description":"Right input data"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging"},\
            {'name': 'output', 'type': 'message.DataFrame',"description":"Output data"}]

def call_on_input(left_msg, right_msg) :
    log, msg = process(left_msg, right_msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], msg)

#api.set_port_callback(inports[0]['name'], inports[1]['name'], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback([inports[0]['name'], inports[1]['name']], call_on_input)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
