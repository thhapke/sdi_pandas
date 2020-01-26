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
            df = pd.DataFrame({'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100, 200, 300, 400, 500]})
            default_msg = api.Message(attributes = {'format': 'csv', 'name': 'DF_name'},body=df)
            api.config.selection_num = "icol >2"
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': '','sdi_utils':''}
            operator_description = "Select Values"
            operator_description_long = "Selecting data records based on column data restrictions (= SELECT * FROM ... WHERE COLX = x AND ...) of numeric types and lists of data. "
            add_readme = dict()
            add_readme["References"] = "[pandas doc: sample](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sample.html)"

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            selection_num = 'None'
            config_params['selection_num'] = {'title': 'Selection in columns of numeric type', 'type': 'string'}
            config_params['selection_num']['description'] = "Selection criteria for numerical columns. Comparison operators: ['=', '>', '<', '!' or '!=' ]. Example: order_id < 100000"
            selection_list = 'None'
            config_params['selection_list'] = {'title': 'Selection list', 'type': 'string'}
            config_params['selection_list']['description'] = "Inclusion or exclusion list of values for numerical and string column.  Comparison operators: ['=', '!' or '!=' ]. Example: trans_date = 2016-03-03, 2016-02-04"


def process(msg) :
    att_dict = dict()
    att_dict['config'] = dict()

    att_dict['operator'] = 'selectValues'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

    # start custom process definition
    prev_att = msg.attributes
    df = msg.body

    ######################### Start Calculation

    # save and reset indices
    index_names = df.index.names
    if index_names[0]:
        logger.debug("Reset index")
        df.reset_index(inplace=True)

    # prepare selection for numbers
    if api.config.selection_num and not api.config.selection_num.upper() == 'NONE':

        selection_map = tfp.read_relations(api.config.selection_num)

        for s in selection_map:
            if s[1] == '≤':
                df = df.loc[df[s[0]] <= s[2]]
            elif s[1] == '<':
                df = df.loc[df[s[0]] < s[2]]
            elif s[1] == '≥':
                df = df.loc[df[s[0]] >= s[2]]
            elif s[1] == '>':
                df = df.loc[df[s[0]] > s[2]]
            elif s[1] == '=':
                df = df.loc[df[s[0]] == s[2]]
            elif s[1] == '!':
                df = df.loc[df[s[0]] != s[2]]
            else:
                raise ValueError('Unknown relation: ' + str(s))
    att_dict['config']['selection_num'] = api.config.selection_num

    if api.config.selection_list and not api.config.selection_list.upper() == 'NONE':
        value_list_dict = tfp.read_dict_of_list(api.config.selection_list)
        for key, vl in value_list_dict.items():
            df = df.loc[df[key].isin(vl)]
    att_dict['config']['selection_list'] = api.config.selection_list

    # set  index again
    if index_names[0]:
        att_dict['indices'] = index_names
        logger.debug('Set indices to: {}'.format(str(index_names)))
        df.set_index(keys=index_names, inplace=True)

    if df.empty:
        logger.error('DataFrame is empty')
        raise ValueError('DataFrame is empty')
    ######################### End Calculation

    ##############################################
    #  final infos to attributes and info message
    ##############################################
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['shape'] = df.shape
    att_dict['id'] = str(id(df))

    logger.debug('Columns: {}'.format(str(df.columns)))
    logger.debug('Shape (#rows - #columns): {} - {}'.format(df.shape[0],df.shape[1]))
    logger.debug('Memory: {} kB'.format(att_dict['memory']))
    example_rows = EXAMPLE_ROWS if df.shape[0] > EXAMPLE_ROWS else df.shape[0]
    for i in range(0, example_rows):
        att_dict['row_' + str(i)] = str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])
        logger.debug('Head data: {}'.format(att_dict['row_' + str(i)]))

    # end custom process definition

    log = log_stream.getvalue()
    msg = api.Message(attributes=att_dict, body=df)
    return log, msg


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
        
