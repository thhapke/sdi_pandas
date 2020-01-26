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
            df = pd.DataFrame(
                {'icol': [1, 1, 3, 3, 3], 'col2': [1, 2, None, 4, 5], 'col3': [2, 3, 4, 5, 6],
                 'col4': [5, 6.5, 7.5, 8, 9], 'col5': [6, 6.7, 8.2, None, 10.1]})
            default_msg = api.Message(attributes = {'format': 'csv', 'name': 'DF_name'},body=df)
            api.config.map_values = "icol:1:2; col2:5:55"
            api.config.fill_nan_values = "col2:0,col5:0"
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': '','sdi_utils':''}
            operator_description = "Set Value"
            operator_description_long ="Replacing values or NaN for the whole DataFrame."
            add_readme = dict()
            add_readme["References"] = r"""
[pandas doc: replace](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.replace.html)
[pandas doc: fillna](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.fillna.html)"""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            map_values = 'None'
            config_params['map_values'] = {'title': 'Mapping Values', 'description': 'Mapping Values', 'type': 'string'}
            fill_nan_values = 'None'
            config_params['fill_nan_values'] = {'title': 'Fill NaN for Values', 'description': 'Fill NaN for Values', 'type': 'string'}


def process(msg) :

    att_dict = dict()
    att_dict['config'] = dict()

    att_dict['operator'] = 'setValue'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

    # start custom process definition
    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')


    ###### start of doing calculation

    # map_values : column1: {from_value: to_value}, column2: {from_value: to_value}
    att_dict['config']['set_value'] = api.config.map_values
    maps_map = tfp.read_dict_of_dict(api.config.map_values)
    df.replace(maps_map,inplace=True)

    # Fill NaN value : column1: value, column2: value,
    att_dict['config']['fill_nan_values'] = api.config.fill_nan_values
    map_dict = tfp.read_dict(api.config.fill_nan_values)
    if map_dict :
        df.fillna(map_dict,inplace=True)

    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        raise ValueError('DataFrame is empty')

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
        
