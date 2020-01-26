import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp

import pandas as pd

EXAMPLE_ROWS =5

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
            attributes = {'format': 'csv', 'name': 'DF_name'}
            default_msg = api.Message(attributes=attributes,body = df)
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': '','sdi_utils':''}
            operator_description = "Cast Column Data Types"
            operator_description_long = "Casting the types of columns according to the mapping given."
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            cast = 'None'
            config_params['cast'] = {'title': 'Cast Mapping List', 'description': "List of cast mappings. Example: price:float32, rank:uint8, 'type': string"}
            round = False
            config_params['round'] = {'title': 'Round when cast', 'description': 'when true the values are rounded before the type is casted', 'type': 'boolean'}


def process(msg) :
    att_dict = dict()
    att_dict['config'] = dict()

    att_dict['operator'] = 'castColumns'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

    logger.debug("Process started")

    # start custom process definition
    prev_att = msg.attributes
    df = msg.body

    castmap = tfp.read_dict(api.config.cast)

    if castmap:
        for col, casttype in castmap.items():
            if api.config.round:
                df[col] = df[col].round()
            df[col] = df[col].astype(casttype)

    ###### end calculation

    ##############################################
    #  final infos to attributes and info message
    ##############################################
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

    msg = api.Message(attributes=att_dict, body = df)
    # end custom process definition
    log = log_stream.getvalue()
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

    print('Test: Changed config and inupt')
    config = api.config
    config.cast = "col3:float32, 'col 2': int8"
    config.reset_index = True

    df = pd.DataFrame({'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100, 200, 300, 400, 500]})
    df = df.set_index(keys=['icol'])

    attributes = {'format': 'csv', 'name': 'DF_name'}
    msg = api.Message(attributes=attributes, body=df)

    print("=BEFORE=")
    print(msg.body.dtypes)
    log, msg = api.call(config, msg)
    print("=BEFORE=")
    print(msg.body.dtypes)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
