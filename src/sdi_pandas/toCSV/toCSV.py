import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog

import pandas as pd

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
            df = pd.DataFrame({'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100,200,300,400,500]})
            attributes = {'format': 'csv','name':'DF_name'}
            default_msg = api.Message(attributes=attributes,body = df)
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
            tags = {'pandas': '','sdi_utils':''}
            operator_description = "To CSV from DataFrame"
            operator_description_long = "Creates a csv-formatted data passed to outport as message with the csv-string as body."
            add_readme = dict()
            add_readme["References"] = r"""[pandas doc: to_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html)"""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            write_index = False
            config_params['write_index'] = {'title': 'Write Index', 'description': 'Write index or ignore it', 'type': 'boolean'}
            separator = ';'
            config_params['separator'] = {'title': 'Separator', 'description': 'Separator', 'type': 'string'}
            reset_index = False
            config_params['reset_index'] = {'title': 'Reset Index', 'description': 'Reset index or indices', 'type': 'boolean'}


def process(msg) :
    att_dict = dict()
    att_dict['config'] = dict()

    att_dict['operator'] = 'anonymizeData'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

    # start custom process definition
    df = msg.body
    if api.config.reset_index :
        logger.debug('Reset Index')
        df = df.reset_index()

    data_str = df.to_csv(sep=api.config.separator, index=api.config.write_index)
    # end custom process definition

    log = log_stream.getvalue()
    return log, data_str


inports = [{'name': 'data', 'type': 'message.DataFrame',"description":"Input data"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'csv', 'type': 'string',"description":"Output data as csv"}]


def call_on_input(msg) :
    log, data_str = process(msg)
    api.send(outports[0]['name'], log)
    api.send(outports[1]['name'], data_str)

#api.set_port_callback(inports[0]['name'], call_on_input)

def main() :
    print('Test: Default config and input')
    api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: Changed config and inupt')
    config = api.config
    config.write_index = False
    config.reset_index = True

    df = pd.DataFrame({'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100, 200, 300, 400, 500]})
    df = df.set_index(keys=['icol'])
    attributes = {'format': 'csv', 'name': 'DF_name'}
    msg = api.Message(attributes=attributes,body=df)
    log, data_str = api.call(config,msg)

    print(data_str)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
