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
            tags = {'pandas': ''}
            operator_description = "castColumns"
            operator_description_long = "Casting the types of columns according to the mapping given."
            cast = 'None'
            config_params['cast'] = {'title': 'Cast Mapping List', 'description': "List of cast mappings. Example: price:float32, rank:uint8, 'type': string"}
            round = False
            config_params['round'] = {'title': 'Round when cast', 'description': 'when true the values are rounded before the type is casted', 'type': 'boolean'}


def process(msg) :

    logger, log_stream = slog.set_logging('DEBUG')

    # start custom process definition
    prev_att = msg.attributes
    df = msg.body

    att_dict = dict()
    att_dict['config'] = dict()

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
    att_dict['operator'] = 'castDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]
    if 'id' in prev_att.keys():
        att_dict['id'] = prev_att['id'] + '; ' + att_dict['operator'] + ': ' + str(id(df))
    else:
        att_dict['id'] = att_dict['operator'] + ': ' + str(id(df))

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0, example_rows):
        att_dict['row_' + str(i)] = str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    msg = api.Message(attributes=att_dict, body = df)
    # end custom process definition
    log = log_stream.getvalue()
    return log, msg


inports = [{'name': 'inDataFrameMsg', 'type': 'message.DataFrame'}]
outports = [{'name': 'Info', 'type': 'string'}, {'name': 'outDataFrameMsg', 'type': 'message.DataFrame'}]

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
        
