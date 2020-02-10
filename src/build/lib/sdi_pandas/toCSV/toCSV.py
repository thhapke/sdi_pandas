import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

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



            keyword_args = "None"
            config_params['keyword_args'] = {'title': 'Keyword Arguments',
                                             'description': 'Mapping of key-values passed as arguments \"to read_csv\"',
                                             'type': 'string'}

def process(msg) :
    att_dict = msg.attributes
    att_dict['operator'] = 'toCSV'
    if api.config.debug_mode == True:
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='DEBUG')
    else :
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='INFO')
    logger.info("Process started")
    time_monitor = tp.progress()

    # start custom process definition
    df = msg.body
    if api.config.reset_index :
        logger.debug('Reset Index')
        df = df.reset_index()

    kwargs = tfp.read_dict(text=api.config.keyword_args, map_sep='=')

    if not kwargs == None :
        data_str = df.to_csv(sep=api.config.separator, index=api.config.write_index, **kwargs)
    else :
        data_str = df.to_csv(sep=api.config.separator, index=api.config.write_index)
    # end custom process definition
    logger.info('End of Process: {}'.format(time_monitor.elapsed_time()))

    # create dict of columns and types for HANA
    map_hana = {'int8': 'TINYINT', 'int16': 'SMALLINT', 'int32': 'INTEGER', 'int64': 'BIGINT', 'float32': 'FLOAT',
                'float64': 'DOUBLE', \
                'object': 'VARCHAR', 'datetime64': 'TIMESTAMP'}
    col_dict = { c : str(df[c].dtype) for c in df.columns}
    hana_table_dict = list()
    for c,ty in col_dict.items() :
        if ty == 'object' :
            size = df[c].str.len().max()
            hana_table_dict.append({'name':c,'type':map_hana[col_dict[c]],'size':size})
        elif 'datetime64' in ty :
            hana_table_dict.append({'name': c, 'type': 'TIMESTAMP'})
        else :
            hana_table_dict.append({'name': c, 'type': map_hana[col_dict[c]]})
    logger.info('For Hana table definition: {}'.format(hana_table_dict))

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

    df = pd.DataFrame({'icol': [1, 2, 3, 4, 5], 'col 2': ['2020-01-01', '2020-02-01', '2020-01-31', '2020-01-28','2020-04-12'],\
                       'col3': [100.0, 200.2, 300.4, 400, 500],'names':['Anna','Berta','Berta','Claire','Dora']})
    df = df.set_index(keys=['icol'])
    df['col 2'] = pd.to_datetime(df['col 2'],format='%Y-%m-%d',utc=True)

    attributes = {'format': 'csv', 'name': 'DF_name'}
    msg = api.Message(attributes=attributes,body=df)
    log, data_str = api.call(config,msg)

    print(data_str)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
