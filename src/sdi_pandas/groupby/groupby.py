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
                {'icol': [1, 1, 1, 1, 2], 'xcol 2': ['A', 'A', 'B', 'B', 'C'], 'xcol 3': [1, 1, 2, 2, 3],
                 'xcol4': ['a', 'a', 'b', 'a', 'b']})
            default_msg = api.Message(attributes = {'format': 'csv', 'name': 'DF_name'}, body=df)
            api.config.groupby = "'icol', 'xcol 2'"
            api.config.aggregation = "'xcol 3':'sum', 'xcol4':'count'"
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': ''}
            operator_description = "Group by"
            operator_description_long = "Groups the named columns by using the given aggregations."
            add_readme = dict()
            add_readme["References"] = "[pandas doc: grouby](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html)"

            groupby = 'None'
            config_params['groupby'] = {'title': 'Groupby Columns', 'description': 'List of comma separated columns to group', 'type': 'string'}
            aggregation = 'None'
            config_params['aggregation'] = {'title': 'Aggregation Mapping', 'description': 'List of comma separated mappings of columns with the type of aggregation, e.g. price:mean,city:count', 'type': "string"}
            index = False
            config_params['index'] = {'title': 'Set Index', 'description': 'Set Index', 'type': 'boolean'}
            drop_columns = 'None'
            config_params['drop_columns'] = {'title': 'Drop Columns', 'description': "List of columns of the joined DataFrame that could be dropped.", 'type': 'string'}


def process(msg) :

    logger, log_stream = slog.set_logging('DEBUG')

    # start custom process definition
    prev_att = msg.attributes
    df = msg.body

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation

    # groupby list
    cols = tfp.read_list(api.config.groupby)
    att_dict['config']['groupby'] = api.config.groupby

    # mapping
    colagg = tfp.read_dict(api.config.aggregation)
    att_dict['config']['aggregation'] = api.config.aggregation

    # groupby
    df = df.groupby(cols, as_index=api.config.index).agg(colagg)

    # drop col
    att_dict['config']['dropcols'] = api.config.drop_columns
    dropcols = tfp.read_list(api.config.drop_columns)
    if dropcols :
        df.drop(columns=dropcols,inplace=True)

    ##############################################
    #  final infos to attributes and info message
    ##############################################
    att_dict['operator'] = 'groupbyDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = list(df.columns)
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0, example_rows):
        att_dict['row_' + str(i)] = str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])
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

#api.set_port_callback(inports[0]['name'], call_on_input)

def main() :
    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
