import sdi_utils.gensolution as gs
from  sdi_utils import set_logging
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
                {'icol': [1, 2, 3, 4, 5], 'xcol2': ['A', 'A', 'B', 'B', 'C'], \
                 'xcol3': ['K', 'L', 'M', 'N', 'O'], 'xcol4': ['a1', 'a1', 'b1', 'b1', 'b1']})
            default_msg  = api.Message(attributes = {'format': 'pandas', 'name': 'test'}, body=df)
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': ''}
            operator_description = "sample"
            sample_size = 0.1
            config_params['sample_size'] = {'title': 'Sample size', 'description': 'Sample size', 'type': 'number'}
            random_state = 1
            config_params['random_state'] = {'title': 'Random state', 'description': 'Random state', 'type': 'integer'}
            invariant_column = ''
            config_params['invariant_column'] = {'title': 'Invariant column', 'description': 'Column where all the same value records should be kept as a whole in a sample', 'type': 'string'}


def process(msg) :

    logger, log_stream = set_logging('DEBUG')

    # start custom process definition
    # test if body refers to a DataFrame type
    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df, pd.DataFrame):
        logger.error('Message body does not contain a pandas DataFrame')
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start  calculation

    sample_size = api.config.sample_size
    if sample_size < 1 :
        sample_size = int(sample_size * df.shape[0])
        if sample_size < 1 :
            sample_size = 1
            logger.warning("Fraction of sample size too small. Set sample size to 1.")
    elif sample_size > df.shape[0]:
        logger.warning("Sample size larger than number of rows")

    logger.debug("Samples_size: {}/() ({})".format(sample_size,df.shape[0],sample_size/df.shape[0]))
    random_state = api.config.random_state

    invariant_column = tfp.read_value(api.config.invariant_column)
    if invariant_column and sample_size < df.shape[0]:
        # get the average number of records for each value of invariant
        sc_df = df.groupby(invariant_column)[invariant_column].count()
        sample_size_invariant = int(sample_size / sc_df.mean())
        sample_size_invariant = 1 if sample_size_invariant == 0 else sample_size_invariant  # ensure minimum
        sc_df = sc_df.sample(n=sample_size_invariant, random_state=random_state).to_frame()
        sc_df.rename(columns={invariant_column: 'sum'}, inplace=True)
        # sample the df by merge 2 df
        df = pd.merge(df, sc_df, how='inner', right_index=True, left_on=invariant_column)
        df.drop(columns=['sum'], inplace=True)
    else:
        df = df.sample(n=sample_size, random_state=random_state)

    ###### end  calculation

    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty:
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'selectDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0, example_rows):
        att_dict['row_' + str(i)] = str([str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    # end custom process definition

    log = log_stream.getvalue()
    msg = api.Message(attributes=att_dict,body=df)
    return log, msg


inports = [{'name': 'input', 'type': 'message.DataFrame'}]
outports = [{'name': 'log', 'type': 'string'}, {'name': 'output', 'type': 'message.DataFrame'}]

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
        
