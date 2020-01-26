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
                {'icol': [1, 2, 3, 4, 5], 'xcol2': ['A', 'A', 'B', 'B', 'C'], \
                 'xcol3': ['K', 'L', 'M', 'N', 'O'], 'xcol4': ['a1', 'a1', 'b1', 'b1', 'b1']})
            default_msg  = api.Message(attributes = {'format': 'pandas', 'name': 'test'}, body=df)
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': '','sdi_utils':''}
            operator_description = "Sample from Dataframe"
            operator_description_long = "Sampling over a DataFrame but keeps datasets with the same value of the \
            defined column as set and not splitting them, e.g. sampling with the invariant_column='date' samples \
            but ensures that all datasets of a certain date are taken or none. This leads to the fact that the \
            sample_size is only a guiding target. Depending on the size of the datasets with the same value of \
            the *invariant_column* compared to the *sample_size* this could deviate a lot. "
            add_readme = dict()
            add_readme["References"] = "[pandas doc: sample](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sample.html)"

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            sample_size = 0.1
            config_params['sample_size'] = {'title': 'Sample size', 'description': 'Sample size', 'type': 'number'}
            random_state = 1
            config_params['random_state'] = {'title': 'Random state', 'description': 'Random state', 'type': 'integer'}
            invariant_column = ''
            config_params['invariant_column'] = {'title': 'Invariant column', 'description': 'Column where all the same value records should be kept as a whole in a sample', 'type': 'string'}


def process(msg) :
    att_dict = dict()
    att_dict['config'] = dict()

    att_dict['operator'] = 'sample'
    logger, log_stream = slog.set_logging(att_dict['operator'])
    if api.config.debug_mode == True:
        logger.setLevel('DEBUG')

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
        
