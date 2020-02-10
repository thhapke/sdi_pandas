import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import pandas as pd
import io
import re

EXAMPLE_ROWS = 5

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if isinstance(msg, api.Message):
                print('Port: ', port)
                print('Attributes: ', msg.attributes)
                print('Body: ', str(msg.body))
            else:
                print(str(msg))
            return msg

        def call(config, msg):
            api.config = config
            return process(msg)

        def set_port_callback(port, callback):
            csv = b"""col1;col2;col3
                                     1;4.4;99
                                     2;4.5;200
                                     3;4.7;65
                                     4;3.2;140
                                     """
            attributes = {'format': 'csv', "storage.filename": 'filename', 'storage.endOfSequence': True, \
                          'storage.fileIndex': 0, 'storage.fileCount': 1}
            default_msg = api.Message(attributes=attributes, body=csv)
            callback(default_msg)

        class config:
            ## Meta data
            config_params = dict()
            version = "0.0.17"
            tags = {'pandas': '','sdi_utils':''}
            operator_description = "From CSV to DataFrame"
            operator_description_long = "Creating a DataFrame with csv-data passed through inport."
            add_readme = dict()
            add_readme[
                "References"] = "[pandas doc: read_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html)"

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode', 'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            collect = True
            config_params['collect'] = {'title': 'Collect data', 'description': 'Collect data before sending it to the output port',
                                           'type': 'boolean'}
            index_cols = 'None'
            config_params['index_cols'] = {'title': 'Index Columns', 'description': 'Index columns of dataframe',
                                           'type': 'string'}
            separator = ';'
            config_params['separator'] = {'title': 'Separator of CSV', 'description': 'Separator of CSV',
                                          'type': 'string'}
            use_columns = 'None'
            config_params['use_columns'] = {'title': 'Use columns from CSV',
                                            'description': 'Use columns from CSV (list)', 'type': 'string'}
            limit_rows = 0
            config_params['limit_rows'] = {'title': 'Limit number of rows',
                                           'description': 'Limit number of rows for testing purpose', 'type': 'number'}
            downcast_int = False
            config_params['downcast_int'] = {'title': 'Downcast integers',
                                             'description': 'Downcast integers from int64 to int with smallest memory footprint',
                                             'type': 'boolean'}
            downcast_float = False
            config_params['downcast_float'] = {'title': 'Downcast float datatypes',
                                               'description': 'Downcast float64 to float32 datatypes',
                                               'type': 'boolean'}
            df_name = 'DataFrame'
            config_params['df_name'] = {'title': 'DataFrame name',
                                        'description': 'DataFrame name for debugging reasons', 'type': 'string'}

            decimal = '.'
            config_params['decimal'] = {'title': 'Decimals separator', 'description': 'Decimals separator',
                                        'type': 'string'}
            dtypes = 'None'
            config_params['dtypes'] = {'title': 'Data Types of Columns',
                                       'description': 'Data Types of Columns (list of maps)', 'type': 'string'}
            data_from_filename = 'None'
            config_params['data_from_filename'] = {'title': 'Data from Filename', 'description': 'Data from Filename',
                                                   'type': 'string'}
            todatetime = 'None'
            config_params['todatetime'] = {'title': 'To Datetime', 'description': 'To Datetime', 'type': 'string'}
            utc = True
            config_params['utc'] = {'title': 'Use UTC', 'description': 'If true utc is used for time conversion', 'type': 'boolean'}

            keyword_args = "'error_bad_lines'= True, 'low_memory' = False, compression = None, thousands = None "
            config_params['keyword_args'] = {'title': 'Keyword Arguments',
                                             'description': 'Mapping of key-values passed as arguments \"to read_csv\"',
                                             'type': 'string'}


def downcast(df, data_type, to_type):
    cols = list(df.select_dtypes(include=[data_type]).columns)
    if len(cols) == 0:
        return df, None

    downcast_dict = dict()
    downcast_dict['data_type'] = data_type
    cdtypes = df[cols].dtypes.to_dict()
    downcast_dict['previous_subtypes'] = {col: str(itype) for col, itype in cdtypes.items()}
    downcast_dict['previous_mem_usage'] = df[cols].memory_usage(deep=True).sum() / 1024 ** 2

    df[cols] = df[cols].apply(pd.to_numeric, downcast=to_type)

    int_dtypes2 = df[cols].dtypes.to_dict()
    downcast_dict['subtypes'] = {col: str(itype) for col, itype in int_dtypes2.items()}
    downcast_dict['mem_usage'] = df[cols].memory_usage(deep=True).sum() / 1024 ** 2

    return df, downcast_dict


def process(msg):

    att_dict = msg.attributes

    global result_df

    att_dict['operator'] = 'fromCSV'
    if api.config.debug_mode == True:
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='DEBUG')
    else :
        logger, log_stream = slog.set_logging(att_dict['operator'],loglevel='INFO')
    logger.info("Process started")
    time_monitor = tp.progress()

    logger.info('Filename: {} index: {}  count: {}  endofSeq: {}'.format(msg.attributes["storage.filename"], \
                                                                         msg.attributes["storage.fileIndex"], \
                                                                         msg.attributes["storage.fileCount"], \
                                                                         msg.attributes["storage.endOfSequence"]))

    if msg.body == None:
        logger.info('Process ended.')
        msg = api.Message(attributes=att_dict, body=result_df)
        log = log_stream.getvalue()
        return log, msg
    elif isinstance(msg.body, str):
        csv_io = io.StringIO(msg.body)
        logger.debug("Input format: <string>")
    elif isinstance(msg.body, bytes):
        csv_io = io.BytesIO(msg.body)
        logger.debug("Input format: <bytes>")
    elif isinstance(msg.body, io.BytesIO):
        logger.debug("Input format: <io.Bytes>")
        csv_io = msg.body
    else:
        raise TypeError('Message body has unsupported type' + str(type(msg.body)))

    # nrows
    nrows = None
    if not api.config.limit_rows == 0:
        nrows = api.config.limit_rows

    # usecols
    use_cols = tfp.read_list(api.config.use_columns)
    logger.debug('Columns used: {}'.format(use_cols))

    # dtypes mapping
    typemap = tfp.read_dict(api.config.dtypes)
    logger.debug('Type cast: {}'.format(str(typemap)))

    kwargs = tfp.read_dict(text=api.config.keyword_args, map_sep='=')

    ##### Read string from buffer
    logger.debug("Read from input")
    df = pd.read_csv(csv_io, api.config.separator, usecols=use_cols, dtype=typemap, decimal=api.config.decimal, \
                     nrows=nrows, **kwargs)

    # Data from filename
    if api.config.data_from_filename and not api.config.data_from_filename == 'None':
        col = api.config.data_from_filename.split(':')[0].strip().strip("'").strip('"')
        pat = api.config.data_from_filename.split(':')[1].strip().strip("'").strip('"')
        logger.debug('Filename: {}  pattern: {}'.format(att_dict['filename'], pat))
        try:
            dataff = re.match('.*(\d{4}-\d+-\d+).*', att_dict['filename'])
            df[col] = dataff.group(1)
        except AttributeError:
            raise ValueError('Pattern not found - Filename: {}  pattern: {}'.format(att_dict['filename'], pat))

    # To Datetime
    if api.config.todatetime and not api.config.todatetime == 'None':
        dt_fmt = tfp.read_dict(api.config.todatetime)
        logger.debug('Time conversion {} by using UTC {}'.format(api.config.todatetime,api.config.utc))
        for col, fmt in dt_fmt.items() :
            df[col] = pd.to_datetime(df[col], format=fmt, utc= api.config.utc)

    ###### Downcasting
    # save memory footprint for calculating the savings of the downcast
    logger.debug('Memory used before downcast: {}'.format(df.memory_usage(deep=True).sum() / 1024 ** 2))
    if api.config.downcast_int:
        df, dci = downcast(df, 'int', 'unsigned')
    if api.config.downcast_float:
        df, dcf = downcast(df, 'float', 'float')

    # check if index is provided and set
    index_list = tfp.read_list(api.config.index_cols)
    if index_list:
        df.set_index(index_list, inplace=True)

    if api.config.collect :
        # stores the result in global variable result_df
        if msg.attributes['storage.fileIndex'] == 0:
            logger.debug('Added to DataFrame: {}'.format(att_dict['storage.filename']))
            result_df = df
        else:
            try :
                result_df = pd.concat([result_df, df], axis=0, sort=False)
            except Exception  as e:
                logger.error(str(e))
                result_df = df
    else :
        result_df = df

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
    logger.debug('Process ended: {}  - {}  '.format(progress_str,time_monitor.elapsed_time()))

    return log_stream.getvalue(), api.Message(attributes=att_dict,body=df)


inports = [{'name': 'csv', 'type': 'message',"description":"Input byte or string csv"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame',"description":"Output data"}]


def call_on_input(msg):
    commit_token = "0"

    # if  collect==True each file is send to process
    if msg.attributes["storage.endOfSequence"] or api.config.collect == False:
        commit_token = "1"

    log, msg = process(msg)
    msg.attributes['commit.token'] = commit_token

    # body == None then no msg is send
    if commit_token == "1" and isinstance(msg.body, pd.DataFrame) :
        api.send(outports[1]['name'], msg)

    api.send(outports[0]['name'], log)


#api.set_port_callback(inports[0]['name'], call_on_input)

def main():
    import os

    print('Test: Default')
    api.set_port_callback(inports[0]['name'], call_on_input)

    print('Test: config and iput')
    in_dir = '/Users/Shared/data/OptRanking/portal1_samples25'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('.*csv', f)]

    try :
        logfile = open('/tmp/mylog/fromCSV.log', 'w')
    except FileNotFoundError :
        print('logfile: {}'.format(os.getcwd()))
        logfile = open('fromCSV.log', 'w')

    for i, fname in enumerate(files_in_dir):

        #if i == 5 :
        #    break

        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir)}
        csv = open(os.path.join(in_dir, fname), mode='rb').read()
        msg = api.Message(attributes=attributes, body=csv)

        ########## Operation fromCSV
        config = api.config
        config.debug_mode = True
        config.use_columns = "'Exportdatum','Postleitzahl','Ort','Ortsteil','Verbrauchsstufe','Rang','Gesamtpreis','Anbietername'"
        config.downcast_float = True
        config.downcast_int = True
        config.dtypes = "'Gesamtpreis':'float32','Postleitzahl':'uint32','Verbrauchsstufe':'uint16'"
        config.separator = ';'
        config.index_cols = "None"
        config.limit_rows = 0
        config.df_name = 'DataFrame'
        config.decimal = '.'
        config.utc = False
        config.collect = False
        config.todatetime = 'Exportdatum : %Y-%m-%d'
        config.keyword_args = "'error_bad_lines'= True, 'low_memory' = False, compression = None, comment = '#'"
        log, msg = api.call(config, msg)

        logfile.write(log)

    msg = api.Message(attributes=attributes, body=None)

    logfile.close()


if __name__ == '__main__':
    main()
    # gs.gensolution(os.path.realpath(__file__), config, inports, outports)
