import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog

import sdi_utils.textfield_parser as tfp

import pandas as pd
import numpy as np

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
            df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100, 200, 300, 400, 500]})
            attributes = {'format': 'csv', 'name': 'DF_name'}
            default_msg = api.Message(attributes=attributes,body = df)
            callback(default_msg)
    
        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.17'
            tags = {'pandas': ''}
            operator_description = "cleanseHeuristics"

            value_to_nan = 'None'
            config_params['const_to_NaN'] = {'title': 'Set value to Null/Nan', 'description': 'Sets all data of categorical columns with value to nan', 'type': 'string'}
            yes_no_to_num = False
            config_params['yes_no_to_num'] = {'title': 'Yes/No to Numeric', 'description': 'Yes/No to Numeric 1/0', 'type': 'boolean'}
            drop_nan_columns = False
            config_params['drop_nan_columns'] = {'title': 'Drops columns when all values are NaN', 'description': 'Drop NaN Columns', 'type': 'boolean'}
            all_constant_to_NaN = False
            config_params['all_constant_to_NaN'] = {'title': 'Columns with unique Value to NaN', 'description': 'Columns with unique Value to NaN', 'type': 'boolean'}
            threshold_unique = 0
            config_params['threshold_unique'] = {'title': 'Threshold of unique values', 'description': 'Threshold of unique values set to 1 (-> value exist) ', 'type': 'number'}
            threshold_unique_cols = 'None'
            config_params['threshold_unique_cols'] = {'title': 'Columns for unique threshold criteria', 'description': 'Columns for unique threshold criteria', 'type': 'string'}
            sparse = 0
            config_params['sparse'] = {'title': 'Sparse ', 'description': 'Absolute or relative number criteria of sparsenss. All values of column are set to nan', 'type': 'number'}
            sparse_cols = 'None'
            config_params['sparse_cols'] = {'title': 'Columns for check on sparse', 'description': 'Columns for check on sparse', 'type': 'string'}
            drop_nan_rows_cols = 'None'
            config_params['drop_nan_rows_cols'] = {'title': 'Drop NaN rows columns', 'description': 'Columns for dropping NaN rows ', 'type': 'string'}
            rare_value_quantile = 0
            config_params['rare_value_quantile'] = {'title': 'Rare Value Quantile', 'description': 'Rare Value Quantile', 'type': 'number'}
            rare_value_cols = 'None'
            config_params['rare_value_cols'] = {'title': 'Columns for Rare Value Criteria', 'description': 'Columns for Rare Value Criteria', 'type': 'string'}
            rare_value_std = None
            config_params['rare_value_std'] = {'title': 'Rare Value Standard Deviation', 'description': 'Rare Value Standard Deviation', 'type': 'number'}
            max_cat_num = 0
            config_params['max_cat_num'] = {'title': 'Maximum Number of Categories', 'description': 'Maximum Number of Categories', 'type': 'number'}
            max_cat_num_cols = 'None'
            config_params['max_cat_num_cols'] = {'title': 'Columns for Maximum Number Categories Criteria', 'description': 'Columns for Maximum Number Categories Criteria', 'type': 'string'}
            reduce_categoricals_only = False
            config_params['reduce_categoricals_only'] = {'title': 'Reduce Categorical Type Columns only', 'description': 'Reduce Categorical Type Columns only', 'type': 'boolean'}
            remove_duplicates_cols = 'None'
            config_params['remove_duplicates_cols'] = {'title': 'Columns for Remove Duplicate Criteria', 'description': 'Columns for Remove Duplicate Criteria', 'type': 'string'}
            fill_categoricals_nan = 'None'
            config_params['fill_categoricals_nan'] = {'title': 'Value to replace NaN', 'description': 'Value that replaces NaN for categorical columns', 'type': 'string'}
            fill_numeric_nan_zero = False
            config_params['fill_numeric_nan_zero'] = {'title': 'Replaces numeric type columns nan with 0',
                                                      'description': 'Replaces numeric type columns nan with 0',
                                                      'type': 'boolean'}

            #cut_obj_size = 0
            #config_params['cut_obj_size'] = {'title': 'Cut object siez', 'description': 'Truncate lengthy strings to this size', 'type': 'number'}


def process(msg) :

    logger, log_stream = slog.set_logging('DEBUG')

    # start custom process definition
    prev_att = msg.attributes
    df = msg.body
    if not isinstance(df, pd.DataFrame):
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation

    att_dict['prev_number_columns'] = df.shape[1]
    att_dict['prev_number_rows'] = df.shape[0]

    #
    att_dict['config']['remove_duplicates_cols'] = api.config.remove_duplicates_cols
    remove_duplicates_cols = tfp.read_list(api.config.remove_duplicates_cols)
    if remove_duplicates_cols:
        df = df.groupby(remove_duplicates_cols).first().reset_index()
        logger.debug('#Dropped duplicates: {} - {} = {}'.format(att_dict['prev_number_rows'], df.shape[0], \
                                                                 att_dict['prev_number_rows'] - df.shape[0]))

    att_dict['config']['value_to_nan'] = api.config.value_to_nan
    value_to_nan = tfp.read_value(api.config.value_to_nan)
    if value_to_nan:
        df.select_dtypes(include='object').replace(value_to_nan, value_to_nan.nan, inplace=True)

    att_dict['config']['yes_no_to_boolean'] = str(api.config.yes_no_to_num)
    if api.config.yes_no_to_num:
        prev_categoricals = len(df.select_dtypes(include=np.object).columns)
        for col in df.select_dtypes(include=np.object):
            df[col] = df[col].str.upper()
            vals = [x for x in df.loc[df[col].notnull(), col].unique()]
            if len(vals) == 1 and vals[0] in ['YES', 'Y']:
                df.loc[df[col].notnull(), col] = 1
                df.loc[df[col].isnull(), col] = 0
                try:
                    df[col] = df[col].astype('int8')
                except ValueError:
                    print('Value Error: {}'.format(col))
                    print(df[col].unique())
            if len(vals) == 1 and vals[0] in ['NO', 'N']:
                df.loc[df[col].notnull(), col] = 1
                df.loc[df[col].isnull(), col] = 0
                df[col] = df[col].astype('int8')
            if len(vals) == 2 and (all(i in vals for i in ['YES', 'NO']) or all(i in vals for i in ['Y', 'N'])):
                df[col].replace(to_replace={'NO': 0, 'N': 0, 'no': 0, 'n': 0, 'YES': 1, 'Y': 1, 'yes': 1, 'y': 1})
                df[col] = df[col].astype('int8')
        after_categoricals = len(df.select_dtypes(include=np.object).columns)
        logger.debug('<yes_no_to_boolean> impact: {} -> {}'.format(prev_categoricals, after_categoricals))

    att_dict['config']['all_constant_to_NaN'] = str(api.config.all_constant_to_NaN)
    if api.config.all_constant_to_NaN:
        num_constant_cols = 0
        for col in df.columns:
            unique_vals = df[col].unique()
            if len(unique_vals) == 1:
                df[col] = np.nan
                num_constant_cols = num_constant_cols + 1
        logger.debug('<all_constant_to_NaN> number of columns: {}'.format(num_constant_cols))

    # remove rare value rows with quantile
    att_dict['config']['rare_value_cols'] = api.config.rare_value_cols
    att_dict['config']['rare_value_quantile'] = api.config.rare_value_quantile
    att_dict['config']['rare_value_std'] = api.config.rare_value_std
    rare_value_cols = tfp.read_list(api.config.rare_value_cols, list(df.columns))
    if rare_value_cols:
        logger.debug('quantile')
        # drop rare values by quantile
        if api.config.rare_value_quantile > 0:
            if not api.config.rare_value_quantile >= 0 and api.config.rare_value_quantile < 1:
                raise ValueError('Quantile value range: [0,1[, not {}'.format(api.config.rare_value_quantile))
            num_reduce_categoricals_col = 0
            for col in rare_value_cols:
                unique_num = len(df[col].unique())
                val_num = df[col].count()
                ratio = df[col].count() / len(df[col].unique())
                threshold = df[col].count() / len(df[col].unique()) * api.config.rare_value_quantile
                value_counts = df[col].value_counts()  # Specific column
                # kept_values = value_counts[value_counts > threshold].count()
                if value_counts[value_counts > threshold].count() > 1:
                    to_remove = value_counts[value_counts <= threshold].index
                    if len(to_remove) > 0:
                        logging.debug(
                            'Drop rare value by quantile: Column {}: {}/{} '.format(col, len(to_remove), unique_num))
                        df[col].replace(to_remove, np.nan, inplace=True)
                        num_reduce_categoricals_col += 1
            logger.debug('<rare_value_quantile> impact on columns: {}/{}'.format(num_reduce_categoricals_col,
                                                                                  len(rare_value_cols)))

        # drop rare values by std
        if api.config.rare_value_std > 0:
            num_reduce_categoricals_col = 0
            for col in df.columns:
                unique_num = len(df[col].unique())
                value_counts = df[col].value_counts()
                mean = value_counts.mean()
                threshold = value_counts.mean() - value_counts.std() * api.config.rare_value_std
                if threshold > 1:
                    to_remove = value_counts[value_counts <= threshold].index
                    if len(to_remove) > 0:
                        logger.debug(
                            'Drop rare value by std: Column {}: {}/{} '.format(col, len(to_remove), unique_num))
                        df[col].replace(to_remove, np.nan, inplace=True)
                        num_reduce_categoricals_col += 1
            logger.debug(
                '<rare_value_std> impact on columns: {}/{}'.format(num_reduce_categoricals_col, len(rare_value_cols)))

    # for unique values less then threshold_unique set to 1. All NaN set to 0
    att_dict['config']['threshold_unique_cols'] = api.config.threshold_unique_cols
    att_dict['config']['threshold_unique'] = api.config.threshold_unique
    threshold_unique_cols = tfp.read_list(api.config.threshold_unique_cols, list(df.columns))
    if threshold_unique_cols:
        prev_obj_cols = len(df.select_dtypes("object"))
        for col in threshold_unique_cols:
            if df[col].dtype == np.object:
                unique_vals = list(df[col].unique())
                if len(unique_vals) <= api.config.threshold_unique:
                    # test if one of the values is nan
                    if np.nan in unique_vals:
                        df.loc[df[col].notnull(), col] = 1
                        df.loc[df[col].isnull(), col] = 0
                        df[col] = df[col].astype('int8')
        after_obj_cols = len(df.select_dtypes("object"))
        logger.debug(
            'Threshold unique effect on number of categorical columns: {} -> {}'.format(prev_obj_cols, after_obj_cols))

    # for count values less then threshold_count set to NaN
    att_dict['config']['sparse_cols'] = api.config.sparse_cols
    att_dict['config']['sparse'] = api.config.sparse
    sparse_cols = tfp.read_list(api.config.sparse_cols)
    if sparse_cols:
        logger.debug('Sparse check')
        if api.config.reduce_categoricals_only:
            test_cols = [ot for ot in sparse_cols if df[ot].dtype == np.object]
        if api.config.sparse < 1:
            api.config.sparse = api.config.sparse * df.shape[0]
        for col in sparse_cols:
            if df[col].count() < api.config.sparse_freq:
                logger.debug('Threshold_count: Removed column {} (#values {})'.format(col, df[col].count()))
                df[col] = np.nan

    # removes columns with to many category values that could not be transposed
    att_dict['config']['max_cat_num'] = api.config.max_cat_num
    att_dict['config']['max_cat_num_cols'] = api.config.max_cat_num_cols
    max_cat_num_cols = tfp.read_list(api.config.max_cat_num_cols)
    if api.config.max_cat_num > 0 and max_cat_num_cols:
        drop_cols = list()
        for col in max_cat_num_cols:
            if df[col].dtype == np.object:
                if len(df[col].unique()) > api.config.max_cat_num:
                    drop_cols.append(col)
        df.drop(columns=drop_cols, inplace=True)

    # remove cols with only NaN
    att_dict['config']['drop_nan_columns'] = api.config.drop_nan_columns
    if api.config.drop_nan_columns:
        df.dropna(axis='columns', how='all', inplace=True)

    # remove rows with NAN except for dimension cols
    att_dict['config']['drop_nan_rows_cols'] = api.config.drop_nan_rows_cols
    drop_nan_rows_cols = tfp.read_list(api.config.drop_nan_rows_cols, df.columns)
    if drop_nan_rows_cols:
        prev_row_num = df.shape[0]
        df[drop_nan_rows_cols].dropna(subset=drop_nan_rows_cols, how='all', inplace=True)
        logger.debug('<drop_nan_rows_cols> deleted rows: {}/{}'.format(prev_row_num - df.shape[0], prev_row_num))

    # maps a certain value to nan for all object type columns
    if tfp.read_value(api.config.fill_categoricals_nan):
        cat_cols = df.select_dtypes(include='object')
        for col in cat_cols:
            df[col].fillna(value=api.config.fill_categoricals_nan, inplace=True)

    # im construction error-prone and ugly
    #if api.config.cut_obj_size > 0:
    #    cols_obj = df.select_dtypes(include='object')
    #    dict_mapping = dict()
    #    for col in cols_obj:
    #        if df[col].str.len().max() > api.config.cut_obj_size:
    #            catmap = dict(enumerate(df[col].unique()))
    #            valmap = {val: val[:api.config.cut_obj_size - 3] + '_' + str(cat) for cat, val in catmap.items()}
    #            if len(api.config.fill_categoricals_nan) > 0:
    #                if api.config.fill_categoricals_nan in valmap.keys():
    #                    valmap[api.config.fill_categoricals_nan] = api.config.fill_categoricals_nan
    #            df[col] = df[col].map(valmap)  # problem
    #        df[col].str.replace(r'[,\.:;]', '')
    #    print(dict_mapping)

    if api.config.fill_numeric_nan_zero:
        cols_num = df.select_dtypes(include=np.number)
        for col in cols_num:
            df[col] = df[col].fillna(0.0)

    print('Cols: {} -> {}   Rows: {} -> {}'.format(att_dict['prev_number_columns'], df.shape[1],
                                                   att_dict['prev_number_rows'], df.shape[0]))

    ###### end of doing calculation

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

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
