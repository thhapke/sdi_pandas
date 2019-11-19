import pandas as pd
import numpy as np
import logging

import json

import textfield_parser.textfield_parser as tfp

EXAMPLE_ROWS = 5

def process(df_msg):

    prev_att = df_msg.attributes
    df = df_msg.body
    if not isinstance(df,pd.DataFrame) :
        raise TypeError('Message body does not contain a pandas DataFrame')

    att_dict = dict()
    att_dict['config'] = dict()

    ###### start of doing calculation

    att_dict['prev_number_columns'] = df.shape[1]
    att_dict['prev_number_rows'] = df.shape[0]

    att_dict['config']['remove_duplicates_of_cols'] = api.config.remove_duplicates_of_cols
    remove_duplicates_cols = tfp.read_list(api.config.remove_duplicates_of_cols)
    if remove_duplicates_cols :
        df = df.groupby(remove_duplicates_cols).first().reset_index()
        logging.debug('#Dropped duplicates: {} - {} = {}'.format(att_dict['prev_number_rows'],df.shape[0], \
                                                                 att_dict['prev_number_rows']- df.shape[0]))

    att_dict['config']['to_nan'] = api.config.to_nan
    to_nan = tfp.read_value(api.config.to_nan)
    if to_nan :
        df.select_dtypes(include='object').replace(to_nan,np.nan,inplace = True)

    att_dict['config']['yes_no_to_boolean'] = str(api.config.yes_no_to_num)
    if api.config.yes_no_to_num :
        prev_categoricals = len (df.select_dtypes(include=np.object).columns)
        for col in df.select_dtypes(include=np.object) :
            df[col] = df[col].str.upper()
            vals = [x for x in df.loc[df[col].notnull(),col].unique()]
            if len(vals) == 1 and vals[0] in ['YES','Y'] :
                df.loc[df[col].notnull(),col] = 1
                df.loc[df[col].isnull(), col] = 0
                try :
                    df[col] = df[col].astype('int8')
                except ValueError :
                    print('Value Error: {}'.format(col))
                    print(df[col].unique())
            if len(vals) == 1 and vals[0] in ['NO','N'] :
                df.loc[df[col].notnull(),col] = 1
                df.loc[df[col].isnull(), col] = 0
                df[col] = df[col].astype('int8')
            if len(vals) == 2 and (all( i in vals for i in ['YES','NO']) or all( i in vals for i in ['Y','N'])) :
                df[col].replace(to_replace={'NO':0,'N':0,'no':0,'n':0,'YES':1,'Y':1,'yes':1,'y':1})
                df[col] = df[col].astype('int8')
        after_categoricals = len(df.select_dtypes(include=np.object).columns)
        logging.debug('<yes_no_to_boolean> impact: {} -> {}'.format(prev_categoricals,after_categoricals))

    # if all values of column == 0 then NaN
    att_dict['config']['all_constant_to_NaN'] = str(api.config.all_constant_to_NaN)
    if api.config.all_constant_to_NaN :
        num_constant_cols = 0
        for col in df.columns:
            unique_vals = df[col].unique()
            if len(unique_vals) == 1  :
                df[col] = np.nan
                num_constant_cols = num_constant_cols + 1
        logging.debug('<all_constant_to_NaN> number of columns: {}'.format(num_constant_cols))

    # remove rare value rows with quantile
    att_dict['config']['rare_value_cols'] = api.config.rare_value_cols
    att_dict['config']['rare_value_quantile'] = api.config.rare_value_quantile
    att_dict['config']['rare_value_std'] = api.config.rare_value_std
    rare_value_cols = tfp.read_list(api.config.rare_value_cols,list(df.columns))
    if rare_value_cols :
        logging.debug('quantile')
        # drop rare values by quantile
        if api.config.rare_value_quantile > 0 :
            if not api.config.rare_value_quantile >= 0 and api.config.rare_value_quantile < 1:
                raise ValueError('Quantile value range: [0,1[, not {}'.format(api.config.rare_value_quantile))
            num_reduce_categoricals_col = 0
            for col in rare_value_cols:
                unique_num = len(df[col].unique())
                val_num = df[col].count()
                ratio = df[col].count()/len(df[col].unique())
                threshold = df[col].count()/len(df[col].unique())*api.config.rare_value_quantile
                value_counts = df[col].value_counts()  # Specific column
                #kept_values = value_counts[value_counts > threshold].count()
                if value_counts[value_counts > threshold].count() > 1:
                    to_remove = value_counts[value_counts <= threshold].index
                    if len(to_remove) > 0 :
                        logging.debug('Drop rare value by quantile: Column {}: {}/{} '.format(col,len(to_remove),unique_num))
                        df[col].replace(to_remove, np.nan, inplace=True)
                        num_reduce_categoricals_col += 1
            logging.debug('<rare_value_quantile> impact on columns: {}/{}'.format(num_reduce_categoricals_col,len(rare_value_cols)))


        # drop rare values by std
        if api.config.rare_value_std > 0:
            num_reduce_categoricals_col = 0
            for col in df.columns:
                unique_num = len(df[col].unique())
                value_counts = df[col].value_counts()
                mean = value_counts.mean()
                threshold = value_counts.mean() - value_counts.std() * api.config.rare_value_std
                if threshold > 1  :
                    to_remove = value_counts[value_counts <= threshold].index
                    if len(to_remove) > 0  :
                        logging.debug('Drop rare value by std: Column {}: {}/{} '.format(col, len(to_remove),unique_num))
                        df[col].replace(to_remove, np.nan, inplace=True)
                        num_reduce_categoricals_col += 1
            logging.debug('<rare_value_std> impact on columns: {}/{}'.format(num_reduce_categoricals_col, len(rare_value_cols)))

    # for unique values less then threshold_unique set to 1. All NaN set to 0
    att_dict['config']['threshold_unique_cols'] = api.config.threshold_unique_cols
    att_dict['config']['threshold_unique'] = api.config.threshold_unique
    threshold_unique_cols = tfp.read_list(api.config.threshold_unique_cols,list(df.columns))
    if threshold_unique_cols:
        prev_obj_cols = len(df.select_dtypes("object"))
        for col in threshold_unique_cols:
            if df[col].dtype == np.object :
                unique_vals = list(df[col].unique())
                if len(unique_vals) <= api.config.threshold_unique:
                    # test if one of the values is non
                    if np.nan in unique_vals :
                        df.loc[df[col].notnull(),col] = 1
                        df.loc[df[col].isnull(),col] = 0
                        df[col] = df[col].astype('int8')
        after_obj_cols = len(df.select_dtypes("object"))
        logging.debug('Threshold unique effect on number of categorical columns: {} -> {}'.format(prev_obj_cols,after_obj_cols))


    # for count values less then threshold_count set to NaN
    att_dict['config']['threshold_freq_cols'] = api.config.threshold_unique_cols
    att_dict['config']['threshold_freq'] = api.config.threshold_unique
    threshold_freq_cols = tfp.read_list(api.config.threshold_freq_cols)
    if threshold_freq_cols :
        logging.debug('Threshold freq')
        if api.config.reduce_categoricals_only :
            test_cols = [ot for ot in threshold_freq_cols if df[ot].dtype==np.object]
        if api.config.threshold_freq < 1:
            api.config.threshold_freq = api.config.threshold_freq * df.shape[0]

        for col in threshold_freq_cols:
            if df[col].count() < api.config.threshold_freq:
                logging.debug('Threshold_count: Removed column {} (#values {})'.format(col,df[col].count()))
                df[col] = np.nan

    # removes columns with to many category values that could not be transposed
    att_dict['config']['max_cat_num'] = api.config.max_cat_num
    att_dict['config']['max_cat_num_cols'] = api.config.max_cat_num_cols
    max_cat_num_cols = tfp.read_list(api.config.max_cat_num_cols)
    if api.config.max_cat_num > 0 and max_cat_num_cols:
        drop_cols = list()
        for col in max_cat_num_cols:
            if df[col].dtype == np.object :
                if len(df[col].unique()) > api.config.max_cat_num :
                    drop_cols.append(col)
        df.drop(columns = drop_cols,inplace=True)

    # remove cols with only NaN
    att_dict['config']['drop_nan_columns'] = api.config.drop_nan_columns
    if api.config.drop_nan_columns :
        df.dropna(axis='columns',how='all',inplace=True)

    # remove rows with NAN except for dimension cols
    att_dict['config']['drop_nan_rows_cols'] = api.config.drop_nan_rows_cols
    drop_nan_rows_cols = tfp.read_list(api.config.drop_nan_rows_cols,df.columns)
    if drop_nan_rows_cols:
        prev_row_num = df.shape[0]
        df[drop_nan_rows_cols].dropna(subset=drop_nan_rows_cols, how = 'all',inplace=True)
        logging.debug('<drop_nan_rows_cols> deleted rows: {}/{}'.format(prev_row_num-df.shape[0],prev_row_num))

    if len(api.config.fill_categoricals_nan) > 0  :
        cat_cols = df.select_dtypes(include='object')
        for col in cat_cols :
            df[col].fillna(value=api.config.fill_categoricals_nan,inplace=True)

    # im construction error-prone and ugly
    if api.config.cut_obj_size > 0 :
        cols_obj = df.select_dtypes(include='object')
        dict_mapping = dict()
        for col in cols_obj :
            if df[col].str.len().max() > api.config.cut_obj_size :
                catmap = dict(enumerate(df[col].unique()))
                valmap = { val : val[:api.config.cut_obj_size-3] + '_' + str(cat)  for cat, val in catmap.items() }
                if len(api.config.fill_categoricals_nan) > 0:
                    if api.config.fill_categoricals_nan in valmap.keys() :
                        valmap[api.config.fill_categoricals_nan] = api.config.fill_categoricals_nan
                df[col] = df[col].map(valmap)  # problem
            df[col].str.replace(r'[,\.:;]','')
        print(dict_mapping)

    if api.config.fill_numeric_nan_zero :
        cols_num = df.select_dtypes(include=np.number)
        for col in cols_num :
            df[col] = df[col].fillna(0.0)


    print('Cols: {} -> {}   Rows: {} -> {}'.format(att_dict['prev_number_columns'],df.shape[1],
                                                   att_dict['prev_number_rows'], df.shape[0]))

    ###### end of doing calculation


    ##############################################
    #  final infos to attributes and info message
    ##############################################

    if df.empty :
        raise ValueError('DataFrame is empty')

    att_dict['operator'] = 'selectDataFrame'
    att_dict['name'] = prev_att['name']
    att_dict['memory'] = df.memory_usage(deep=True).sum() / 1024 ** 2
    att_dict['columns'] = str(list(df.columns))
    att_dict['number_columns'] = df.shape[1]
    att_dict['number_rows'] = df.shape[0]

    example_rows = EXAMPLE_ROWS if att_dict['number_rows'] > EXAMPLE_ROWS else att_dict['number_rows']
    for i in range(0,example_rows) :
        att_dict['row_'+str(i)] = str([ str(i)[:10].ljust(10) for i in df.iloc[i, :].tolist()])

    return  api.Message(attributes = att_dict,body=df)


'''
Mock pipeline engine api to allow testing outside pipeline engine
'''

class test :
    SIMPLE = 0

actual_test = test.SIMPLE

try:
    api
except NameError:
    class api:

        def set_default_input():
            df = pd.DataFrame(
                {'icol': [1, 2, 3, 4, 5], 'col 2': [1, 2, 3, 4, 5], 'col3': [100,200,300,400,500]})

            attributes = {'format': 'csv','name':'DF_name'}

            return api.Message(attributes=attributes,body=df)

        class config:
            to_nan = '0'
            yes_no_to_num = False
            drop_nan_columns = False
            all_constant_to_NaN = False
            threshold_unique = 0
            threshold_unique_cols = 'None'
            threshold_freq = 0
            threshold_freq_cols = 'None'
            drop_nan_rows_cols = 'None'
            rare_value_quantile = 0
            rare_value_cols = 'None'
            rare_value_std = 0
            max_cat_num = 0
            max_cat_num_cols = "None"
            reduce_categoricals_only = True
            remove_duplicates_of_cols = 'None'
            fill_categoricals_nan = '-'
            cut_obj_size = 0
            fill_numeric_nan_zero = True

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if not isinstance(msg,str) :
                print(msg.body.head(10))
            #else :
            #    print(msg)
            pass

        def set_port_callback(port, callback):
            msg = api.set_default_input()
            print("Call \"" + callback.__name__ + "\"  messages port \"" + port + "\"..")
            callback(msg)

        def call(msg,config):
            api.config = config
            result = process(msg)
            return result, json.dumps(result.attributes, indent=4)


def interface(msg):
    result = process(msg)
    api.send("outDataFrameMsg", result)
    info_str = json.dumps(result.attributes, indent=4)
    api.send("Info", info_str)


# Triggers the request for every message (the message provides the stock_symbol)
#api.set_port_callback("inDataFrameMsg", interface)

