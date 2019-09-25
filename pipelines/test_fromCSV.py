
import pandas as pd
import csv
import os
import src.fromCSVDataFrame as fromCSV



# setting display options for df
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 15)


# read files of directory
in_path = "/Users/d051079/data/Addresses/zip_split_uk-500"
out_path = "/Users/d051079/data/Addresses"
files_in_dir = [f for f in os.listdir(in_path) if os.path.isfile(os.path.join(in_path, f))and f.split('.')[1] == 'zip']

config = fromCSV.api.config
config.use_columns = "None"
config.downcast_float = True
config.downcast_int = True
config.compression = 'zip'


for i,f in enumerate(files_in_dir) :
    print(f)
    endofseq = True if i == len(files_in_dir) - 1 else False
    attributes = {'format': 'csv', "storage.pathInPolledDirectory": 'filename', 'name': 'Addresses', \
                  "storage.pathInPolledDirectory": f, \
                  'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir), 'storage.endOfSequence': endofseq}


    if  config.compression :
        f = open(os.path.join(in_path, f), mode='rb')
        csv_str = f.read()
    else:
        f = open(os.path.join(in_path, f), mode='r')
        csv_str = ''
        for line in f.read():
            csv_str += line

    msg = fromCSV.api.Message(attributes=attributes, body=csv_str)
    msg, info = fromCSV.api.call(msg, config)

    print(msg.body)
    print(msg.attributes)

msg.body.to_csv(os.path.join(out_path,'concat-uk-500.csv'),sep=',',quoting=csv.QUOTE_ALL,index=False)