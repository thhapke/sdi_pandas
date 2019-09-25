import pandas as pd
import os

# setting display options for df
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 15)

sourcefile = "/Users/d051079/data/Addresses/uk-500.csv"
df = pd.read_csv(sourcefile, sep=',',nrows=None)

division = 6
parts = int(df.shape[0]/division)
print(df.tail(1))

list_df = list()
for  i in range(0,division-1) :
    i_df = df.iloc[i:(i + parts), :]
    list_df.append(i_df)
    print('Row index: {}  {}->{}   parts:{}'.format(i,i*parts,(i+1)*parts,i_df.shape[0]))

idf = df.iloc[(division-1)*parts:, :]
list_df.append(idf)
print('Row index: {}  {}->{}   parts:{}'.format(division-1,(division-1)*parts,df.shape[0],idf.shape[0]))

bfname,suffix = os.path.basename(sourcefile).split(".")
dirname = os.path.dirname(sourcefile)

subdir = os.path.join(dirname, 'split_' + bfname)
try:
    subdir = os.mkdir(subdir)
except FileExistsError:
    print("Directory " , subdir ,  " already exists")

for i,i_df in enumerate(list_df) :
    new_file = os.path.join(dirname, subdir, bfname + '_' + str(i) + '.'+suffix)
    print('Write file: ' + new_file)
    i_df.to_csv(new_file,sep = ';',index = False)