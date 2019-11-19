
import pandas as pd
import src.selectValues as selectdf
import src.fuzzyjoin as fuzzyjoindf

# setting display options for df
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 15)

df = pd.read_csv("/Users/d051079/data/Addresses/mapped_accounts_cleansed.csv", sep=',',nrows=None)
attributes = {'format': 'csv',"storage.pathInPolledDirectory" : 'filename','name':'Addresses'}
msg = selectdf.api.Message(attributes=attributes, body=df)

#print('===============================')
#print(df.head(10))

########## Operation select 1
config = selectdf.api.config
config.selection_num = "None"
config.selection_list = "Source = SFDC"
msgt,info = selectdf.api.call(msg,config)
print(info)


########## Operation select 2
config = selectdf.api.config
config.selection_num = "None"
config.selection_list = "Source = C4C"
msgb,info = selectdf.api.call(msg,config)
print(info)

########## Operation fuzzyjoin
config = fuzzyjoindf.api.config
config.check_columns = "AccountName : AccountName,  Country : Country, City : City, Street: Street, PostalCode: PostalCode"
config.limit = 80
config.test_index = 'AccountID'
config.only_index = False
config.joint_id = True
config.base_index = 'AccountID'
config.add_non_matching = True
msg,info = fuzzyjoindf.api.call(msgt,msgb,config)

print('===============================')
print(msg.body.head(10))
print(info)


msg.body.to_csv("/Users/d051079/data/Addresses/mapped_accounts_cleansed_matched.csv", sep=';',index=False)
