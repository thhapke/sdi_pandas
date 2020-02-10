# Select Values - sdi_pandas.selectValues (Version: 0.0.17)

Selecting data records based on column data restrictions (= SELECT * FROM ... WHERE COLX = x AND ...) of numeric types and lists of data. 

## Inport

* **inDataFrameMsg** (Type: message.DataFrame) 

## outports

* **Info** (Type: string) 
* **outDataFrameMsg** (Type: message.DataFrame) 

## Config

* **selection_num** - Selection in columns of numeric type (Type: string) Selection criteria for numerical columns. Comparison operators: ['=', '>', '<', '!' or '!=' ]. Example: order_id < 100000
* **selection_list** - Selection list (Type: string) Inclusion or exclusion list of values for numerical and string column.  Comparison operators: ['=', '!' or '!=' ]. Example: trans_date = 2016-03-03, 2016-02-04


# Tags
pandas : 

# References
[pandas doc: sample](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sample.html)

