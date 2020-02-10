# fuzzyjoin - sdi_pandas.fuzzyjoin (Version: 0.0.17)

A test datasets (testDataFrame) are checked if they (string-) match with a             base dataset (baseDataFrame). If more than one column are provided for checking then the average is              calculated of all columns.

## Inport

* **testdata** (Type: message.DataFrame) 
* **basedata** (Type: message.DataFrame) 

## outports

* **log** (Type: string) 
* **output** (Type: message.DataFrame) 

## Config

* **check_columns** - Columns to check (Type: string) Columns to check
* **limit** - Matching Limit (Type: integer) Matching Limit
* **test_index** - Index of Test DataFrame (Type: string) Index of Test DataFrame
* **only_matching_rows** - Matching Rows only (Type: boolean) Matching Rows only
* **only_index** - Add only Index to DataFrame (Type: boolean) Add only Index to DataFrame
* **joint_id** - Add joint id (Type: boolean) Add joint id
* **base_index** - Index of Base Dataset (Type: string) Index of Base Dataset
* **add_non_matching** - Add non matching datasets (Type: boolean) Add non matching datasets


# Tags
fuzzywuzzy : pandas : 

# References

[pandas doc: groupby](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html)
[fuzzywuzzy](https://github.com/seatgeek/fuzzywuzzy)

