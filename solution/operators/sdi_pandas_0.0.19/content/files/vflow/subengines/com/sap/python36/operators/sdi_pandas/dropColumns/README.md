# dropColumns - sdi_pandas.dropColumns (Version: 0.0.17)

Drops or/and renames DataFrame columns

## Inport

* **input** (Type: message.DataFrame) 

## outports

* **log** (Type: string) 
* **output** (Type: message.DataFrame) 

## Config

* **drop_columns** - Columns to drop (Type: string) * *comma separated list of columns*: columns to drop
* *NOT: comma separated list of columns*: drop all columns except columns in the list
* *ALL* : drop all columns and reset index - same as *NOT*
* **rename_columns** - Rename Columns (Type: string) *  *comma separated list of mappings*: columns to be
renamed, e.g. Col1:col_1, Col2:col_2


# Tags
pandas : 

# References

[pandas doc: drop](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop.html)
[pandas doc: rename](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rename.html)

