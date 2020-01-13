# Transpose Column - build.lib.sdi_pandas.transposeColumn (Version: 0.0.17)

Transposes the values of a column to new columns with the name of the values.             The values are taken from the value_column. The labels of the new columns are a concatination ot the             *transpose_column* and the value. *transpose_column* and *value_column*  are dropped.

## Inport

* **inDataFrameMsg** (Type: message.DataFrame) 

## outports

* **Info** (Type: string) 
* **outDataFrameMsg** (Type: message.DataFrame) 

## Config

* **transpose_column** - Transpose Column (Type: string) Transpose Column
* **value_column** - Value Column (Type: string) Value Column
* **aggr_trans** - Aggregation of transposed column (Type: string) Aggregation of transposed column
* **aggr_default** - Default aggregation  (Type: string) Default aggregation
* **groupby** - Group by columns (Type: string) Group by columns
* **as_index** - Groupby as index (Type: boolean) Groupby as index
* **reset_index** - Reset index (Type: boolean) Reset Index
* **prefix** - Prefix of transposed values (Type: string) Prefix of transposed values


# Tags
pandas : 

# References
[pandas doc: groupby](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html)

