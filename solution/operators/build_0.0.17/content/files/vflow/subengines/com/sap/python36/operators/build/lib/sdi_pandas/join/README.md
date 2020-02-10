# Join Dataframes - build.lib.sdi_pandas.join (Version: 0.0.17)

Joining 2 DataFrames using either the indices of both or on specified columns. Setting the new index ist necessary.

## Inport

* **left_input** (Type: message.DataFrame) 
* **right_input** (Type: message.DataFrame) 

## outports

* **log** (Type: string) 
* **output** (Type: message.DataFrame) 

## Config

* **how** - How to join (Type: string) How to join 2 DataFrames
* **on_index** - On index (Type: boolean) Join on indices of both DataFrames
* **left_on** - Left df column to join (Type: string) Left DataFrame column to join
* **right_on** - Right df column to join (Type: string) Right DataFrame column to join
* **drop_columns** - Drop columns (Type: string) List of columns to drop after join
* **new_indices** - New index or index list (Type: string) New index/list after merge


# Tags
pandas : 

# References
[pandas doc: .merge](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.merge.html)

