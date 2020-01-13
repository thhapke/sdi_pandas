# Group by - sdi_pandas.groupby (Version: 0.0.17)

Groups the named columns by using the given aggregations.

## Inport

* **input** (Type: message.DataFrame) 

## outports

* **log** (Type: string) 
* **output** (Type: message.DataFrame) 

## Config

* **groupby** - Groupby Columns (Type: string) List of comma separated columns to group
* **aggregation** - Aggregation Mapping (Type: string) List of comma separated mappings of columns with the type of aggregation, e.g. price:mean,city:count
* **index** - Set Index (Type: boolean) Set Index
* **drop_columns** - Drop Columns (Type: string) List of columns of the joined DataFrame that could be dropped.


# Tags
pandas : 

# References
[pandas doc: grouby](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html)

