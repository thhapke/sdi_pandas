# toCSVDataFrame
Creates a csv-formatted data passed to outport as message with the csv-string as body. 

## Input
* **DataFrame**

## Output
* **CSVMsg** string formatted as csv. 
* **Info**

## Config
* **write_index** -boolean- When True the index is saved as well
* **separator** -string- separator of the csv data (default = ; )

## Pandas Base
[pandas doc: to_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html)
