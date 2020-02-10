# Drop Duplicates - sdi_pandas.cleanse.drop_duplicates (Version: 0.0.1)

Operator that removes duplicate rows in a DataFrame. 

## Inport

* **data** (Type: message.DataFrame) Input data

## outports

* **log** (Type: string) Logging
* **data** (Type: message.DataFrame) Output data

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **columns** - Columns (Type: string) Columns to check for duplicates
* **keep** - Keep rule (Type: string) Rule which values to keep


# Tags
python36 : sdi_utils : 

# References
[pandas.DataFrame.drop_duplicates](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html)

