# Drop Highly Unique Columns - sdi_pandas.cleanse.drop_highly_unique (Version: 0.0.1)

Drop columns with number of unique values (string) close to number of rows.
WARNING: exclude dtype=DateTime columns.

## Inport

* **data** (Type: message.DataFrame) Input data

## outports

* **log** (Type: string) Logging
* **transformation** (Type: message.DataFrame) Transformation data
* **data** (Type: message.DataFrame) Output data

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **columns** - Columns (Type: string) Columns to check for 1 unique value
* **info_only** - Info only (Type: boolean) Only check without data modification.
* **threshold** - Threshold (Type: number) Threshold by with column is droped.


# Tags
python36 : sdi_utils : 

# References


