# Drop Single Value Columns - sdi_pandas.cleanse.drop_1valuecolumn (Version: 0.0.1)

Drops columns of DataFrame with only one unique value.

## Inport

* **input** (Type: message) Input data

## outports

* **output** (Type: message) Output data
* **log** (Type: string) Logging
* **transformation** (Type: string) transformation

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **columns** - Columns (Type: string) Columns to check for 1 unique value
* **info_only** - Info only (Type: boolean) Only check without data modification.


# Tags
python36 : sdi_utils : 

# References


