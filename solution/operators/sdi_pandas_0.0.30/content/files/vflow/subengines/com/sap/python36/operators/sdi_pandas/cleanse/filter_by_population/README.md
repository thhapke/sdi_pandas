# Filter by Population - sdi_pandas.cleanse.filter_by_population (Version: 0.0.1)

Filter out columns with less population than a threshold.

## Inport

* **data** (Type: message.DataFrame) Input data

## outports

* **log** (Type: string) Logging
* **data** (Type: message.DataFrame) Output data
* **transformation** (Type: message.DataFrame) Transformation data

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **columns** - Columns (Type: string) Columns to check for 1 unique value
* **info_only** - Info only (Type: boolean) Only check without data modification.
* **threshold** - Threshold (Type: number) Threshold by with column is droped.


# Tags
python36 : sdi_utils : 

# References


