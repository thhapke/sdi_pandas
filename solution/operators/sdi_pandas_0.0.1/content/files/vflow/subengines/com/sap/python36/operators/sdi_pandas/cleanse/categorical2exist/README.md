# Categorical to Existence Transformation - sdi_pandas.cleanse.categorical2exist (Version: 0.0.1)

Transforms categorical values to exist (integer [0,1]).

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
* **upper_threshold** - Upper Threshold (Type: number) Columns with 2 values anda population more than upper_threshold are transformed to [0,1]
* **num_values** - Number of Values (Type: integer) Number of values that should be tested to set to '1' excluding 'None/nan' values
* **equal_only** - Only equal (Type: boolean) Only number of unique values that match that match 'Number of Values' should be transformed.


# Tags
python36 : sdi_utils : 

# References


