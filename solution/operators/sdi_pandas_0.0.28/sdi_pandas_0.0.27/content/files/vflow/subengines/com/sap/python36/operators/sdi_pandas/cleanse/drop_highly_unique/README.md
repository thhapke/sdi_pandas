# Drop Highly Unique Columns - sdi_pandas.cleanse.drop_highly_unique (Version: 0.0.1)

Drop columns with number of unique values (string) close to number of rows.
WARNING:             exclude dtype=DateTime columns.

## Inport

* **input** (Type: message) Input data

## outports

* **output** (Type: message) Output data
* **log** (Type: string) Logging
* **transformation** (Type: string) transformation

## Config

* **columns** - Columns (Type: string) Columns to check for 1 unique value
* **info_only** - Info only (Type: boolean) Only check without data modification.
* **threshold** - Threshold (Type: number) Threshold by with column is droped.


# Tags
python36 : 

# References


