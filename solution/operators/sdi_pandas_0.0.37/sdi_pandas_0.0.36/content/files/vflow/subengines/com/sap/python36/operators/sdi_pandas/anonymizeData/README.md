# Anonymize Data - sdi_pandas.anonymizeData (Version: 0.0.1)

Anonymizes the dataset.

## Inport

* **data** (Type: message.DataFrame) Input data

## outports

* **log** (Type: string) Logging
* **data** (Type: message.DataFrame) Output data

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **to_nan** - To NaN (Type: string) Character to be replaced by NaN
* **anonymize_cols** - Anonymize Columns (Type: string) Anonymize columns for replacing with random strings orlinear transformed numbers
* **anonymize_to_int_cols** - Anonymize to Integer Columns (Type: string) Anonymize columns for replacing with random integers, e.g. IDs
* **enumerate_cols** - Prefix of enumerated columns (Type: string) Prefix of enumerated columns


# Tags
python36 : sdi_utils : 

# References
[Download template](https://raw.githubusercontent.com/thhapke/gensolution/master/diutil/customOperatorTemplate.py)

