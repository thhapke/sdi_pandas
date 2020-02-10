# Custom Operator Template - sdi_pandas.anonymizeData (Version: 0.0.1)

Template Operator that provides the framework for a custom operator that includes all the information needed for generating the descriptive json-files and the README.md.

## Inport

* **input** (Type: message) Input data

## outports

* **output** (Type: message) Output data
* **log** (Type: string) Logging

## Config

* **to_nan** - To NaN (Type: string) Character to be replaced by NaN
* **anonymize_cols** - Anonymize Columns (Type: string) Anonymize columns for replacing with random strings orlinear transformed numbers
* **anonymize_to_int_cols** - Anonymize to Integer Columns (Type: string) Anonymize columns for replacing with random integers, e.g. IDs
* **enumerate_cols** - Prefix of enumerated columns (Type: string) Prefix of enumerated columns


# Tags
python36 : 

# References
[Download template](https://raw.githubusercontent.com/thhapke/gensolution/master/diutil/customOperatorTemplate.py)

