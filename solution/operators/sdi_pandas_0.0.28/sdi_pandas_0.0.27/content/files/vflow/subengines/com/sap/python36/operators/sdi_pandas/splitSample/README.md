# Custom Operator Template - sdi_pandas.splitSample (Version: 0.0.1)

Template Operator that provides the framework for a custom operator that includes all the information needed for generating the descriptive json-files and the README.md.

## Inport

* **input** (Type: message) Input data

## outports

* **train** (Type: message) train sample
* **test** (Type: message) test sample
* **log** (Type: string) Logging

## Config

* **label** - Label (Type: string) Label to split
* **split** - Split Factor (Type: float) Split Factor
* **seed** - Seed (Type: int) Seed for random number generator
* **to_category** - To Categorgy (Type: Boolean) Cast <object> data type to categorical.


# Tags
python36 : 

# References
[Download template](https://raw.githubusercontent.com/thhapke/gensolution/master/diutil/customOperatorTemplate.py)

