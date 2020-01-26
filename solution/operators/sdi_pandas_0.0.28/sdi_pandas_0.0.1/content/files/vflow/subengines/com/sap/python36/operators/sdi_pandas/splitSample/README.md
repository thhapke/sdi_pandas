# Split Sample - sdi_pandas.splitSample (Version: 0.0.1)

Splits a sample by factor. If the 'lable' is defined the split is according to the frequency of the label to ensure that even for labels with far less frequency that the split factor still taken into account properly. 

## Inport

* **input** (Type: message) Input data

## outports

* **train** (Type: message) train sample
* **test** (Type: message) test sample
* **log** (Type: string) Logging

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **label** - Label (Type: string) Label to split
* **split** - Split Factor (Type: float) Split Factor
* **seed** - Seed (Type: int) Seed for random number generator
* **to_category** - To Categorgy (Type: Boolean) Cast <object> data type to categorical.


# Tags
python36 : sdi_utils : 

# References
[Download template](https://raw.githubusercontent.com/thhapke/gensolution/master/diutil/customOperatorTemplate.py)

