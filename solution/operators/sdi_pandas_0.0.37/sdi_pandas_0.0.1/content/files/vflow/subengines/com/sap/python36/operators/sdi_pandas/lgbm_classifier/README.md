# LGBM Classifier - sdi_pandas.lgbm_classifier (Version: 0.0.1)

Classifies the data by using the light gbm classifier. 

## Inport

* **data** (Type: message.DataFrame) Input data

## outports

* **log** (Type: string) Logging
* **data** (Type: message.DataFrame) Output model

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **train_cols** - Training Columns (Type: string) Columns of DataFrame that are used for training
* **label_col** - Column of Label (Type: string) Label of DataFrame that needs to be predict


# Tags
python36 : sdi_utils : lightgbm : 

# References
[Download template](https://raw.githubusercontent.com/thhapke/gensolution/master/diutil/customOperatorTemplate.py)

