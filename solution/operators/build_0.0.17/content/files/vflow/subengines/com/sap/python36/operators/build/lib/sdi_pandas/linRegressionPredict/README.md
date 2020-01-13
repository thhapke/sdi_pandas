# Linear Regression Predict - build.lib.sdi_pandas.linRegressionPredict (Version: 0.0.17)

Using the model calculated with the Scikit Learn module to predict values.

## Inport

* **inData** (Type: message.DataFrame) 
* **inCoef** (Type: message.DataFrame) 

## outports

* **log** (Type: string) 
* **output** (Type: message.DataFrame) 

## Config

* **prediction_col_only** - Prediction Columns only (Type: boolean) The output only contains the prediction columns
* **regresssion_cols_value** - Set value for some regression columns (Type: string) list of comma-separated maps with columns and values that overrides the prediction data of the *inData* message. Only applicable for fixe values. Otherwise the *inData* message needs to be used. 
* **prediction_prefix** - Prefix for prediction columns (Type: string) Prefix for prediction columns


# Tags
pandas : sklearn : 

# References
[ScitLearn Linear Regression](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html)

