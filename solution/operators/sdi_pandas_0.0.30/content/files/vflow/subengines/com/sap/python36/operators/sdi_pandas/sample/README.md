# Sample from Dataframe - sdi_pandas.sample (Version: 0.0.17)

Sampling over a DataFrame but keeps datasets with the same value of the             defined column as set and not splitting them, e.g. sampling with the invariant_column='date' samples             but ensures that all datasets of a certain date are taken or none. This leads to the fact that the             sample_size is only a guiding target. Depending on the size of the datasets with the same value of             the *invariant_column* compared to the *sample_size* this could deviate a lot. 

## Inport

* **input** (Type: message.DataFrame) 

## outports

* **log** (Type: string) 
* **output** (Type: message.DataFrame) 

## Config

* **sample_size** - Sample size (Type: number) Sample size
* **random_state** - Random state (Type: integer) Random state
* **invariant_column** - Invariant column (Type: string) Column where all the same value records should be kept as a whole in a sample


# Tags
pandas : 

# References
[pandas doc: sample](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.sample.html)

