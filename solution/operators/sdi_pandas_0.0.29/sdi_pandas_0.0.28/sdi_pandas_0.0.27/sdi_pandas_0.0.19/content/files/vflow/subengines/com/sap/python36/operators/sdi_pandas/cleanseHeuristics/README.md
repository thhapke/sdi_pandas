# Cleanse Heuristics - sdi_pandas.cleanseHeuristics (Version: 0.0.17)

A couple of heuristics collected into one operator for cleansing data in a DataFrame.

## Inport

* **inDataFrameMsg** (Type: message.DataFrame) 

## outports

* **Info** (Type: string) 
* **outDataFrameMsg** (Type: message.DataFrame) 

## Config

* **value_to_nan** - Set value to Null/Nan (Type: string) Sets all data of categorical columns with value to nan
* **yes_no_to_num** - Yes/No to Numeric (Type: boolean) Yes/No to Numeric 1/0
* **drop_nan_columns** - Drops columns when all values are NaN (Type: boolean) Drop NaN Columns
* **all_constant_to_NaN** - Columns with unique Value to NaN (Type: boolean) Columns with unique Value to NaN
* **threshold_unique** - Threshold of unique values (Type: number) Threshold of unique values set to 1 (-> value exist) 
* **threshold_unique_cols** - Columns for unique threshold criteria (Type: string) Columns for unique threshold criteria
* **sparse** - Sparse  (Type: number) Absolute or relative number criteria of sparsenss. All values of column are set to nan
* **sparse_cols** - Columns for check on sparse (Type: string) Columns for check on sparse
* **drop_nan_rows_cols** - Drop NaN rows columns (Type: string) Columns for dropping NaN rows 
* **rare_value_quantile** - Rare Value Quantile (Type: number) Rare Value Quantile
* **rare_value_cols** - Columns for Rare Value Criteria (Type: string) Columns for Rare Value Criteria
* **rare_value_std** - Rare Value Standard Deviation (Type: number) Rare Value Standard Deviation
* **max_cat_num** - Maximum Number of Categories (Type: number) Maximum Number of Categories
* **max_cat_num_cols** - Columns for Maximum Number Categories Criteria (Type: string) Columns for Maximum Number Categories Criteria
* **reduce_categoricals_only** - Reduce Categorical Type Columns only (Type: boolean) Reduce Categorical Type Columns only
* **remove_duplicates_cols** - Columns for Remove Duplicate Criteria (Type: string) Columns for Remove Duplicate Criteria
* **fill_categoricals_nan** - Value to replace NaN (Type: string) Value that replaces NaN for categorical columns
* **fill_numeric_nan_zero** - Replaces numeric type columns nan with 0 (Type: boolean) Replaces numeric type columns nan with 0


# Tags
pandas : 

