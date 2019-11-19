# Pandas DataFrame Operators
Are a set of operators that can be implemented on SAP Data Hub/SAP Data Intelligence. These operators 
help to create Pandas DataFrames from CSV-strings or byte-encoded data. 

Example graph with creating DataFrames, sampling, joining, selecting and creating CSV:
![Example pipeline: Create POS](images/CreatePOS_pipeline.png)

The list of operators are constantly growing and will never be complete. In any case it should provide you the idea how to develop quickly similar pandas operators that suits your requirements. At the end of the README.md you find a documention with common features and some practices of how it was developed. 

More on the pandas project and the benefits it provides to high-performance data structures and analysis you find at https://pandas.pydata.org.

## Requirements

In order to be able to deploy and run the examples, the following requirements need to be fulfilled:

- SAP Data Hub 2.3 or later installed on a supported [platform](https://support.sap.com/content/dam/launchpad/en_us/pam/pam-essentials/SAP_Data_Hub_2_PAM.pdf) or SAP Data Hub, [trial edition 2.3](https://blogs.sap.com/2018/04/26/sap-data-hub-trial-edition/)
- A docker-image with pandas package is available 

## Download and Installation
In the *solution*-folder you find the ready-to-import operators that will be stored under the path: 

- /files/vflow/subengines/com/sap/python36/operators/pandas


## Examples
In the github folder *example-graphs* you find an example how to use the operators.

## Known Issues

Currently there are no known issues with the operators but nonetheless although all operators come with test cases and the code has limited complexities there might be errors that are not discovered yet. Notes of failing cases are well-appreciated. 

## How to get support

If you need help or in case you found a bug please open a [Github Issue](https://github.com/SAP/datahub-integration-examples/issues).

## How to run
 Import lastest release in */solution/PandasDataFrameOperators-0.0.x.zip* via `SAP Data Hub System Management` -> `Files` -> `Import Solution`

## License

Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
This project is licensed under the SAP Sample Code License except as noted otherwise in the [LICENSE file](LICENSE).


## Documentation

* [fromCSVDataFrame.py](./manuals/fromCSVDataFrame.md) - creating DataFrame using a csv-string or byte-encoded csv-string
* [toCSVDataFrame.py](./manuals/toCSVDataFrame.md)  - creating a csv-string for saving as a file
* [joinDataFrame.py](./manuals/joinDataFrame.md) - joining 2 DataFrames
* [sampleDataFrame.py](./manuals/sampleDataFrame.md) - samples from DataFrame while taking the data of a defined column for a certain value
* [selectDataFrame.py](./manuals/selectDataFrame.md)- selecting rows of columns based on values
* [groupDataFrame.py](./manuals/groupbyDataFrame.py) - Grouping of columns with defined aggregation
* [fuzzyjoinDataFrame.py](./manuals/fuzzyjoinDataFrame.py) - Test the existance of the datasets of one 'test'-DataFrame in the 'base'-DataFrame with defined matching factor
* [dropDataFrame.py](./manuals/dropDataFrame.md) - drops or/and renames columns 
* [transposeColumnDataFrame.py](./manuals/transposeColumnDataFrame.py) - transposes the values one column to additional columns
* customDataFrame.py - doing nothing but can be enhanced by own scripts
* ... 

### Local Development Support
To work with the IDE of your choice and to run unit tests, you may start the development locally and do the appropriate tests before deploying the scripts in a SAP Data Hub / SAP Data Intelligence cluster. For doing this for all scripts supporting features are provided. There is also a hint for a simulation of a pipeline. Examples are given in the folder of */pipelines*. 


### Basic Architecture
The communication is based on **message.DataFrame** where the body is linked to the DataFrame and the attributes provides some basic information like

* number of columns
* number of rows
* index
* column names
* memory usage
* data types of columns

The ports of communincating between **pandas** operators are type **message.DataFrame** to ensure a test of connecting operators on modeler level. In addition in the script the type of the body (pandas.DataFrame) is tested as well. 

### Some common features 
#### Memory
Because memory usage for big data is critical, **fromCSV** supports to select columns and 
to downcast datatypes. Open is the implementation of datatype **category** to reduce the 
memory of the extremely memory demanding strings. 
It is assumed that all data processing with the pandas operators runs on the same pod. For crossing pods a streaming needs to be implemented that is still open. A simple workaround would be the saving of the results in an object store or a database and then reading it from other pods. 

#### Communication between operators
For the communication the data type **message** is used where 
* **attributes** contains a basic profile of the DataFrame i(e.g. name, last_operator, number of rows and columns, message usage, data types, column names, ...). 
* **body** of the message contains the byte-encoded DataFrame. 

The alternative of using a custom type was discarded because it is not supported within Python operators by providing and supporting the pre-defined structure. The only benefit is that in the Modeler the compatibility of the connections are checked. 

Within a Python operator you can access the attributes of the message as a dictionary where as the body stores the pointer to the DataFrame. 

Most of the di_pandas operators have 1 input dataport and 2 outputdata ports. The nomenclature is **DataFrameMsg** for the data message and **Info** for channeling infos to a terminal or a logging file for monitoring the graph behaviour while developing. 

### Operator Descriptions


