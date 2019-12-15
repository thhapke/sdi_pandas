# Pandas DataFrame Operators
Are a set of operators that can be implemented on SAP Data Hub/SAP Data Intelligence. These operators 
help to create Pandas DataFrames from CSV-strings or byte-encoded data. 

Example graph with creating DataFrames, sampling, joining, selecting and creating CSV:
![Example pipeline: Create POS](images/CreatePOS_pipeline.png)

The list of operators are constantly growing and will never be complete. In any case it should provide you the idea of how to develop quickly similar pandas operators that suits your requirements. At the end of the README.md you find a documention with common features and some practices of how it was developed. 

More on the pandas project and the benefits it provides to high-performance data structures and analysis you find at [https://pandas.pydata.org](https://pandas.pydata.org).

All operators have been developed locally and tested both locally and on an SAP Data Intelligence instance. For more information of how I have done it you find at [sdi_utils](https://github.com/thhapke/sdi_utils) and my blog on SAP Community platform. 

## Requirements

In order to be able to deploy and run the examples, the following requirements need to be fulfilled:

- SAP Data Hub 2.3 or later installed on a supported [platform](https://support.sap.com/content/dam/launchpad/en_us/pam/pam-essentials/SAP_Data_Hub_2_PAM.pdf) or SAP Data Hub, [trial edition 2.3](https://blogs.sap.com/2018/04/26/sap-data-hub-trial-edition/)
- A docker-image with pandas package installed

## Download and Installation
In the *solution*-folder you find the ready-to-import operators that will be stored under the path: 

- /files/vflow/subengines/com/sap/python36/operators/pandas


## Examples
In the github folder *example-graphs* you find an example of how to use the operators.

## Known Issues

Currently there are no known issues with the operators but nonetheless although all operators come with test cases and the code has limited complexities there might be errors that are not discovered yet. Notes of failing cases are well-appreciated. 

## How to get support

If you need help or in case you found a bug please open a [Github Issue](https://github.com/SAP/datahub-integration-examples/issues).

## How to run
Import lastest release in */solution/PandasDataFrameOperators-0.0.x.zip* via `SAP Data Hub System Management` -> `Files` -> `Import Solution`

## License

This project is licensed under the [MIT License](src/LICENSE)


## Documentation
Each operator folder has a README that should describe the behaviour of the operator. 

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

The ports of communincating between **pandas** operators are type **message.DataFrame** to ensure a test of connecting operators on modeler level. 

In addition there is a port 'log' that collects all logging statements and provided it as string. 


### Some common features 
#### Memory
Because memory usage for big data is critical, **fromCSV** supports to select columns and 
to downcast datatypes. Open is the implementation of datatype **category** to reduce the 
memory of the extremely memory demanding strings. 
It is assumed that all data processing with the pandas operators runs in the same container. For crossing pods a streaming needs to be implemented or an intermediate saving of the results in an object store or a database and then reading it from other pods. 

#### Communication between operators
For the communication the data type **message** is used where 
* **attributes** contains a basic profile of the DataFrame i(e.g. name, last_operator, number of rows and columns, message usage, data types, column names, ...). 
* **body** of the message contains the byte-encoded DataFrame. 

The alternative of using a custom type was discarded because it is not supported within Python operators by providing and supporting the pre-defined structure. The only benefit is that in the Modeler the compatibility of the connections are checked. 

Within a Python operator you can access the attributes of the message as a dictionary where as the body stores the pointer to the DataFrame. 

Most of the di_pandas operators have 1 input dataport and 2 outputdata ports. The nomenclature is **DataFrameMsg** for the data message and **Info** for channeling infos to a terminal or a logging file for monitoring the graph behaviour while developing. 



