import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="SDI_PANDAS",
    version="0.0.10",
    author="Thorsten Hapke",
    author_email="thorsten.hapke@sap.com",
    description="List of operators using the pandas module for processing the input",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/thhapke/DI_Pandas/",
    keywords = ['SAP Data Intelligence','pandas','operator'],
    packages=setuptools.find_packages(),
    install_requires=[],
    include_package_data=True,
    classifiers=[
    	'Programming Language :: Python :: 3.5',
    	'Programming Language :: Python :: 3.6',
    	'Programming Language :: Python :: 3.7',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)