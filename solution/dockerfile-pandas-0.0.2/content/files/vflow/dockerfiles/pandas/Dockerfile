# Use an official Python 3.6 image as a parent image
FROM python:3.6.4-slim-stretch

# Install python library "pandas"
RUN pip install pyarrow pandas

# Install python library "tornado" (Only required with SAP Data Hub version >= 2.5)
RUN pip install tornado==5.0.2