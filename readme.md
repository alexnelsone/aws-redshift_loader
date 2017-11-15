# Lambda Redshift Data Loader

This lambda was written in oder to load data from s3 into Amazon Redshift. The Lambda is meant to read a CSV that contains column headers and build out a schema and the necessary tables. When the structure is done building it then loads the data to the target table.

If you are looking for a lambda to load data from s3 into your Redshift cluster, have a look at the AWSLabs github and look at their [Lambda Redshift Loader](https://github.com/awslabs/aws-lambda-redshift-loader).  This project is to develop a similar lambda using Python.  The lambda in the AWSLabs repo is a Node.js Lambda.
