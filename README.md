# DEPRECATED
This project is no longer actively maintained.

It is recommended that you use the [officially supported open source JDBC driver for Google Cloud Spanner](https://github.com/googleapis/java-spanner-jdbc) in combination with the [corresponding Hibernate dialect](https://github.com/GoogleCloudPlatform/google-cloud-spanner-hibernate).

## spanner-jdbc-converter
A generic tool for converting JDBC-compliant databases to Google Cloud Spanner and vice-versa. The converter will convert both your data model and data to Google Cloud Spanner. It has currently only been tested with PostgreSQL source databases.
It can also convert databases in the other direction, i.e. download a Google Cloud Spanner database into a PostgreSQL database. This way you can create a local backup of your Google Cloud Spanner database.

## How it works
The tool does the conversion in these stages:<br>
1. Copy the definition of all tables from the source database to Google Cloud Spanner. The tool uses a generic mapping for column data types. The generic mapping can be configured to suit your specific needs. Only tables with a primary key can be converted. <br>
2. Copy the data of all tables from the source database to Google Cloud Spanner.<br>


The conversion of both table definitions and data can be run in two different modes: SkipExisting and DropAndRecreate. <br>
In SkipExisting mode, tables that are already present in the destination Google Cloud Spanner database will be skipped, even if there are structural differences between the source and destination (i.e. it does not update the definition of the table). <br>
In SkipExisting mode, data will only be copied if the destination table is empty. <br>
In DropAndRecreate mode for table definition conversion, existing destination tables will be dropped and re-created during conversion, regardless of the contents of the table. During data conversion, this mode causes a "DELETE FROM TABLE" statement to be issued on all tables that are not empty.

## Parallelism
Data copy is performed as much as possible using parallel workers in order to speed up the process. The optimal settings depend on your local resources (number of CPU's, memory, etc.) and the number of nodes on your Cloud Spanner instance.

You can use these configuration options to set the degree of parallelism and other performance considerations (with default values):

DataConverter.batchSize=1500000			// The number of bytes in each commit to Cloud Spanner
DataConverter.maxNumberOfWorkers=10		// The maximum number of upload workers for one table running in parallel.
DataConverter.numberOfTableWorkers=10	// The number of table workers running in parallel.
