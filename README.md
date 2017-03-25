# spanner-jdbc-converter
A generic tool for converting JDBC-compliant databases to Google Cloud Spanner. The converter will convert both your data model and data to Google Cloud Spanner.
It has currently only been tested with PostgreSQL source databases. More will be added later.

## How it works
The tool does the conversion in these stages:<br>
1. Copy the definition of all tables from the source database to Google Cloud Spanner. The tool uses a generic mapping for column data types. The generic mapping can be configured to suit your specific needs. Only tables with a primary key can be converted. <br>
2. Copy the data of all tables from the source database to Google Cloud Spanner.<br>


The conversion of both table definitions and data can be run in two different modes: SkipExisting and DropAndRecreate. <br>
In SkipExisting mode, tables that are already present in the destination Google Cloud Spanner database will be skipped, even if there are structural differences between the source and destination (i.e. it does not update the definition of the table). <br>
In SkipExisting mode, data will only be copied if the destination table is empty. <br>
In DropAndRecreate mode for table definition conversion, existing destination tables will be dropped and re-created during conversion, regardless of the contents of the table. During data conversion, this mode causes a "DELETE FROM TABLE" statement to be issued on all tables that are not empty.
