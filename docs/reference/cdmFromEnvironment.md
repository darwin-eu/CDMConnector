# Create a CDM object from a pre-defined set of environment variables

This function is intended to be used with the Darwin execution engine.
The execution engine runs OHDSI studies in a pre-defined runtime
environment and makes several environment variables available for
connecting to a CDM database. Programmer writing code to run on the
execution engine and simply use `cdm <- cdmFromEnvironment()` to create
a cdm reference object to use for their analysis and the database
connection and cdm object should be automatically created. This obviates
the need for site specific code for connecting to the database and
creating the cdm reference object.

## Usage

``` r
cdmFromEnvironment(writePrefix = "")

cdm_from_environment(write_prefix = "")
```

## Arguments

- write_prefix, writePrefix:

  (string) An optional prefix to use for all tables written to the CDM.

## Value

A cdm_reference object

## Details

The environment variables used by this function and provided by the
execution engine are listed below.

- DBMS_TYPE: one of "postgresql", "sql server", "redshift", "duckdb",
  "snowflake".

- DATA_SOURCE_NAME: a free text name for the CDM given by the person
  running the study.

- CDM_VERSION: one of "5.3", "5.4".

- DBMS_CATALOG: The database catalog. Important primarily for compound
  schema names used in SQL Server and Snowflake.

- DBMS_SERVER: The database server URL.

- DBMS_NAME: The database name used for creating the connection.

- DBMS_PORT: The database port number.

- DBMS_USERNAME: The database username needed to authenticate.

- DBMS_PASSWORD: The database password needed to authenticate.

- CDM_SCHEMA: The schema name where the OMOP CDM is located in the
  database.

- WRITE_SCHEMA: The shema where the user has write access and tables
  will be created during study execution.

## Examples

``` r
if (FALSE) { # \dontrun{

library(CDMConnector)

# This will only work in an evironment where the proper variables are present.
cdm <- cdmFromEnvironment()

# Proceed with analysis using the cdm object.

# Close the database connection when done.
cdmDisconnect(cdm)
} # }
```
