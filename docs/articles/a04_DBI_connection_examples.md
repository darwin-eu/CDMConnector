# DBI connection examples

The following connection examples are provided for reference.

### Postgres

Connect to Postgres using the RPostgres package.

``` r
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                      host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                      user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                      password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

cdm <- cdmFromCon(con, 
                  cdmSchema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"), 
                  writeSchema = Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA"))

DBI::dbDisconnect(con)
```

Connect to Postgres using DatabaseConnector (version 7 or later).

``` r

library(DatabaseConnector)
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
                                             user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                                             password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))


con <- connect(connectionDetails)

cdm <- cdmFromCon(con, 
                  cdmSchema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"), 
                  writeSchema = Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA"))

disconnect(con)
```

### Redshift

Connect to Redshift using the RPostgres package.

``` r
con <- DBI::dbConnect(RPostgres::Redshift(),
                      dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                      host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                      port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                      user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                      password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

cdm <- cdmFromCon(con, 
                  cdmSchema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"), 
                  writeSchema = Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA"))

DBI::dbDisconnect(con)
```

Connect to Redshift using the DatabaseConnector package (version 7 or
later).

``` r
library(DatabaseConnector)  

connectionDetails <- createConnectionDetails(dbms = "redshift",
                                             server = Sys.getenv("CDM5_REDSHIFT_SERVER"),
                                             user = Sys.getenv("CDM5_REDSHIFT_USER"),
                                             password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"),
                                             port = Sys.getenv("CDM5_REDSHIFT_PORT"))
con <- connect(connectionDetails)

cdm <- cdmFromCon(con, 
                  cdmSchema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"), 
                  writeSchema = Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA"))

disconnect(con)
```

### SQL Server

Using odbc with SQL Server requires driver setup described
[here](https://solutions.posit.co/connections/db/r-packages/odbc/).
Note, you’ll likely need to [download the ODBC Driver for SQL
Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16).

``` r
con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = "ODBC Driver 18 for SQL Server",
                      Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                      Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                      UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                      PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                      TrustServerCertificate="yes",
                      Port     = 1433)

cdm <- cdmFromCon(con, 
                    cdmSchema = c("cdmv54", "dbo"), 
                    writeSchema =  c("tempdb", "dbo"))

DBI::dbDisconnect(con)
```

The connection to SQL Server can be simplified by configuring a DSN. See
[here](https://www.r-bloggers.com/2018/05/setting-up-an-odbc-connection-with-ms-sql-server-on-windows/)
for instructions on how to set up the DSN. If we named it “SQL”, our
connection is then simplified to.

``` r
con <- DBI::dbConnect(odbc::odbc(), "SQL")
cdm <- cdmFromCon(con, 
                    cdmSchema = c("tempdb", "dbo"), 
                    writeSchema =  c("ATLAS", "RESULTS"))
DBI::dbDisconnect(con)
```

Connect to SQL Server using the DatabaseConnector package (version 7 or
later).

``` r
library(DatabaseConnector)
connectionDetails <- createConnectionDetails(
  dbms = "sql server",
  server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
  user = Sys.getenv("CDM5_SQL_SERVER_USER"),
  password = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
  port = Sys.getenv("CDM5_SQL_SERVER_PORT")
)

con <- connect(connectionDetails)

cdm <- cdmFromCon(con, 
                  cdmSchema = c("cdmv54", "dbo"), 
                  writeSchema =  c("tempdb", "dbo"))

disconnect(con)
```

### Snowflake

We can use the odbc package to connect to snowflake.

``` r
con <- DBI::dbConnect(odbc::odbc(),
                          SERVER = Sys.getenv("SNOWFLAKE_SERVER"),
                          UID = Sys.getenv("SNOWFLAKE_USER"),
                          PWD = Sys.getenv("SNOWFLAKE_PASSWORD"),
                          DATABASE = Sys.getenv("SNOWFLAKE_DATABASE"),
                          WAREHOUSE = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
                          DRIVER = Sys.getenv("SNOWFLAKE_DRIVER"))
cdm <- cdmFromCon(con, 
                  cdmSchema =  c("OMOP_SYNTHETIC_DATASET", "CDM53"), 
                  writeSchema =  c("ATLAS", "RESULTS"))

DBI::dbDisconnect(con)
```

Note, as with SQL server we could set up a DSN to simplify this
connection as described
[here](https://docs.snowflake.com/en/developer-guide/odbc/odbc-windows) for
windows and
[here](https://docs.snowflake.com/en/developer-guide/odbc/odbc-mac) for
macOS.

Connect to Snowflake using the DatabaseConnector package (version 7 or
later).

Your connection string will look something like
`jdbc:snowflake://asdf.snowflakecomputing.com?db=DBNAME&warehouse=COMPUTE_WH`

``` r
library(DatabaseConnector)

connectionDetails <- createConnectionDetails(
  dbms = "snowflake",
  connectionString = Sys.getenv("SNOWFLAKE_CONNECTION_STRING"),
  user = Sys.getenv("SNOWFLAKE_USER"),
  password = Sys.getenv("SNOWFLAKE_PASSWORD")
)

con <- connect(connectionDetails)

cdm <- cdmFromCon(con, 
                  cdmSchema =  c("OMOP_SYNTHETIC_DATASET", "CDM53"), 
                  writeSchema =  c("ATLAS", "RESULTS"))

disconnect(con)
```

### Databricks/Spark

To connect to Databricks using ODBC please follow the instructions here:
<https://solutions.posit.co/connections/db/databases/databricks/>

You will need to set two environment variables in your .Renviron file:
DATABRICKS_HOST=“\[Your organization’s Host URL\]”
DATABRICKS_TOKEN=“\[Your personal Databricks token\]”

Create or open the .Renviron file by running
[`usethis::edit_r_environ()`](https://usethis.r-lib.org/reference/edit.html)

``` r
con <- DBI::dbConnect(
  odbc::databricks(),
  httpPath = Sys.getenv("DATABRICKS_HTTPPATH"),
  useNativeQuery = FALSE
)

cdm <- cdmFromCon(con, 
                  cdmSchema =  "gibleed", 
                  writeSchema = "scratch")

DBI::dbDisconnect(con)
```

To connect to Databricks using DatabaseConnector use the following
example. The connection will look something like
`"jdbc:databricks://asdf.cloud.databricks.com/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/6"`

The password should be your databricks token.

``` r
library(DatabaseConnector)

connectionDetails <- createConnectionDetails(
  dbms = "spark",
  user = "token",
  password = Sys.getenv('DATABRICKS_TOKEN'),
  connectionString = Sys.getenv('DATABRICKS_CONNECTION_STRING')
)

con <- connect(connectionDetails)


cdm <- cdmFromCon(con, 
                  cdmSchema =  "gibleed", 
                  writeSchema = "scratch")

disconnect(con)
```

We can ignore the “ERROR StatusLogger Unrecognized format/conversion
specifier” messages as these have to do with the log format.

### Duckdb

Duckdb is an in-process database similar to SQLite. We use the duckdb
package to connect. The `dbdir` argument should point to the database
file location.

``` r
library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), 
                      dbdir = eunomiaDir("GiBleed"))

cdm <- cdmFromCon(con, 
                  cdmSchema = "main", 
                  writeSchema = "main")

DBI::dbDisconnect(con)
```

We can also use DatabaseConnector to connect to duckdb. In the example
the `server` argument points to the duckdb file location.

``` r
library(DatabaseConnector)
connectionDetails <- createConnectionDetails(
  "duckdb", 
  server = CDMConnector::eunomiaDir("GiBleed"))

con <- connect(connectionDetails)

cdm <- cdmFromCon(con, 
                  cdmSchema = "main", 
                  writeSchema = "main")


disconnect(con)
```
