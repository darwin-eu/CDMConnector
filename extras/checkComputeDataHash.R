# some manual checks that computeDataHash works

# postgres ----
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                      host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                      user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                      password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

write_schema <- Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA")
cdm_schema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")

DatabaseConnector::computeDataHash(con, cdm_schema)

DBI::dbDisconnect(con)


# redshift ----

con <- DBI::dbConnect(RPostgres::Redshift(),
                      dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                      host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                      port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                      user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                      password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

write_schema <- Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA")
cdm_schema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")


DatabaseConnector::computeDataHash(con, cdm_schema)

DBI::dbDisconnect(con)

# snowflake ----

con <- DBI::dbConnect(odbc::odbc(),
                      SERVER = Sys.getenv("SNOWFLAKE_SERVER"),
                      UID = Sys.getenv("SNOWFLAKE_USER"),
                      PWD = Sys.getenv("SNOWFLAKE_PASSWORD"),
                      DATABASE = Sys.getenv("SNOWFLAKE_DATABASE"),
                      WAREHOUSE = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
                      DRIVER = Sys.getenv("SNOWFLAKE_DRIVER"))


DatabaseConnector::dbms(con)
DatabaseConnector::computeDataHash(con, Sys.getenv("SNOWFLAKE_CDM_SCHEMA"))

DBI::dbDisconnect(con)

# sql server ----

con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
                      Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                      Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                      UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                      PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                      TrustServerCertificate="yes",
                      Port     = 1433)

DatabaseConnector::computeDataHash(con, Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"))

DBI::dbDisconnect(con)


# duckdb ----
con <- DBI::dbConnect(duckdb::duckdb(), CDMConnector::eunomiaDir())

DatabaseConnector::computeDataHash(con, "main")

DBI::dbDisconnect(con)


# spark ----
con <- DBI::dbConnect(
  odbc::databricks(),
  httpPath = Sys.getenv("DATABRICKS_HTTPPATH"),
  useNativeQuery = FALSE
)

cdmSchema <- Sys.getenv("DATABRICKS_CDM_SCHEMA")
DatabaseConnector::dbms(con)
class(con)
DatabaseConnector::getTableNames(con, cdmSchema)
DatabaseConnector::computeDataHash(con, cdmSchema)

DBI::dbDisconnect(con)

# bigquery ----
library(bigrquery)
bq_auth(path = Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH"))

cdmSchema <- Sys.getenv("BIGQUERY_CDM_SCHEMA")

con <- DBI::dbConnect(
  bigrquery::bigquery(),
  project = Sys.getenv("BIGQUERY_PROJECT_ID"),
  dataset = cdmSchema
)

DatabaseConnector::dbms(con)
class(con)
DatabaseConnector::getTableNames(con, cdmSchema)
options(bigrquery.quiet = TRUE)
# debugonce(DatabaseConnector::computeDataHash)

withr::with_options(list(bigrquery.quiet = TRUE),
                    DatabaseConnector::computeDataHash(con, cdmSchema)
)
DatabaseConnector::computeDataHash(con, cdmSchema)

DBI::dbDisconnect(con)


