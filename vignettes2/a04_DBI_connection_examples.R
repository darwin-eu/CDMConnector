## ---- include = FALSE---------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>",
  eval = FALSE
)

## ---- eval=FALSE--------------------------------------------------------------
#  con <- DBI::dbConnect(RPostgres::Postgres(),
#                        dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
#                        host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
#                        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
#                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
#  
#  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
#  DBI::dbDisconnect(con)

## ---- eval=FALSE--------------------------------------------------------------
#  con <- DBI::dbConnect(odbc::odbc(),
#                        Driver   = "ODBC Driver 18 for SQL Server",
#                        Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
#                        Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
#                        UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
#                        PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
#                        TrustServerCertificate="yes",
#                        Port     = 1433)
#  
#  cdm <- cdm_from_con(con, cdm_schema = c("CDMV5", "dbo"))
#  DBI::dbDisconnect(con)

## ---- eval=FALSE--------------------------------------------------------------
#  con <- DBI::dbConnect(RPostgres::Redshift(),
#                        dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
#                        host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
#                        port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
#                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
#                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
#  
#  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))
#  DBI::dbDisconnect(con)

## ---- eval=FALSE--------------------------------------------------------------
#  # odbc::odbcListDrivers() # view all installed odbc drivers
#  con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")
#  cdm <- cdm_from_con(con, cdm_schema = "CDMV5")
#  DBI::dbDisconnect(con)

