
get_connection <- function(dbms) {
  if (dbms == "postgres") {
    return(dbConnect(RPostgres::Postgres(),
                     dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                     host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                     user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                     password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD")))
  }

  if (dbms == "local" && Sys.getenv("LOCAL_POSTGRESQL_DBNAME") != "") {
    return(DBI::dbConnect(RPostgres::Postgres(),
                          dbname = Sys.getenv("LOCAL_POSTGRESQL_DBNAME"),
                          host = Sys.getenv("LOCAL_POSTGRESQL_HOST"),
                          user = Sys.getenv("LOCAL_POSTGRESQL_USER"),
                          password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD")))
  }

  if (dbms == "redshift" && Sys.getenv("CDM5_REDSHIFT_DBNAME") != "") {
    return(DBI::dbConnect(RPostgres::Redshift(),
                          dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                          host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                          port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                          user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                          password = Sys.getenv("CDM5_REDSHIFT_PASSWORD")))
  }

  if (dbms == "sqlserver" && Sys.getenv("SQL_SERVER_DRIVER") != "") {
    return(DBI::dbConnect(odbc::odbc(),
                          Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
                          Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                          Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                          UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                          PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                          TrustServerCertificate="yes",
                          Port     = 1433))
  }

  if (dbms == "oracle" && "OracleODBC-19" %in% odbc::odbcListDataSources()$name) {
    return(DBI::dbConnect(odbc::odbc(), "OracleODBC-19"))
  }
  NULL
}


get_cdm_schema <- function(dbms) {
  switch (dbms,
          "postgres" = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"),
          "local" =  Sys.getenv("LOCAL_POSTGRESQL_SCRATCH_SCHEMA"),
          "redshift" = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"),
          "sqlserver" = strsplit(Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"), "\\.")[[1]],
          "oracle" =  "CDMV5",
          NULL

  )
}

get_write_schema <- function(dbms) {
  switch (dbms,
          "postgres" = Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA"),
          "local" = Sys.getenv("LOCAL_POSTGRESQL_SCRATCH_SCHEMA"),
          "redshift" = Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA"),
          "sqlserver" = strsplit(Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA"), "\\.")[[1]],
          "oracle" = "OHDSI",
          NULL
  )
}
