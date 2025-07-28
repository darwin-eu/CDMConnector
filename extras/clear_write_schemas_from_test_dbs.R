
# postgres ----
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                      host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                      user = Sys.getenv("CDM5_POSTGRESQL_USER"),
                      password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

write_schema <- Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA")
cdm_schema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")

tables <- listTables(con, write_schema)
tablesToDrop <- grep("temp|^tbl|test|og_", tables, value = T)

df <- dplyr::tibble(tables = tables)

# slow way
# for (i in cli::cli_progress_along(tablesToDrop)) {
#   DBI::dbRemoveTable(con, DBI::Id(table = tablesToDrop[i], schema = write_schema))
# }

# fast way
tablesToDropSql <- paste("drop table if exists", paste(tablesToDrop, collapse = ", "), ";")

DBI::dbExecute(con, tablesToDropSql)
DBI::dbDisconnect(con)

# snowflake ---

library(CDMConnector)
con <- DBI::dbConnect(odbc::odbc(),
                      SERVER = Sys.getenv("SNOWFLAKE_SERVER"),
                      UID = Sys.getenv("SNOWFLAKE_USER"),
                      PWD = Sys.getenv("SNOWFLAKE_PASSWORD"),
                      DATABASE = Sys.getenv("SNOWFLAKE_DATABASE"),
                      WAREHOUSE = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
                      DRIVER = Sys.getenv("SNOWFLAKE_DRIVER"))

cdm_schema <- strsplit(Sys.getenv("SNOWFLAKE_CDM_SCHEMA"), "\\.")[[1]]
write_schema <- strsplit(Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA"), "\\.")[[1]]
write_schema <- c("ATLAS", "RESULTS")

tables <- listTables(con, write_schema)
tablesToDrop <- grep("temp|^tbl|test|og_|tmp|TMP|TEMP|TBL|OG_", tables, value = T)
tablesToDrop <- stringr::str_subset(tables, "ACHILLES|COHORT_DIAGNOSTICS", negate = T)

df <- dplyr::tibble(tables =tables)

# slow way. no faster way on snowflake as far as I can tell. Need to quote lower case names.
for (i in cli::cli_progress_along(tablesToDrop)) {
  DBI::dbExecute(con, paste0("drop table if exists ", Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA"),'."',
                             tablesToDrop[i], '";'))
}

DBI::dbGetQuery(con, "select * from ATLAS.RESULTS.TEMP24218_CHRT0")
DBI::dbExecute(con, paste0("drop table ", Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA"),".",
                           '"codelist_og_006_1748253280"', ";"))

DBI::dbQuoteIdentifier(con, "adf")
is <- DBI::dbGetQuery(con, "select * from information_schema.tables")

DBI::dbGetQuery(con, "SHOW DATABASES;")
DBI::dbExecute(con, "USE DATABASE ATLAS;")
DBI::dbGetQuery(con, "SHOW SCHEMAS;")
DBI::dbExecute(con, 'CREATE SCHEMA SCRATCH;')

DBI::dbDisconnect(con)


# sql server -----
con <- DBI::dbConnect(odbc::odbc(),
                      Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
                      Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                      Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                      UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                      PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                      TrustServerCertificate="yes",
                      Port     = 1433)

write_schema <- strsplit(Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA"), "\\.")[[1]]

tables <- listTables(con, write_schema)
df <- dplyr::tibble(tables = tables)
tablesToDrop <- grep("temp|^tbl|test|og_|tmp|TMP|TEMP|TBL|OG_", tables, value = T)
tablesToDrop <- paste0('"', tablesToDrop, '"')
tablesToDropSql <- paste0("drop table if exists ", paste(paste0("tempdb.dbo." , tablesToDrop), collapse = ", "), ";")
tablesToDropSql <- paste0("drop table if exists ", paste(paste0("tempdb.dbo." , tables), collapse = ", "), ";")
# tablesToDropSql <- paste0("drop table ", 'tempdb.dbo."temp43500_cohort_attrition", tempdb.dbo."temp43500_cohort_codelist"')
DBI::dbExecute(con, tablesToDropSql)
DBI::dbQuoteIdentifier(con, "asdf")

DBI::dbDisconnect(con)

# redshift -----
con <- DBI::dbConnect(RPostgres::Redshift(),
                      dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                      host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                      port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                      user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                      password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

write_schema <- Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA")
tables <- listTables(con, write_schema)
tablesToDrop <- grep("temp|^tbl|test|og_|tmp", tables, value = T)

df <- dplyr::tibble(tables = tables)

# slow way
# for (i in cli::cli_progress_along(tablesToDrop)) {
#   DBI::dbRemoveTable(con, DBI::Id(table = tablesToDrop[i], schema = write_schema))
# }

# fast way
tablesToDropSql <- paste("drop table if exists", paste(tablesToDrop, collapse = ", "), ";")
tablesToDropSql <- paste("drop table if exists", paste(tables, collapse = ", "), ";")

DBI::dbExecute(con, tablesToDropSql)

DBI::dbDisconnect(con)

