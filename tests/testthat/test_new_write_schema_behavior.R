# library(CDMConnector)
# con <- DBI::dbConnect(odbc::odbc(),
#                       Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
#                       Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
#                       Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
#                       UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
#                       PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
#                       TrustServerCertificate="yes",
#                       Port     = 1433)
#
# # "schema" now includes a prefix which is treated like a sub-schema namespace
# write_schema <- c(catalog = "CDMV5",
#                   schema = "dbo",
#                   prefix = "test_")
#
# cdm <- cdm_from_con(con,
#                     cdm_schema = c("CDMV5", "dbo"),
#                     write_schema = write_schema)
#
# DBI::dbWriteTable(con, inSchema(write_schema, "test", dbms = dbms(con)), cars)
#
# # lists only tables with the prefix (i.e. in the sub-schema)
# list_tables(con, schema = write_schema)
#
# # lists tables in the schema including the prefix i.e. "test_test"
# list_tables(con, schema = write_schema[1:2])
#
#
# # create a reference to the table "CDMV5"."dbo"."test_test"
# db <- dplyr::tbl(con, inSchema(write_schema, "test", dbms = dbms(con)))
# head(db, 3)
#
# # create a reference to the table "CDMV5"."dbo"."test_test"
# library(dplyr, warn.conflicts = FALSE)
# db2 <- db %>%
#   filter(speed == 4) %>%
#   computeQuery(name = "test2",
#                schema = write_schema,
#                temporary = FALSE,
#                overwrite = TRUE)
#
# # this table is now called "test_test2" in the database
# db2
#
#
# # remove the prefixed table "test_test" and "test_test2"
# DBI::dbRemoveTable(con, inSchema(write_schema, "test"))
# DBI::dbRemoveTable(con, inSchema(write_schema, "test2"))
#
#
#
#
#
