#
# test_that("computeQuery works on duckdb", {
#
#   skip_if_not(rlang::is_installed("duckdb", version = "0.6"))
#   skip_if_not(eunomia_is_available())
#
#   con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#   concept <- dplyr::tbl(con, "concept")
#
#   q <- concept %>%
#     dplyr::filter(domain_id == "Drug") %>%
#     dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
#     dplyr::count(isRxnorm)
#
#   x <- dplyr::compute(q)
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- computeQuery(q, "rxnorm_count")
#   expect_true(nrow(dplyr::collect(x)) == 2)
#   expect_true("rxnorm_count" %in% DBI::dbListTables(con))
#
#   x <- computeQuery(q, "rxnorm_count", temporary = FALSE, schema = "main", overwrite = TRUE)
#   expect_true(nrow(dplyr::collect(x)) == 2)
#   expect_true("rxnorm_count" %in% DBI::dbListTables(con))
#
#   x <- appendPermanent(q, "rxnorm_count")
#   expect_true(nrow(dplyr::collect(x)) == 4)
#
#   DBI::dbRemoveTable(con, "rxnorm_count")
#   DBI::dbDisconnect(con, shutdown = TRUE)
# })
#
# test_that("computeQuery works on Postgres", {
#
#   skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
#
#   con <- DBI::dbConnect(RPostgres::Postgres(),
#                         dbname   = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
#                         host     = Sys.getenv("CDM5_POSTGRESQL_HOST"),
#                         user     = Sys.getenv("CDM5_POSTGRESQL_USER"),
#                         password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
#
#   newTableName <- paste0(c("temptable", sample(1:9, 7, replace = T)), collapse = "")
#
#   vocab <- dplyr::tbl(con, dbplyr::in_schema("cdmv531", "vocabulary"))
#
#   tempSchema <- "ohdsi"
#
#   # temp table creation
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery()
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   # permanent table creation
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#   expect_true(newTableName %in% CDMConnector::listTables(con, tempSchema))
#
#   expect_error({vocab %>%
#       dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#       computeQuery(newTableName, schema = tempSchema, temporary = FALSE)}, "already exists")
#
#   expect_error({x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE, overwrite = TRUE)}, NA)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
#     appendPermanent(newTableName, schema = tempSchema)
#
#   expect_true(nrow(dplyr::collect(x)) == 3)
#
#   DBI::dbRemoveTable(con, DBI::SQL(paste0(c(tempSchema, newTableName), collapse = ".")))
#   expect_false(newTableName %in% CDMConnector::listTables(con, tempSchema))
#   DBI::dbDisconnect(con)
# })
#
# test_that("computeQuery works on SQL Server", {
#
#   skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
#
#   con <- DBI::dbConnect(odbc::odbc(),
#                         Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
#                         Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
#                         Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
#                         UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
#                         PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
#                         TrustServerCertificate="yes",
#                         Port     = 1433)
#
#   tempSchema <- c("cdmv54", "dbo")
#
#   vocab <- dplyr::tbl(con, dbplyr::in_catalog("cdmv54", "dbo", "vocabulary"))
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     dplyr::compute()
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   newTableName <- paste0(c("temptable", sample(1:9, 7, replace = TRUE)), collapse = "")
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#   expect_true(newTableName %in% CDMConnector::listTables(con, tempSchema))
#
#   expect_error({vocab %>%
#       dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#       computeQuery(newTableName, schema = tempSchema, temporary = FALSE)}, "already exists")
#
#   expect_error({x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE, overwrite = TRUE)}, NA)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
#     appendPermanent(newTableName, schema = tempSchema)
#
#   expect_true(nrow(dplyr::collect(x)) == 3)
#
#   DBI::dbRemoveTable(con, DBI::SQL(paste0(c(tempSchema, newTableName), collapse = ".")))
#   expect_false(newTableName %in% CDMConnector::listTables(con, tempSchema))
#   DBI::dbDisconnect(con)
# })
#
# test_that("computeQuery works on Redshift", {
#
#   skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")
#
#   con <- DBI::dbConnect(RPostgres::Redshift(),
#                         dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
#                         host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
#                         port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
#                         user     = Sys.getenv("CDM5_REDSHIFT_USER"),
#                         password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
#
#   newTableName <- paste0(c("temptable", sample(1:9, 7, replace = TRUE)), collapse = "")
#
#   vocab <- dplyr::tbl(con, dbplyr::in_schema("cdmv531", "vocabulary"))
#
#   # tables <- DBI::dbGetQuery(con, "select * from information_schema.tables")
#   # dplyr:tibble(tables) %>% dplyr::distinct(table_schema)
#
#   tempSchema <- "public"
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     dplyr::compute()
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#   expect_true(newTableName %in% CDMConnector::listTables(con, tempSchema))
#
#   expect_error({vocab %>%
#       dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#       computeQuery(newTableName, schema = tempSchema, temporary = FALSE)}, "already exists")
#
#   expect_error({x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE, overwrite = TRUE)}, NA)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
#     appendPermanent(newTableName, schema = tempSchema)
#
#   expect_true(nrow(dplyr::collect(x)) == 3)
#
#   DBI::dbRemoveTable(con, DBI::SQL(paste0(c(tempSchema, newTableName), collapse = ".")))
#   expect_false(newTableName %in% CDMConnector::listTables(con, tempSchema))
#   DBI::dbDisconnect(con)
# })
#
# test_that("computeQuery works on Snowflake", {
#
#   skip_if_not("Snowflake" %in% odbc::odbcListDataSources()$name)
#   skip("failing test")
#
#   con <- DBI::dbConnect(odbc::odbc(), "Snowflake")
#   cdm_schema <- strsplit(Sys.getenv("SNOWFLAKE_CDM_SCHEMA"), "\\.")[[1]]
#   write_schema <- strsplit(Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA"), "\\.")[[1]]
#
#   newTableName <- paste0(c("temptable", sample(1:9, 7, replace = TRUE)), collapse = "")
#
#   vocab <- dplyr::tbl(con, DBI::Id(catalog = "OMOP_SYNTHETIC_DATASET", schema = "CDM53", table = "vocabulary"))
#   vocab <- dplyr::tbl(con, DBI::SQL("OMOP_SYNTHETIC_DATASET.CDM53.vocabulary"))
#   vocab <- dplyr::tbl(con, DBI::SQL("OMOP_SYNTHETIC_DATASET.CDM53.VOCABULARY"))
#   listTables(con, c("OMOP_SYNTHETIC_DATASET", "CDM53"))
#   vocab <- dplyr::tbl(con, inSchema(cdm_schema, "vocabulary"))
#
#   # tables <- DBI::dbGetQuery(con, "select * from information_schema.tables")
#   # dplyr:tibble(tables) %>% dplyr::distinct(table_schema)
#
#   tempSchema <- "public"
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     dplyr::compute()
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#   expect_true(newTableName %in% CDMConnector::listTables(con, tempSchema))
#
#   expect_error({vocab %>%
#       dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#       computeQuery(newTableName, schema = tempSchema, temporary = FALSE)}, "already exists")
#
#   expect_error({x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE, overwrite = TRUE)}, NA)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
#     appendPermanent(newTableName, schema = tempSchema)
#
#   expect_true(nrow(dplyr::collect(x)) == 3)
#
#   DBI::dbRemoveTable(con, DBI::SQL(paste0(c(tempSchema, newTableName), collapse = ".")))
#   expect_false(newTableName %in% CDMConnector::listTables(con, tempSchema))
#   DBI::dbDisconnect(con)
# })
#
# test_that("computeQuery works on Spark", {
#
#   skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
#   skip("manual test")
#
#   con <- DBI::dbConnect(odbc::odbc(), dsn = "Databricks")
#
#   newTableName <- paste0(c("temptable", sample(1:9, 7, replace = TRUE)), collapse = "")
#
#   vocab <- dplyr::tbl(con, dbplyr::in_schema("omop531", "vocabulary"))
#
#   # tables <- DBI::dbGetQuery(con, "select * from information_schema.tables")
#   # dplyr::tibble(tables) %>% dplyr::distinct(table_schema)
#
#   tempSchema <- "omop531results"
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     # dplyr::compute() # Fails
#     computeQuery()
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, overwrite = TRUE)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE, overwrite = TRUE) # fails when overwrite is FALSE
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   expect_true(newTableName %in% listTables(con, tempSchema))
#
#   expect_error({x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE)}, "already exists")
#
#   expect_error({x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE, overwrite = TRUE)}, NA)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
#     appendPermanent(newTableName, schema = tempSchema)
#
#   expect_true(nrow(dplyr::collect(x)) == 3)
#
#   DBI::dbRemoveTable(con, DBI::SQL(paste0(c(tempSchema, newTableName), collapse = ".")))
#   expect_false(newTableName %in% listTables(con, tempSchema))
#   DBI::dbDisconnect(con)
# })
#
#
# test_that("computeQuery works on Oracle", {
#   # library(ROracle)
#   # con <- DBI::dbConnect(DBI::dbDriver("Oracle"),
#   #                       username = Sys.getenv("CDM5_ORACLE_USER"),
#   #                       password= Sys.getenv("CDM5_ORACLE_PASSWORD"),
#   #                       dbname = Sys.getenv("CDM5_ORACLE_SERVER"))
#
#   skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)
#
#   con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")
#
#   # dbGetQuery(con, "select username as schema from sys.all_users")
#
#   newTableName <- paste0(c("temptable", sample(1:9, 7, replace = T)), collapse = "")
#   # newTableName <- toupper(newTableName) # This test only passes when the table name is uppercase
#
#   vocab <- dplyr::tbl(con, dbplyr::in_schema("CDMV5", "VOCABULARY")) %>%
#     dplyr::rename_all(tolower)
#
#   # tempSchema <- "TEMPEMULSCHEMA"
#   tempSchema <- "OHDSI"
#
#   # dplyr::compute does not work on Oracle
#   # x <- vocab %>%
#   #   dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#   #   dplyr::compute()
#   #
#   # expect_true(nrow(dplyr::collect(x)) == 2)
#
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery()
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#   expect_true(newTableName %in% listTables(con, tempSchema))
#
#   expect_error({vocab %>%
#       dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#       computeQuery(newTableName, schema = tempSchema, temporary = FALSE)}, "already exists")
#
#   expect_error({x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
#     computeQuery(newTableName, schema = tempSchema, temporary = FALSE, overwrite = TRUE)}, NA)
#
#   expect_true(nrow(dplyr::collect(x)) == 2)
#
#   # x <- vocab %>%
#   #   dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
#   #   appendPermanent(newTableName, schema = tempSchema)
#   x <- vocab %>%
#     dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
#     appendPermanent(newTableName, schema = tempSchema)
#
#   expect_true(nrow(dplyr::collect(x)) == 3)
#
#   DBI::dbRemoveTable(con, name = newTableName, schema = tempSchema)
#   expect_false(newTableName %in% listTables(con, tempSchema))
#   DBI::dbDisconnect(con)
# })
#
# test_that("dropTable works on duckdb", {
#   skip_if_not_installed("duckdb")
#   skip_if_not(eunomia_is_available())
#
#   con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
#   cdm <- cdm_from_con(con, "main", write_schema = "main")
#
#   # create a temporary table in the remote database from a query
#   cdm$tmp_table <- cdm$concept %>%
#     dplyr::count(domain_id == "Drug") %>%
#     computeQuery("tmp_table", temporary = FALSE, schema = "main")
#
#   expect_true("tmp_table" %in% DBI::dbListTables(con))
#   expect_true("tmp_table" %in% names(cdm))
#
#   cdm <- dropTable(cdm, "tmp_table")
#
#   expect_false("tmp_table" %in% DBI::dbListTables(con))
#   expect_false("tmp_table" %in% names(cdm))
#
#
#
#   DBI::dbDisconnect(con, shutdown = TRUE)
# })
#
# test_that("dropTable works with tidyselect", {
#   skip_if_not_installed("duckdb")
#   skip_if_not(eunomia_is_available())
#
#   con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
#   cdm <- cdm_from_con(con, cdm_schema = "main", write_schema = "main")
#
#   # create two temporary tables in the remote database from a query with a common prefix
#   cdm$tmp_table <- cdm$concept %>%
#     dplyr::count(domain_id == "Drug") %>%
#     computeQuery("tmp_table", temporary = FALSE, schema = "main")
#
#   cdm$tmp_table2 <- cdm$concept %>%
#     dplyr::count(domain_id == "Condition") %>%
#     computeQuery("tmp_table2", temporary = FALSE, schema = "main")
#
#   expect_length(stringr::str_subset(DBI::dbListTables(con), "tmp"), 2)
#   expect_length(stringr::str_subset(names(cdm), "tmp"), 2)
#
#   # drop tables with a common prefix
#   cdm <- dropTable(cdm, name = dplyr::starts_with("tmp"))
#
#   expect_length(stringr::str_subset(DBI::dbListTables(con), "tmp"), 0)
#   expect_length(stringr::str_subset(names(cdm), "tmp"), 0)
#
#   DBI::dbDisconnect(con, shutdown = TRUE)
# })
#
# test_that("dropTable works on postgres", {
#   skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
#
#   con <- DBI::dbConnect(RPostgres::Postgres(),
#                         dbname   = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
#                         host     = Sys.getenv("CDM5_POSTGRESQL_HOST"),
#                         user     = Sys.getenv("CDM5_POSTGRESQL_USER"),
#                         password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
#
#   newTableName <- paste0(c("temptable", sample(1:9, 7, replace = T)), collapse = "")
#
#   cdm <- cdm_from_con(con, "cdmv531", write_schema = "ohdsi")
#
#   # create a temporary table in the remote database from a query
#   cdm$tmp_table <- cdm$vocabulary %>%
#     dplyr::count(vocabulary_reference) %>%
#     computeQuery("tmp_table",
#                  temporary = FALSE,
#                  schema = attr(cdm, "write_schema"),
#                  overwrite = TRUE)
#
#   expect_true("tmp_table" %in% DBI::dbListTables(con))
#   expect_true("tmp_table" %in% names(cdm))
#
#   cdm <- dropTable(cdm, "tmp_table")
#
#   expect_false("tmp_table" %in% DBI::dbListTables(con))
#   expect_false("tmp_table" %in% names(cdm))
#
#   DBI::dbDisconnect(con)
# })
#
#
# test_that("dropTable works on SQL Server", {
#   skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
#
#   con <- DBI::dbConnect(odbc::odbc(),
#                         Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
#                         Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
#                         Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
#                         UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
#                         PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
#                         TrustServerCertificate="yes",
#                         Port     = 1433)
#
#   cdm <- cdm_from_con(con, c("cdmv54", "dbo"), write_schema = c("cdmv54", "dbo"))
#
#   # create a temporary table in the remote database from a query
#   cdm$tmp_table <- cdm$vocabulary %>%
#     dplyr::count(vocabulary_reference) %>%
#     computeQuery("tmp_table",
#                  temporary = FALSE,
#                  schema = attr(cdm, "write_schema"),
#                  overwrite = TRUE)
#
#   expect_true("tmp_table" %in% listTables(con, schema = c("cdmv54", "dbo")))
#   expect_true("tmp_table" %in% names(cdm))
#
#   cdm <- dropTable(cdm, "tmp_table")
#
#   expect_false("tmp_table" %in% DBI::dbListTables(con))
#   expect_false("tmp_table" %in% names(cdm))
#
#   DBI::dbDisconnect(con)
# })
#
# test_that("uniqueTableName", {
#   result <- uniqueTableName()
#   expect_true(startsWith(result, "dbplyr_"))
# })
