
test_that("computePermanent works on duckdb", {

  skip_if_not(rlang::is_installed("duckdb", version = "0.6"))
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
  concept <- dplyr::tbl(con, "concept")

  q <- concept %>%
    dplyr::filter(domain_id == "Drug") %>%
    dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
    dplyr::count(isRxnorm)

  x <- computePermanent(q, "rxnorm_count")
  expect_error(computePermanent(q, "rxnorm_count"))

  expect_true(nrow(dplyr::collect(x)) == 2)
  expect_true("rxnorm_count" %in% DBI::dbListTables(con))

  x <- appendPermanent(q, "rxnorm_count")
  expect_true(nrow(dplyr::collect(x)) == 4)

  DBI::dbRemoveTable(con, "rxnorm_count")
  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("computePermanent works on Postgres", {

  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")

  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                        host     = Sys.getenv("CDM5_POSTGRESQL_HOST"),
                        user     = Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  newTableName <- paste0(c("temptable", sample(1:9, 7, replace = T)), collapse = "")

  vocab <- dplyr::tbl(con, dbplyr::in_schema("cdmv531", "vocabulary"))

  tempSchema <- "ohdsi"

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    computePermanent(newTableName, schema = tempSchema)

  expect_true(nrow(dplyr::collect(x)) == 2)
  expect_true(newTableName %in% CDMConnector::listTables(con, tempSchema))

  expect_error({vocab %>%
      dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
      computePermanent(newTableName, schema = tempSchema)})

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
    appendPermanent(newTableName, schema = tempSchema)

  expect_true(nrow(dplyr::collect(x)) == 3)

  DBI::dbRemoveTable(con, DBI::SQL(paste0(c(tempSchema, newTableName), collapse = ".")))
  expect_false(newTableName %in% CDMConnector::listTables(con, tempSchema))
  DBI::dbDisconnect(con)
})

test_that("computePermanent works on SQL Server", {

  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")

  con <- DBI::dbConnect(odbc::odbc(),
                        Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
                        Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                        Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                        UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                        PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                        TrustServerCertificate="yes",
                        Port     = 1433)

  newTableName <- paste0(c("temptable", sample(1:9, 7, replace = T)), collapse = "")

  vocab <- dplyr::tbl(con, dbplyr::in_catalog("cdmv54", "dbo", "vocabulary"))

  tempSchema <- c("cdmv54", "dbo")

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    computePermanent(newTableName, schema = tempSchema)

  expect_true(nrow(dplyr::collect(x)) == 2)
  expect_true(newTableName %in% CDMConnector::listTables(con, tempSchema))

  expect_error({vocab %>%
      dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
      computePermanent(newTableName, schema = tempSchema)})

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
    appendPermanent(newTableName, schema = tempSchema)

  expect_true(nrow(dplyr::collect(x)) == 3)

  DBI::dbRemoveTable(con, DBI::SQL(paste0(c(tempSchema, newTableName), collapse = ".")))
  expect_false(newTableName %in% CDMConnector::listTables(con, tempSchema))
  DBI::dbDisconnect(con)
})

test_that("computePermanent works on Redshift", {

  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")

  con <- DBI::dbConnect(RPostgres::Redshift(),
                        dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                        host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                        port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

  newTableName <- paste0(c("temptable", sample(1:9, 7, replace = T)), collapse = "")

  vocab <- dplyr::tbl(con, dbplyr::in_schema("cdmv531", "vocabulary"))

  # tables <- DBI::dbGetQuery(con, "select * from information_schema.tables")
  # tibble::tibble(tables) %>% dplyr::distinct(table_schema)

  tempSchema <- "public"

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    computePermanent(newTableName, schema = tempSchema)

  expect_true(nrow(dplyr::collect(x)) == 2)
  expect_true(newTableName %in% CDMConnector::listTables(con, tempSchema))

  expect_error({vocab %>%
      dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
      computePermanent(newTableName, schema = tempSchema)})

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
    appendPermanent(newTableName, schema = tempSchema)

  expect_true(nrow(dplyr::collect(x)) == 3)

  DBI::dbRemoveTable(con, DBI::SQL(paste0(c(tempSchema, newTableName), collapse = ".")))
  expect_false(newTableName %in% CDMConnector::listTables(con, tempSchema))
  DBI::dbDisconnect(con)
})

test_that("computePermanent works on Spark", {

  skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
  # skip("Only run this test manually")


  con <- DBI::dbConnect(odbc::odbc(), dsn = "Databricks")

  newTableName <- paste0(c("temptable", sample(1:9, 7, replace = T)), collapse = "")

  vocab <- dplyr::tbl(con, dbplyr::in_schema("omop531", "vocabulary"))

  tempSchema <- "omop531results"

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    computePermanent(newTableName, schema = tempSchema, overwrite = T)

  expect_true(nrow(dplyr::collect(x)) == 2)
  expect_true(newTableName %in% CDMConnector::listTables(con, tempSchema))

  expect_error({vocab %>%
      dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
      computePermanent(newTableName, schema = tempSchema)}, "already exists")

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
    appendPermanent(newTableName, schema = tempSchema)

  expect_true(nrow(dplyr::collect(x)) == 3)

  DBI::dbRemoveTable(con, DBI::SQL(paste0(c(tempSchema, newTableName), collapse = ".")))
  expect_false(newTableName %in% CDMConnector::listTables(con, tempSchema))
  DBI::dbDisconnect(con)
})


test_that("computeQuery works on Oracle", {
  library(ROracle)
  con <- DBI::dbConnect(DBI::dbDriver("Oracle"),
                   username = Sys.getenv("CDM5_ORACLE_USER"),
                   password= Sys.getenv("CDM5_ORACLE_PASSWORD"),
                   dbname = Sys.getenv("CDM5_ORACLE_SERVER"))


  person <- dplyr::tbl(con, dbplyr::in_schema("CDMV5", "PERSON")) %>%
    dplyr::rename_all(tolower)

  x <- person %>%
    dplyr::select(person_id) %>%
    head(5) %>%
    computeQuery()

  expect_equal(nrow(dplyr::collect(x)), 5)
  expect_s3_class(dplyr::collect(x), "data.frame")

  # list schemas
  # dbGetQuery(con, "select username as schema from sys.all_users")

  # TODO add support to listTables
  # x <- person %>%
  #   dplyr::select(person_id) %>%
  #   head(5) %>%
  #   computeQuery(temporary = FALSE, name = "tmp", schema = "TEMPEMULSCHEMA")
  #

  DBI::dbDisconnect(con)

})
