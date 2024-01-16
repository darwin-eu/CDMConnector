

test_copy_cdm_to <- function(con, write_schema) {
  if (dbms(con) == "bigquery") return(testthat::skip("failing test"))

  con1 <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  on.exit(DBI::dbDisconnect(con1, shutdown = TRUE), add = TRUE)

  cdm <- cdm_from_con(con1, cdm_schema = "main", cdm_name = "test") %>%
      # cdm_select_tbl(tbl_group("default")) # takes longer
      cdm_select_tbl("person", "vocabulary")

  # create another cdm
  cdm2 <- copy_cdm_to(con, cdm, schema = write_schema)
  expect_setequal(names(cdm), names(cdm2))
  expect_s3_class(cdm2, "cdm_reference")
  expect_error(copy_cdm_to(con, cdm, schema = write_schema))

  # drop test tables
  list_tables(con, write_schema) %>%
    stringr::str_subset(paste0("^", write_schema["prefix"])) %>%
    purrr::walk(~DBI::dbRemoveTable(con, inSchema(write_schema, ., dbms(con))))
}

# dbtype = "bigquery" # bigquery is failing
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - copy_cdm_to"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran()
    con <- get_connection(dbtype)
    prefix <- paste0("tbl", as.integer(Sys.time()), "_")
    write_schema <- get_write_schema(dbtype, prefix = prefix)
    skip_if(any(write_schema == "") || is.null(con))
    test_copy_cdm_to(con, write_schema)
    disconnect(con)
  })
}

test_that("duckdb - copy_cdm_to without prefix", {
  con1 <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm1 <- cdm_from_con(con1, cdm_schema = "main")

  con2 <- DBI::dbConnect(duckdb::duckdb())
  cdm2 <- copy_cdm_to(con2, cdm1, schema = "main")

  expect_setequal(names(cdm1), names(cdm2))
  expect_s3_class(cdm2, "cdm_reference")

  con3 <- DBI::dbConnect(duckdb::duckdb())
  cdm3 <- copy_cdm_to(con3, dplyr::collect(cdm1), schema = "main")

  expect_setequal(names(cdm1), names(cdm3))
  expect_s3_class(cdm3, "cdm_reference")

  DBI::dbDisconnect(con1, shutdown = T)
  DBI::dbDisconnect(con2, shutdown = T)
  DBI::dbDisconnect(con3, shutdown = T)
})



test_that("copy_to works locally", {
  skip("manual test")

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir("synthea-covid19-200k"))
  cdm <- cdm_from_con(con, "main")

  con2 <- DBI::dbConnect(RPostgres::Postgres(),
                         dbname = Sys.getenv("LOCAL_POSTGRESQL_DBNAME"),
                         host = Sys.getenv("LOCAL_POSTGRESQL_HOST"),
                         user = Sys.getenv("LOCAL_POSTGRESQL_USER"),
                         password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD"))

  # cdm_schema <- "cdm_covid19"
  cdm_schema <- c(schema = "scratch", prefix = "test")

  purrr::walk(
    list_tables(con2, cdm_schema),
    ~DBI::dbRemoveTable(con2, inSchema(cdm_schema, ., dbms = dbms(con))))

  # takes 10 minutes or so
  system.time({
    cdm2 <- copy_cdm_to(con2, cdm, schema = cdm_schema)
  })

  expect_s3_class(cdm2, "cdm_reference")

  purrr::walk(
    list_tables(con2, cdm_schema),
    ~DBI::dbRemoveTable(con2, inSchema(cdm_schema, ., dbms = dbms(con))))

  DBI::dbDisconnect(con, shutdown = TRUE)
  DBI::dbDisconnect(con2)
})
