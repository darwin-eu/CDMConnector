
test_compute_query <- function(con, cdm_schema, write_schema) {
  cdm <- cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)

  new_table_name <- paste0("temp", floor(10*as.numeric(Sys.time()) %% 1e7))

  # temp table creation from query ----
  q <- cdm$concept %>%
    dplyr::filter(domain_id == "Drug") %>%
    dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
    dplyr::count(.data$isRxnorm)

  x <- computeQuery(q,
                    name = new_table_name,
                    temporary = TRUE,
                    overwrite = FALSE)


  expect_true(new_table_name %in% list_tables(con))

  expect_error({
    computeQuery(q,
                 name = new_table_name,
                 temporary = TRUE,
                 overwrite = FALSE)
  })


  expect_s3_class(dplyr::collect(x), "data.frame")

  # test overwrite for temp tables.
  expect_no_error({
    x <- computeQuery(q,
                      name = new_table_name,
                      temporary = TRUE,
                      overwrite = TRUE)
  })
  expect_s3_class(dplyr::collect(x), "data.frame")

  # test removal of temp tables
  DBI::dbRemoveTable(con, inSchema(NULL, table = new_table_name, dbms = dbms(con)))
  expect_false(new_table_name %in% list_tables(con))

  # permanent table creation from query ----
  new_table_name <- paste0("temp", floor(10*as.numeric(Sys.time()) %% 1e7))
  x <- cdm$vocabulary %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    compute_query(name = new_table_name, schema = write_schema, temporary = FALSE, overwrite = TRUE)

  expect_true(nrow(dplyr::collect(x)) == 2)
  expect_true(new_table_name %in% listTables(con, write_schema))

  expect_error({
    cdm$vocabulary  %>%
      dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
      computeQuery(new_table_name, schema = write_schema, temporary = FALSE, overwrite = FALSE)},
  "already exists")

  expect_no_error({
    x <- cdm$vocabulary  %>%
      dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
      computeQuery(new_table_name, schema = write_schema, temporary = FALSE, overwrite = TRUE)
  })

  expect_true(nrow(dplyr::collect(x)) >= 2)

  x <- cdm$vocabulary %>%
    dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
    appendPermanent(new_table_name, schema = write_schema)

  expect_true(nrow(dplyr::collect(x)) >= 3)

  DBI::dbRemoveTable(con, inSchema(write_schema, new_table_name, dbms(con)))
  expect_false(new_table_name %in% list_tables(con, write_schema))
}

# dbtype = "bigquery"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - compute_query"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran()
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_compute_query(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}

test_that("uniqueTableName", {
  result <- uniqueTableName()
  expect_true(startsWith(result, "dbplyr_"))
})

test_that("message does not duplicate when prefix is used", {
  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con, "main", c(prefix = "a_", schema = "main"))
  DBI::dbWriteTable(con, inSchema(attr(cdm, "write_schema"), "cars"), cars)
  expect_message(dropTable(cdm, "cars", verbose = TRUE))
  DBI::dbDisconnect(con, shutdown = TRUE)
})
