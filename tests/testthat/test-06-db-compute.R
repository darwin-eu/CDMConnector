
test_compute_query <- function(con, cdm_schema, write_schema) {
  cdm <- cdmFromCon(con, cdmName = "test", cdmSchema = cdm_schema, writeSchema = write_schema)

  new_table_name <- paste0("temp", floor(10*as.numeric(Sys.time()) %% 1e7))

  # temp table creation from query ----
  q <- cdm$concept %>%
    dplyr::filter(domain_id == "Drug") %>%
    dplyr::mutate(is_rxnorm = (vocabulary_id == "RxNorm")) %>%
    dplyr::count(.data$is_rxnorm)

  x <- compute(q,
               name = new_table_name,
               temporary = TRUE,
               overwrite = FALSE)

  expect_s3_class(dplyr::collect(x), "data.frame")

  expect_error({
    compute(q,
            name = new_table_name,
            temporary = TRUE,
            overwrite = FALSE)
  })

  expect_s3_class(dplyr::collect(x), "data.frame")

  # test overwrite
    x <- compute(q,
                 name = new_table_name,
                 schema = write_schema,
                 temporary = TRUE,
                 overwrite = TRUE)

  expect_s3_class(dplyr::collect(x), "data.frame")

  # TODO: test removal of temp tables. Also need to be able to get temp table names.

  # permanent table creation from query ----
  new_table_name <- paste0("temp", floor(10*as.numeric(Sys.time()) %% 1e7))
  x <- cdm$vocabulary %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    compute(name = new_table_name, temporary = FALSE, overwrite = TRUE)

  expect_true(nrow(dplyr::collect(x)) == 2)
  expect_true(new_table_name %in% listTables(con, write_schema))

  expect_error({
    cdm$vocabulary  %>%
      dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
      compute(new_table_name, temporary = FALSE, overwrite = FALSE)},
  "already exists")

  expect_no_error({
    x <- cdm$vocabulary  %>%
      dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
      compute(new_table_name, temporary = FALSE, overwrite = TRUE)
  })

  expect_true(nrow(dplyr::collect(x)) >= 2)

  x <- cdm$vocabulary %>%
    dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
    appendPermanent(new_table_name, schema = write_schema)

  expect_true(nrow(dplyr::collect(x)) >= 3)

  DBI::dbRemoveTable(con, inSchema(write_schema, new_table_name, dbms(con)))
  expect_false(new_table_name %in% listTables(con, write_schema))
}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - computeQuery"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
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
  expect_true(startsWith(result, "og_"))
})

test_that("message does not duplicate when prefix is used", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb( eunomiaDir()))
  cdm <- cdmFromCon(con, cdmName = "test", cdmSchema = "main", writeSchema = c(prefix = "a_", schema = "main"))
  DBI::dbWriteTable(con, inSchema(attr(attr(cdm, "cdm_source"), "write_schema"), "cars"), cars)
  expect_no_error(dropTable(cdm, "cars"))
  DBI::dbDisconnect(con, shutdown = TRUE)
})
