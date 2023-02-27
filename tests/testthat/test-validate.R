test_that("validate cdm works", {
  skip_if_not_installed("duckdb", "0.6")
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con, cdm_tables = c("person", "observation_period"))
  expect_output(validate_cdm(cdm))

  cdm <- cdm_from_con(con)
  expect_output(validate_cdm(cdm))

  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("assert_tables works", {
  skip_if_not_installed("duckdb", "0.6")
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())

  expect_error(assertTables(cdm_from_con(con), tables = c("person")), NA)

  expect_error(assertTables(cdm_from_con(con, cdm_tables = "drug_era"), tables = c("person")), "not found")

  # add missing table error to collection
  err <- checkmate::makeAssertCollection()
  expect_error(assertTables(cdm_from_con(con, cdm_tables = "drug_era"), tables = c("person"), add = err), NA)
  expect_length(err$getMessages(), 1)

  # add both missing table and empty table to assert collection
  err2 <- checkmate::makeAssertCollection()
  invisible(DBI::dbExecute(con, "delete from condition_era"))
  expect_error(assertTables(cdm_from_con(con, cdm_tables = "condition_era"), tables = c("person", "condition_era"), add = err2), NA)
  expect_length(err2$getMessages(), 2) # one table is missing and the other is empty

  # add missing column error to collection
  err3 <- checkmate::makeAssertCollection()
  cdm <- cdm_from_con(con, cdm_tables = "person")
  cdm$person <- cdm$person %>%
    dplyr::select(-person_id)
  expect_error(assertTables(cdm = cdm, tables = c("person"), add = err3), NA)
  expect_length(err3$getMessages(), 1) # missing column
  expect_error(assertTables(cdm = cdm, tables = c("person"), add = NULL)) # without error collection

  # use in a function (i.e. the typical use case)
  countDrugsByGender <- function(cdm) {
    assertTables(cdm, tables = c("person", "drug_era"), empty.ok = FALSE)

    cdm$person %>%
      dplyr::inner_join(cdm$drug_era, by = "person_id") %>%
      dplyr::count(.data$gender_concept_id, .data$drug_concept_id) %>%
      dplyr::collect()
  }

  expect_error(countDrugsByGender(cdm_from_con(con)), NA)
  expect_error(countDrugsByGender(cdm_from_con(con, cdm_tables = "person")), "not found")

  DBI::dbExecute(con, "delete from drug_era")
  expect_error(countDrugsByGender(cdm_from_con(con)), "empty")

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("assert_write_schema", {
  skip_if_not_installed("duckdb", "0.6")
  skip_if_not(eunomia_is_available())

  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  cdm <- cdm_from_con(con, cdm_tables = c("person"), write_schema = "main")

  result <- assert_write_schema(cdm)
  expect_equal(result, cdm)

  # check invalid input
  expect_error(assert_write_schema(cdm = NULL))
  attr(cdm, "dbcon") <- NULL
  expect_error(assert_write_schema(cdm = cdm))

  DBI::dbDisconnect(con, shutdown = TRUE)
})
