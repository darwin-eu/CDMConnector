library(testthat)
library(dplyr, warn.conflicts = FALSE)

### CDM object DBI drivers ------
test_cdm_from_con <- function(con, cdm_schema, write_schema) {
  expect_error(cdm_from_con(con, cdm_schema = cdm_schema, cdm_name = "test"), "write_schema")

  cdm <- cdm_from_con(con, cdm_schema = cdm_schema, cdm_name = "test", write_schema = write_schema)
  expect_s3_class(cdm, "cdm_reference")
  expect_error(assert_tables(cdm, "person"), NA)
  expect_true(version(cdm) %in% c("5.3", "5.4"))
  expect_s3_class(snapshot(cdm), "data.frame")
  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")


  cdm <- cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
  expect_s3_class(cdm, "cdm_reference")
  expect_error(assert_write_schema(cdm), NA)
  expect_true("concept" %in% names(cdm))
  expect_s3_class(collect(head(cdm$concept)), "data.frame")
  expect_equal(dbms(cdm), dbms(attr(cdm, "dbcon")))

  # check cdm_reference attribute
  expect_true("cdm_reference" %in% names(attributes(cdm[["person"]])))
  x <- unclass(cdm)
  expect_false("cdm_reference" %in% names(attributes(x[["person"]])))
  x[["person"]] <- cdm[["person"]] %>% compute()
  expect_true("cdm_reference" %in% names(attributes(x[["person"]])))
  cdm[["person"]] <- cdm[["person"]] %>% compute()
  x <- unclass(cdm)
  expect_false("cdm_reference" %in% names(attributes(x[["person"]])))

  # simple join
  df <- dplyr::inner_join(cdm$person, cdm$observation_period, by = "person_id") %>%
    head(2) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

}
dbtype = "spark"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - cdm_from_con"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_cdm_from_con(con, cdm_schema = cdm_schema, write_schema = write_schema)
    disconnect(con)
  })
}

test_that("Uppercase tables are stored as lowercase in cdm", {
  skip_if_not_installed("duckdb")
  skip_if_not(eunomia_is_available())
  # create a test cdm with upppercase table names
  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))

  for (name in list_tables(con, "main")) {
    DBI::dbExecute(con,
                   glue::glue("ALTER TABLE {name} RENAME TO {name}2;"))
    DBI::dbExecute(con,
                   glue::glue("ALTER TABLE {name}2 RENAME TO {toupper(name)};"))

  }

  expect_true(all(list_tables(con, "main") == toupper(list_tables(con, "main"))))

  # check that names in cdm are lowercase
  cdm <- cdm_from_con(con = con, cdm_name = "eunomia", cdm_schema = "main", write_schema = "main")
  expect_true(all(names(cdm) == tolower(names(cdm))))

  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("adding achilles", {
  skip_if_not(eunomia_is_available())
  skip_if_not_installed("duckdb")

  con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))

  expect_error(cdm_from_con(con = con,
                            cdm_schema =  "main",
                            write_schema = "main",
                            achilles_schema = "main"))

  DBI::dbWriteTable(
    conn = con,
    name = "achilles_analysis",
    value = tibble(
      analysis_id = 1, analysis_name = 1, stratum_1_name = 1,
      stratum_2_name = 1, stratum_3_name = 1, stratum_4_name = 1,
      stratum_5_name = 1, is_default = 1, category = 1),
    overwrite = TRUE)

  DBI::dbWriteTable(
    conn = con,
    name = "achilles_results",
    value = tibble(analysis_id = 1,
                   stratum_1 = "a",
                   stratum_2 = "b",
                   stratum_3 = 1,
                   stratum_4 = 5,
                   stratum_5 = "u",
                   count_value = 1500),
    overwrite = TRUE
  )

  DBI::dbWriteTable(
    conn = con,
    name = "achilles_results_dist",
    value = tibble(
      analysis_id = 1, count_value = 5, stratum_1 = 1, stratum_2 = 1,
      stratum_3 = 1, stratum_4 = 1, stratum_5 = 1, min_value = 1, max_value = 1,
      avg_value = 1, stdev_value = 1, median_value = 1, p10_value = 1,
      p25_value = 1, p75_value = 1, p90_value = 1),
    overwrite = TRUE
  )

  cdm <- cdm_from_con(con = con,
                      cdm_schema =  "main",
                      write_schema = "main",
                      achilles_schema = "main")

 expect_true(cdm$achilles_analysis %>% dplyr::pull("analysis_name") == 1)
 expect_true(cdm$achilles_results %>% dplyr::pull("stratum_1") == "a")
 expect_true(cdm$achilles_results_dist %>% dplyr::pull("count_value") == 5)

 # we should also be able to add achilles tables manually if in db
 cdm <- cdm_from_con(
   con = con, cdm_name = "eunomia", cdm_schema =  "main", write_schema = "main"
 )
 cdm$achilles_analysis <- dplyr::tbl(con, "achilles_analysis")
 # but should not work if tables are not in db (as cdm is db side)
 expect_error(
 cdm$achilles_analysis <- dplyr::tibble(
   analysis_id = 1, analysis_name = 1, stratum_1_name = 1,
   stratum_2_name = 1, stratum_3_name = 1, stratum_4_name = 1,
   stratum_5_name = 1, is_default = 1, category = 1
 ))
# if local tables, insert table would take care of this

 achilles_analysis_tibble <- dplyr::tibble(
   analysis_id = 1, analysis_name = 1, stratum_1_name = 1,
   stratum_2_name = 1, stratum_3_name = 1, stratum_4_name = 1,
   stratum_5_name = 1, is_default = 1, category = 1
 )
 cdm <- omopgenerics::insertTable(cdm = cdm,
                                                    table = achilles_analysis_tibble,
                                                    name = "achilles_analysis",
                                                    overwrite = TRUE)

 cdm$achilles_analysis <- dplyr::tbl(con, "achilles_analysis")
 # but should not work if tables are not in db (as cdm is db side)
  expect_error(
    cdm$achilles_analysis <- dplyr::tibble(
      analysis_id = 1, analysis_name = 1, stratum_1_name = 1,
      stratum_2_name = 1, stratum_3_name = 1, stratum_4_name = 1,
      stratum_5_name = 1, is_default = 1, category = 1
  ))

 # if local tables, insert table would take care of this
  achilles_analysis_tibble <- dplyr::tibble(
    analysis_id = 1, analysis_name = 1, stratum_1_name = 1,
    stratum_2_name = 1, stratum_3_name = 1, stratum_4_name = 1,
    stratum_5_name = 1, is_default = 1, category = 1
  )

  cdm <- omopgenerics::insertTable(cdm = cdm,
                                    table = achilles_analysis_tibble,
                                    name = "achilles_analysis",
                                    overwrite = TRUE)

  DBI::dbDisconnect(con, shutdown = TRUE)
})

test_that("adding achilles", {
  skip_if_not(eunomia_is_available())
  skip_if_not_installed("duckdb")

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())

  cohorts <- data.frame(
    cohortId = c(1, 2, 3),
    cohortName = c("X", "A", "B"),
    type = c("target", "event", "event")
  )

  cohort_table <- dplyr::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date,    ~cohort_end_date,
    1,                     5,           as.Date("2020-01-01"), as.Date("2020-01-01"),
    2,                     5,           as.Date("2020-01-10"), as.Date("2020-03-10")
  )

  dplyr::copy_to(dest = con,
                 df = cohort_table,
                 name = "test_cohort_table",
                 overwrite = TRUE)

  expect_error(cdmFromCon(con,
                    cdmSchema = "main",
                    writeSchema = c(schema = "main"),
                    cohortTables = "test_cohort_table",
                    .softValidation = FALSE)) # error because cohorts out of obs

  expect_no_error(cdmFromCon(con,
                             cdmSchema = "main",
                             writeSchema = c(schema = "main"),
                             cohortTables = "test_cohort_table",
                             .softValidation = TRUE)) # passes without validation

  DBI::dbDisconnect(con, shutdown = TRUE)
})
