library(testthat)
library(dplyr, warn.conflicts = FALSE)

### CDM object DBI drivers ------
test_cdm_from_con <- function(con, cdm_schema, write_schema) {
  cdm <- cdm_from_con(con, cdm_schema = cdm_schema, cdm_name = "test")
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
  x[["person"]] <- cdm[["person"]] %>% computeQuery()
  expect_true("cdm_reference" %in% names(attributes(x[["person"]])))
  cdm[["person"]] <- cdm[["person"]] %>% computeQuery()
  x <- unclass(cdm)
  expect_false("cdm_reference" %in% names(attributes(x[["person"]])))

  # simple join
  df <- dplyr::inner_join(cdm$person, cdm$observation_period, by = "person_id") %>%
    head(2) %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

}

# dbToTest <- c(
  # "duckdb"
  # ,"postgres"
  # ,"redshift"
  # ,
  # "sqlserver"
  # ,"oracle" # requires development dbplyr version to work
  # ,"snowflake"
  # ,"bigquery" # issue with bigquery tbl
# )

# dbtype = "duckdb"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - cdm_from_con"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype)
    cli::cat_rule(paste("running cdm test on ", dbtype))
    cli::cat_line(paste("DBI::dbIsValid(con):", DBI::dbIsValid(con)))
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
  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())

  for (name in list_tables(con, "main")) {
    DBI::dbExecute(con,
                   glue::glue("ALTER TABLE {name} RENAME TO {name}2;"))
    DBI::dbExecute(con,
                   glue::glue("ALTER TABLE {name}2 RENAME TO {toupper(name)};"))

  }

  expect_true(all(list_tables(con, "main") == toupper(list_tables(con, "main"))))

  # check that names in cdm are lowercase
  cdm <- cdm_from_con(con, "main")
  expect_true(all(names(cdm) == tolower(names(cdm))))

  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("adding achilles", {
  skip_if_not(eunomia_is_available())
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
  expect_error(cdm_from_con(con = con,
                      cdm_schema =  "main",
                      achilles_schema = "main"))
  DBI::dbWriteTable(con, "achilles_analysis",
                    tibble(analysis_id = 1,
                           analysis_name = 1),
                    overwrite = TRUE
  )
  DBI::dbWriteTable(con, "achilles_results",
                    tibble(analysis_id = 1,
                           stratum_1 = "a"),
                    overwrite = TRUE
  )
  DBI::dbWriteTable(con, "achilles_results_dist",
                    tibble(analysis_id = 1,
                           count_value = 5),
                    overwrite = TRUE
  )
 cdm <- cdm_from_con(con = con,
               cdm_schema =  "main",
               achilles_schema = "main")

 expect_true(cdm$achilles_analysis %>% dplyr::pull("analysis_name") == 1)
 expect_true(cdm$achilles_results %>% dplyr::pull("stratum_1") == "a")
 expect_true(cdm$achilles_results_dist %>% dplyr::pull("count_value") == 5)


 DBI::dbDisconnect(con, shutdown = TRUE)

})
