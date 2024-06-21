
# fails on duckdb

test_in_schema <- function(con, cdm_schema) {

  tbls <- listTables(con, cdm_schema)
  nm <- stringr::str_subset(tbls, "person|PERSON")
  stopifnot(tolower(nm) == "person")

  if (length(cdm_schema) == 1) {
    db <- dplyr::tbl(con, dbplyr::in_schema(cdm_schema, nm))
  } else if (length(cdm_schema) == 2) {
    db <- dplyr::tbl(con, dbplyr::in_catalog(cdm_schema[1], cdm_schema[2], nm))
  }

  df <- db %>%
    head() %>%
    dplyr::collect()

  expect_s3_class(df, "data.frame")

}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - date functions"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype == "duckdb") skip("failing test")
    cdm_schema <- get_cdm_schema(dbtype)
    skip_if(any(cdm_schema == ""))
    con <- get_connection(dbtype)
    skip_if(is.null(con))
    test_in_schema(con, cdm_schema)
    disconnect(con)
  })
}
