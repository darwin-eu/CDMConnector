





test_insert_table <- function(cdm, cdm_schema, write_schema) {
  name <- paste0(c(sample(letters, 5, replace = TRUE), "_test_table"), collapse = "")
  table <- datasets::cars |>
    dplyr::arrange(dplyr::across(c("speed", "dist")))

  cdm <- cdmFromCon(con, cdmSchema = cdm_schema, cdmName = "test", writeSchema = write_schema)

  tab <- insertTable(cdm = cdm, name = name, table = table)
}


dbtype = "spark"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - cdmFromCon"), {
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
