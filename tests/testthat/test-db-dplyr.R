# this test file tests basic dplyr verbs and expressions within

test_dplyr <- function(con, cdm_schema, write_schema) {

  cdm <- cdmFromCon(
    con = con, cdmName = "test", cdmSchema = cdm_schema,
    writeSchema = write_schema
  )
  penguinsTbl <- omopgenerics::uniqueTableName(prefix = "penguins_")
  penguinsDf <- palmerpenguins::penguins
  penguinsDf <- penguinsDf %>%
    dplyr::mutate_if(is.factor, as.character) |>
    dplyr::mutate(row_id = dplyr::row_number())

  cdm <- insertTable(cdm,
                     name = penguinsTbl,
                     table = penguinsDf,
                     overwrite = TRUE)

  # collect and compute (to temp or permanent)
  expect_equal(penguinsDf  |>
                 dplyr::arrange(row_id),
               cdm[[penguinsTbl]] |>
              dplyr::collect()  |>
                dplyr::arrange(row_id))
  expect_equal(penguinsDf |>
                 dplyr::arrange(row_id),
               cdm[[penguinsTbl]] |>
               dplyr::compute(temporary = TRUE) |>
               dplyr::collect() |>
                dplyr::arrange(row_id))
  newTbl <- omopgenerics::uniqueTableName()
  expect_equal(penguinsDf  |>
                 dplyr::arrange(row_id),
               cdm[[penguinsTbl]] |>
                    dplyr::compute(temporary = FALSE,
                                   name = newTbl) |>
                 dplyr::collect()  |>
                 dplyr::arrange(row_id))
  dropSourceTable(cdm = cdm, name = newTbl)

  # count records
  expect_equal(penguinsDf |>
                 dplyr::tally() |>
                 dplyr::mutate(n = as.integer(n)),
    cdm[[penguinsTbl]] |>
    dplyr::tally()|>
      dplyr::collect() |>
      dplyr::mutate(n = as.integer(n)))
  expect_equal(penguinsDf |>
                 dplyr::count() |>
                 dplyr::mutate(n = as.integer(n)),
               cdm[[penguinsTbl]] |>
                 dplyr::count()|>
                 dplyr::collect() |>
                 dplyr::mutate(n = as.integer(n)))
  expect_equal(penguinsDf |>
                 dplyr::summarise(n = dplyr::n()) |>
                 dplyr::mutate(n = as.integer(n)),
               cdm[[penguinsTbl]] |>
                 dplyr::summarise(n = n()) |>
                 dplyr::collect() |>
                 dplyr::mutate(n = as.integer(n)))


  # filter
  expect_equal(penguinsDf |>
                 dplyr::filter(species == "Adelie")|>
                 dplyr::arrange(row_id),
               cdm[[penguinsTbl]] |>
                    dplyr::filter(species == "Adelie") |>
                    dplyr::collect()|>
                 dplyr::arrange(row_id))
  # mutate
  expect_equal(penguinsDf |>
                 dplyr::mutate(new_variable = "a")|>
                 dplyr::arrange(row_id),
               cdm[[penguinsTbl]] |>
                    dplyr::mutate(new_variable = "a") |>
                    dplyr::collect()|>
                 dplyr::arrange(row_id))
  # select
  expect_equal(sort(penguinsDf |>
                 dplyr::select("species") |>
                 dplyr::distinct() |>
                 dplyr::pull()),
               sort(cdm[[penguinsTbl]] |>
                    dplyr::select("species") |>
                 dplyr::distinct() |>
                 dplyr::pull()))

  # count distinct records
  expect_equal(penguinsDf  |>
                 dplyr::distinct() |>
                 dplyr::tally() |>
                 dplyr::mutate(n = as.integer(n)),
               cdm[[penguinsTbl]] |>
                    dplyr::distinct() |>
                    dplyr::tally()|>
                    dplyr::collect() |>
                 dplyr::mutate(n = as.integer(n)))
  expect_equal(penguinsDf  |>
                 dplyr::distinct() |>
                 dplyr::count() |>
                 dplyr::mutate(n = as.integer(n)),
               cdm[[penguinsTbl]] |>
                 dplyr::distinct() |>
                 dplyr::count()|>
                 dplyr::collect() |>
                 dplyr::mutate(n = as.integer(n)))
  expect_equal(penguinsDf  |>
                 dplyr::distinct() |>
                 dplyr::summarise(n = dplyr::n()) |>
                 dplyr::mutate(n = as.integer(n)),
               cdm[[penguinsTbl]] |>
                 dplyr::distinct() |>
                 dplyr::summarise(n = dplyr::n()) |>
                 dplyr::collect() |>
                 dplyr::mutate(n = as.integer(n)))

  dropSourceTable(cdm = cdm, name = penguinsTbl)

}

# dbtype="duckdb"
for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - dplyr queries"), {
    if (!(dbtype %in% ciTestDbs)) skip_on_ci()
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))
    test_dplyr(con, cdm_schema, write_schema)
    disconnect(con)
  })
}



