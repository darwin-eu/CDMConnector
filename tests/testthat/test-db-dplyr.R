# this test file tests basic dplyr verbs and expressions within

test_dplyr <- function(con, cdm_schema, write_schema) {

  cdm <- cdm_from_con(
    con = con, cdm_name = "test", cdm_schema = cdm_schema,
    write_schema = write_schema
  )
  penguinsTbl <- omopgenerics::uniqueTableName(prefix = "penguins_")
  penguinsDf <- palmerpenguins::penguins
  cdm <- insertTable(cdm,
                     name = penguinsTbl,
                     table = penguinsDf,
                     overwrite = TRUE)

  # collect and compute (to temp or permanent)
  expect_equal(penguinsDf,
               cdm[[penguinsTbl]] |>
              dplyr::collect())
  expect_equal(penguinsDf,
               cdm[[penguinsTbl]] |>
               dplyr::compute(temporary = TRUE) |>
               dplyr::collect())
  newTbl <- omopgenerics::uniqueTableName()
  expect_equal(penguinsDf,
               cdm[[penguinsTbl]] |>
                    dplyr::compute(temporary = FALSE,
                                   name = newTbl) |>
                 dplyr::collect())
  dropTable(cdm = cdm, name = newTbl)

  # count records
  expect_equal(penguinsDf |>
                 dplyr::tally(),
    cdm[[penguinsTbl]] |>
    dplyr::tally()|>
      dplyr::collect())
  expect_equal(penguinsDf |>
                 dplyr::count(),
               cdm[[penguinsTbl]] |>
                 dplyr::count()|>
                 dplyr::collect())
  expect_equal(penguinsDf |>
                 dplyr::summarise(n = dplyr::n()),
               cdm[[penguinsTbl]] |>
                 dplyr::summarise(n = n()) |>
                 dplyr::collect())


  # filter
  expect_equal(penguinsDf |>
                 dplyr::filter(species == "Adelie"),
               cdm[[penguinsTbl]] |>
                    dplyr::filter(species == "Adelie") |>
                    dplyr::collect())
  # mutate
  expect_equal(penguinsDf |>
                 dplyr::mutate(new_variable = "a"),
               cdm[[penguinsTbl]] |>
                    dplyr::mutate(new_variable = "a") |>
                    dplyr::collect())
  # select
  expect_equal(penguinsDf |>
                 dplyr::select("species"),
               cdm[[penguinsTbl]] |>
                    dplyr::select("species") |>
                    dplyr::collect())

  # count distinct records
  expect_equal(penguinsDf  |>
                 dplyr::distinct() |>
                 dplyr::tally(),
               cdm[[penguinsTbl]] |>
                    dplyr::distinct() |>
                    dplyr::tally()|>
                    dplyr::collect())
  expect_equal(penguinsDf  |>
                 dplyr::distinct() |>
                 dplyr::count(),
               cdm[[penguinsTbl]] |>
                 dplyr::distinct() |>
                 dplyr::count()|>
                 dplyr::collect())
  expect_equal(penguinsDf  |>
                 dplyr::distinct() |>
                 dplyr::summarise(n = dplyr::n()),
               cdm[[penguinsTbl]] |>
                 dplyr::distinct() |>
                 dplyr::summarise(n = dplyr::n()) |>
                 dplyr::collect())

  dropTable(cdm = cdm, name = penguinsTbl)

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



