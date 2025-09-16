# this test file tests basic dplyr verbs and expressions within
penguinsDf <- dplyr::tibble(
  bill_length_mm = c(39.1,39.5,40.3,NA,36.7,39.3,38.9,39.2,34.1,42,37.8,37.8,41.1,38.6,34.6,36.6,38.7,42.5,34.4,46,37.8,37.7,35.9,38.2,38.8,35.3,
                     40.6,40.5,37.9,40.5,39.5,37.2,39.5,40.9,36.4,39.2,38.8,42.2,37.6,39.8,36.5,40.8,36,44.1,37,39.6,41.1,37.5,36,42.3,39.6,40.1,
                     35,42,34.5,41.4,39,40.6,36.5,37.6,35.7,41.3,37.6,41.1,36.4,41.6,35.5,41.1,35.9,41.8,33.5,39.7,39.6,45.8,35.5,42.8,40.9,37.2,
                     36.2,42.1,34.6,42.9,36.7,35.1,37.3,41.3,36.3,36.9,38.3,38.9,35.7,41.1,34,39.6,36.2,40.8,38.1,40.3,33.1,43.2),
  bill_depth_mm  = c(18.7,17.4,18,NA,19.3,20.6,17.8,19.6,18.1,20.2,17.1,17.3,17.6,21.2,21.1,17.8,19,20.7,18.4,21.5,18.3,18.7,19.2,18.1,17.2,18.9,
                     18.6,17.9,18.6,18.9,16.7,18.1,17.8,18.9,17,21.1,20,18.5,19.3,19.1,18,18.4,18.5,19.7,16.9,18.8,19,18.9,17.9,21.2,17.7,18.9,17.9,
                     19.5,18.1,18.6,17.5,18.8,16.6,19.1,16.9,21.1,17,18.2,17.1,18,16.2,19.1,16.6,19.4,19,18.4,17.2,18.9,17.5,18.5,16.8,19.4,16.1,
                     19.1,17.2,17.6,18.8,19.4,17.8,20.3,19.5,18.6,19.2,18.8,18,18.1,17.1,18.1,17.3,18.9,18.6,18.5,16.1,18.5),
  flipper_length_mm = c(181,186,195,NA,193,190,181,195,193,190,186,180,182,191,198,185,195,197,184,194,174,180,189,185,180,187,183,187,172,180,178,
                        178,188,184,195,196,190,180,181,184,182,195,186,196,185,190,182,179,190,191,186,188,190,200,187,191,186,193,181,194,185,195,
                        185,192,184,192,195,188,190,198,190,190,196,197,190,195,191,184,187,195,189,196,187,193,191,194,190,189,189,190,202,205,185,
                        186,187,208,190,196,178,192),
  body_mass_g    = c(3750,3800,3250,NA,3450,3650,3625,4675,3475,4250,3300,3700,3200,3800,4400,3700,3450,4500,3325,4200,3400,3600,3800,3950,3800,3800,
                     3550,3200,3150,3950,3250,3900,3300,3900,3325,4150,3950,3550,3300,4650,3150,3900,3100,4400,3000,4600,3425,2975,3450,4150,3500,4300,
                     3450,4050,2900,3700,3550,3800,2850,3750,3150,4400,3600,4050,2850,3950,3350,4100,3050,4450,3600,3900,3550,4150,3700,4250,3700,3900,
                     3550,4000,3200,4700,3800,4200,3350,3550,3800,3500,3950,3600,3550,4300,3400,4450,3300,4300,3700,4350,2900,4100),
  year           = c(rep(2007,50), rep(2008,50)),
  species        = factor(rep("Adelie", 100)),
  island         = factor(c(rep("Torgersen",20), rep("Biscoe",10), rep("Dream",20), rep("Biscoe",20), rep("Torgersen",20), rep("Dream",10))),
  sex            = factor(c("male","female","female",NA,"female","male","female","male",NA,NA,NA,NA,"female","male","male","female","female","male",
                            "female","male","female","male","female","male","male","female","male","female","female","male","female","male","female",
                            "male","female","male","male","female","female","male","female","male","female","male","female","male","male",NA,"female",
                            "male","female","male","female","male","female","male","female","male","female","male","female","male","female","male",
                            "female","male","female","male","female","male","female","male","female","male","female","male","female","male","female",
                            "male","female","male","female","male","male","female","male","female","female","male","female","male","female","male",
                            "female","male","female","male","female", "male"))
)


test_dplyr <- function(con, cdm_schema, write_schema) {

  cdm <- cdmFromCon(
    con = con, cdmName = "test", cdmSchema = cdm_schema,
    writeSchema = write_schema
  )
  penguinsTbl <- omopgenerics::uniqueTableName(prefix = "penguins_")
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



