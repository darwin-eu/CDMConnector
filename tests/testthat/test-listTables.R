test_that("listTables gives error with bad input", {

  sim_con <- dbplyr::simulate_dbi("sap_hana")
  expect_error(listTables(sim_con))
  expect_error(listTables(sim_con, schema = "schema"), "not supported")

})


test_that("listTables works with duckdb schemas", {

  con <- DBI::dbConnect(duckdb::duckdb())

  DBI::dbWriteTable(con, "cars", cars)

  expect_equal(listTables(con), "cars")
  expect_equal(listTables(con, schema = "main"), "cars")

  # add table to schema in duckdb
  DBI::dbExecute(con, "create schema test")
  DBI::dbWriteTable(con, DBI::Id(schema = "test", table = "cars2"), cars)
  expect_equal(listTables(con, schema = "test"), "cars2")
})
