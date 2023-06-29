
test_compute_query <- function(con, cdm_schema, write_schema) {

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
  concept <- dplyr::tbl(con, "concept")

  q <- concept %>%
    dplyr::filter(domain_id == "Drug") %>%
    dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
    dplyr::count(isRxnorm)

  x <- dplyr::compute(q)
  expect_true(nrow(dplyr::collect(x)) == 2)

  x <- computeQuery(q, "rxnorm_count")
  expect_true(nrow(dplyr::collect(x)) == 2)
  expect_true("rxnorm_count" %in% DBI::dbListTables(con))

  x <- computeQuery(q, "rxnorm_count", temporary = FALSE, schema = "main", overwrite = TRUE)
  expect_true(nrow(dplyr::collect(x)) == 2)
  expect_true("rxnorm_count" %in% DBI::dbListTables(con))

  x <- appendPermanent(q, "rxnorm_count")
  expect_true(nrow(dplyr::collect(x)) == 4)

  DBI::dbRemoveTable(con, "rxnorm_count")
  DBI::dbDisconnect(con, shutdown = TRUE)


  # temp table creation
  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    computeQuery()

  expect_true(nrow(dplyr::collect(x)) == 2)

  # permanent table creation
  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    computeQuery(newTableName, schema = tempSchema, temporary = FALSE)

  expect_true(nrow(dplyr::collect(x)) == 2)
  expect_true(newTableName %in% CDMConnector::listTables(con, tempSchema))

  expect_error({vocab %>%
      dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
      computeQuery(newTableName, schema = tempSchema, temporary = FALSE)}, "already exists")

  expect_error({x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    computeQuery(newTableName, schema = tempSchema, temporary = FALSE, overwrite = TRUE)}, NA)

  expect_true(nrow(dplyr::collect(x)) == 2)

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
    appendPermanent(newTableName, schema = tempSchema)

  expect_true(nrow(dplyr::collect(x)) == 3)

  DBI::dbRemoveTable(con, DBI::SQL(paste0(c(tempSchema, newTableName), collapse = ".")))
  expect_false(newTableName %in% CDMConnector::listTables(con, tempSchema))

  vocab <- dplyr::tbl(con, dbplyr::in_catalog("cdmv54", "dbo", "vocabulary"))

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    dplyr::compute()

  expect_true(nrow(dplyr::collect(x)) == 2)

  newTableName <- paste0(c("temptable", sample(1:9, 7, replace = TRUE)), collapse = "")

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    computeQuery(newTableName)

  expect_true(nrow(dplyr::collect(x)) == 2)

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    computeQuery(newTableName, schema = tempSchema, temporary = FALSE)

  expect_true(nrow(dplyr::collect(x)) == 2)
  expect_true(newTableName %in% CDMConnector::listTables(con, tempSchema))

  expect_error({vocab %>%
      dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
      computeQuery(newTableName, schema = tempSchema, temporary = FALSE)}, "already exists")

  expect_error({x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("ATC", "CPT4")) %>%
    computeQuery(newTableName, schema = tempSchema, temporary = FALSE, overwrite = TRUE)}, NA)

  expect_true(nrow(dplyr::collect(x)) == 2)

  x <- vocab %>%
    dplyr::filter(vocabulary_id %in% c("RxNorm")) %>%
    appendPermanent(newTableName, schema = tempSchema)

  expect_true(nrow(dplyr::collect(x)) == 3)

  DBI::dbRemoveTable(con, DBI::SQL(paste0(c(tempSchema, newTableName), collapse = ".")))
  expect_false(newTableName %in% CDMConnector::listTables(con, tempSchema))

  # create two temporary tables in the remote database from a query with a common prefix
  cdm$tmp_table <- cdm$concept %>%
    dplyr::count(domain_id == "Drug") %>%
    computeQuery("tmp_table", temporary = FALSE, schema = "main")

  cdm$tmp_table2 <- cdm$concept %>%
    dplyr::count(domain_id == "Condition") %>%
    computeQuery("tmp_table2", temporary = FALSE, schema = "main")

  expect_length(stringr::str_subset(DBI::dbListTables(con), "tmp"), 2)
  expect_length(stringr::str_subset(names(cdm), "tmp"), 2)

  # drop tables with a common prefix
  cdm <- dropTable(cdm, name = dplyr::starts_with("tmp"))

  expect_length(stringr::str_subset(DBI::dbListTables(con), "tmp"), 0)
  expect_length(stringr::str_subset(names(cdm), "tmp"), 0)

  newTableName <- paste0(c("temptable", sample(1:9, 7, replace = T)), collapse = "")

  cdm <- cdm_from_con(con, "cdmv531", write_schema = "ohdsi")

  # create a temporary table in the remote database from a query
  cdm$tmp_table <- cdm$vocabulary %>%
    dplyr::count(vocabulary_reference) %>%
    computeQuery("tmp_table",
                 temporary = FALSE,
                 schema = attr(cdm, "write_schema"),
                 overwrite = TRUE)

  expect_true("tmp_table" %in% DBI::dbListTables(con))
  expect_true("tmp_table" %in% names(cdm))

  cdm <- dropTable(cdm, "tmp_table")

  expect_false("tmp_table" %in% DBI::dbListTables(con))
  expect_false("tmp_table" %in% names(cdm))
}





test_that("uniqueTableName", {
  result <- uniqueTableName()
  expect_true(startsWith(result, "dbplyr_"))
})
