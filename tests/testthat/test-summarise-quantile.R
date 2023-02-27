con <- DBI::dbConnect(duckdb::duckdb())
mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)

test_that("summarise-quantile works without group by", {
  df1 <- mtcars_tbl %>%
    summarise_quantile(mpg, probs = c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                      0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                       name_suffix = "value") %>%
    dplyr::collect()

  df2 <- mtcars %>%
    dplyr::summarise(quantiles = quantile(mpg, c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                                   0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                                            type = 1))

  a <- df1 %>% unlist()
  b <- df2 %>% dplyr::pull()
  expect_true(all.equal(a, b, check.attributes = FALSE))
})

test_that("summarise-quantile works without group by (single value quantile)", {
  df1 <- mtcars_tbl %>%
    summarise_quantile(mpg, probs = 0.05, name_suffix = "value") %>%
    dplyr::collect()

  df2 <- mtcars %>%
    dplyr::summarise(quantiles = quantile(mpg, 0.05, type = 1))

  a <- df1 %>% unlist()
  b <- df2 %>% dplyr::pull()
  expect_true(all.equal(a, b, check.attributes = FALSE))
})


test_that("summarise-quantile works with select", {
  df1 <- mtcars_tbl %>%
    dplyr::select(cyl, mpg) %>%
    summarise_quantile(mpg, probs = c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                      0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                       name_suffix = "value") %>%
    dplyr::collect()

  df2 <- mtcars %>%
    dplyr::summarise(quantiles = quantile(mpg, c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                                 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                                          type = 1))

  a <- df1 %>% unlist()
  b <- df2 %>% dplyr::pull()
  expect_true(all.equal(a, b, check.attributes = FALSE))
})


test_that("summarise-quantile works with mutate", {
  df1 <- mtcars_tbl %>%
    dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
    summarise_quantile(mpg, probs = c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                      0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                       name_suffix = "value") %>%
    dplyr::collect()

  df2 <- mtcars %>%
    dplyr::summarise(mean = mean(mpg, na.rm = TRUE),
                     quantiles = quantile(mpg, c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                                 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                                          type = 1))

  a <- df1 %>% dplyr::select(-mean) %>% unlist()
  b <- df2 %>% dplyr::pull(quantiles)
  expect_true(all.equal(a, b, check.attributes = FALSE))
  expect_equal(df1$mean, unique(df2$mean))
})


test_that("summarise-quantile works with select + mutate", {
  df1 <- mtcars_tbl %>%
    dplyr::select(cyl, mpg) %>%
    dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
    summarise_quantile(mpg, probs = c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                      0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                       name_suffix = "value") %>%
    dplyr::collect()

  df2 <- mtcars %>%
    dplyr::summarise(mean = mean(mpg, na.rm = TRUE),
                     quantiles = quantile(mpg, c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                                 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                                          type = 1))

  a <- df1 %>% dplyr::select(-mean) %>% unlist()
  b <- df2 %>% dplyr::pull(quantiles)
  expect_true(all.equal(a, b, check.attributes = FALSE))
  expect_equal(df1$mean, unique(df2$mean))
})


test_that("summarise-quantile works with group by", {
  df1 <- mtcars_tbl %>%
    dplyr::group_by(cyl) %>%
    summarise_quantile(mpg, probs = c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                      0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                       name_suffix = "value") %>%
    dplyr::collect()

  df2 <- mtcars %>%
    dplyr::group_by(cyl) %>%
    dplyr::summarise(quantiles = quantile(mpg, c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                                                 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                                                          type = 1), .groups = 'drop')

  for (c in unique(df1$cyl)) {
    a <- df1 %>% dplyr::filter(cyl == c) %>% dplyr::select(-cyl) %>% unlist()
    b <- df2 %>% dplyr::filter(cyl == c) %>% dplyr::pull(quantiles)
    expect_true(all.equal(a, b, check.attributes = FALSE))
  }
})

test_that("summarise-quantile works with select + group by", {
  df1 <- mtcars_tbl %>%
    dplyr::select(cyl, mpg) %>%
    dplyr::group_by(cyl) %>%
    summarise_quantile(mpg, probs = c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                      0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                       name_suffix = "value") %>%
    dplyr::collect()

  df2 <- mtcars %>%
    dplyr::select(cyl, mpg) %>%
    dplyr::group_by(cyl) %>%
    dplyr::summarise(quantiles = quantile(mpg, c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                                 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                                          type = 1), .groups = 'drop')

  for (c in unique(df1$cyl)) {
    a <- df1 %>% dplyr::filter(cyl == c) %>% dplyr::select(-cyl) %>% unlist()
    b <- df2 %>% dplyr::filter(cyl == c) %>% dplyr::pull(quantiles)
    expect_true(all.equal(a, b, check.attributes = FALSE))
  }
})


test_that("summarise-quantile works in combination with aggreagate functions", {
  df1 <- mtcars_tbl %>%
    dplyr::group_by(cyl) %>%
    dplyr::mutate(mean = mean(mpg, na.rm = TRUE),
                  n = dplyr::n(),
                  min = min(mpg, na.rm = TRUE),
                  max = max(mpg, na.rm = TRUE)) %>%
    summarise_quantile(mpg, probs = c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                      0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                       name_suffix = "value") %>%
    dplyr::collect()

  df2 <- mtcars %>%
    dplyr::group_by(cyl) %>%
    dplyr::summarise(mean = mean(mpg, na.rm = TRUE),
                     n = dplyr::n(),
                     min = min(mpg, na.rm = TRUE),
                     max = max(mpg, na.rm = TRUE),
                     quantiles = quantile(mpg, c(0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                                                 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1),
                                          type = 1), .groups = 'drop')

  for (c in unique(df1$cyl)) {
    a <- df1 %>% dplyr::filter(cyl == c) %>% dplyr::select(-c(cyl, mean, n, min, max)) %>% unlist()
    b <- df2 %>% dplyr::filter(cyl == c) %>% dplyr::pull(quantiles)
    expect_true(all.equal(a, b, check.attributes = FALSE))
  }
})


test_that("summarise-quantile generates error when working in `summarise` context", {
  f <- function(){
    df1 <- mtcars_tbl %>%
      dplyr::group_by(cyl) %>%
      dplyr::summarise(mean = mean(mpg, na.rm = TRUE)) %>%
      summarise_quantile(mpg, probs = 0.05, name_suffix = "value") %>%
      dplyr::collect()
    df1
  }
  expect_error(f())
})

test_that("summarise-quantile works with implicit (context) names", {
  df1 <- mtcars_tbl %>%
    dplyr::group_by(cyl) %>%
    dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
    summarise_quantile(probs = 0.05, name_suffix = "value") %>%
    dplyr::collect()

  df2 <- mtcars %>%
    dplyr::group_by(cyl) %>%
    dplyr::summarise(mean = mean(mpg, na.rm = TRUE),
                     quantiles = quantile(mpg, 0.05, type = 1))

  for (c in unique(df1$cyl)) {
    a <- df1 %>% dplyr::filter(cyl == c) %>% dplyr::select(-c(cyl, mean)) %>% unlist()
    b <- df2 %>% dplyr::filter(cyl == c) %>% dplyr::pull(quantiles)
    expect_true(all.equal(a, b, check.attributes = FALSE))
  }
})

test_that("summarise-quantile generates error when working with conflicting names", {
  f <- function(){
    df1 <- mtcars_tbl %>%
      dplyr::group_by(cyl) %>%
      dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
      summarise_quantile(cyl, probs = 0.05, name_suffix = "value") %>%
      dplyr::collect()
    df1
  }
  expect_error(f())
})

test_that("summarise-quantile generates error when no names passed", {
  f <- function(){
    df1 <- mtcars_tbl %>%
      dplyr::group_by(cyl) %>%
      summarise_quantile(probs = 0.05, name_suffix = "value") %>%
      dplyr::collect()
    df1
  }
  expect_error(f())
})

DBI::dbDisconnect(con)


test_that("`summarise_quantile` works on DuckDB", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
  cdm <- cdm_from_con(con, cdm_schema = "main", cdm_tables = "drug_exposure")
  df1 <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::filter(!is.na(days_supply)) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::mutate(n = dplyr::n()) %>%
    summarise_quantile(days_supply, probs = c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9))  %>%
    dplyr::arrange(desc(n)) %>%
    head(100) %>%
    dplyr::collect()

  df2 <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::collect()
  df2 <- df2 %>%
    dplyr::filter(drug_concept_id %in% unique(df1$drug_concept_id)) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::summarise(n = dplyr::n(),
                     quantiles = quantile(days_supply, probs = c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9), type = 1, na.rm = TRUE),
                     .groups = 'drop')

  unique_ids <-  unique(df1$drug_concept_id)
  for (c in unique_ids) {
    a <- df1 %>% dplyr::filter(drug_concept_id == c) %>% dplyr::select(-c(drug_concept_id, n)) %>% unlist()
    b <- df2 %>% dplyr::filter(drug_concept_id == c) %>% dplyr::pull(quantiles)
    expect_true(all.equal(a, b, check.attributes = FALSE), label = paste('Result for drug_concept_id ', as.character(c)))
  }

  DBI::dbDisconnect(con)
})


test_that("`summarise_quantile` works on Postgres", {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname =   Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
                        host =     Sys.getenv("CDM5_POSTGRESQL_HOST"),
                        user =     Sys.getenv("CDM5_POSTGRESQL_USER"),
                        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"), cdm_tables = "drug_exposure")
  df1 <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::filter(!is.na(days_supply)) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::mutate(n = dplyr::n()) %>%
    summarise_quantile(days_supply, probs = c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9))  %>%
    dplyr::arrange(desc(n)) %>%
    head(100) %>%
    dplyr::collect()

  df2 <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::collect()
  df2 <- df2 %>%
    dplyr::filter(drug_concept_id %in% unique(df1$drug_concept_id)) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::summarise(n = dplyr::n(),
                     quantiles = quantile(days_supply, probs = c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9), type = 1, na.rm = TRUE),
                     .groups = 'drop')

  unique_ids <-  unique(df1$drug_concept_id)
  for (c in unique_ids) {
    a <- df1 %>% dplyr::filter(drug_concept_id == c) %>% dplyr::select(-c(drug_concept_id, n)) %>% unlist()
    b <- df2 %>% dplyr::filter(drug_concept_id == c) %>% dplyr::pull(quantiles)
    expect_true(all.equal(a, b, check.attributes = FALSE), label = paste('Result for drug_concept_id ', as.character(c)))
  }

  DBI::dbDisconnect(con)
  })


test_that("`summarise_quantile` works on SQL Server", {
  skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
  con <- DBI::dbConnect(odbc::odbc(),
                                 Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
                                 Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
                                 Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
                                 UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
                                 PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                                 TrustServerCertificate = "yes",
                                 Port     = 1433)

  cdm <- cdm_from_con(con, cdm_schema = c("CDMV5", "dbo"), cdm_tables = "drug_exposure")
  df1 <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::filter(!is.na(days_supply)) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::mutate(n = dplyr::n()) %>%
    summarise_quantile(days_supply, probs = c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9))  %>%
    dplyr::arrange(desc(n)) %>%
    head(100) %>%
    dplyr::collect()

  df2 <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::collect()
  df2 <- df2 %>%
    dplyr::filter(drug_concept_id %in% unique(df1$drug_concept_id)) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::summarise(n = dplyr::n(),
              quantiles = quantile(days_supply, probs = c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9), type = 1, na.rm = TRUE),
              .groups = 'drop')

  unique_ids <-  unique(df1$drug_concept_id)
  for (c in unique_ids) {
    a <- df1 %>% dplyr::filter(drug_concept_id == c) %>% dplyr::select(-c(drug_concept_id, n)) %>% unlist()
    b <- df2 %>% dplyr::filter(drug_concept_id == c) %>% dplyr::pull(quantiles)
    expect_true(all.equal(a, b, check.attributes = FALSE), label = paste('Result for drug_concept_id ', as.character(c)))
  }

  DBI::dbDisconnect(con)
})


test_that("`summarise_quantile` works on Redshift", {
  skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")
  con <- DBI::dbConnect(RPostgres::Redshift(),
                        dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
                        host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
                        port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
                        user     = Sys.getenv("CDM5_REDSHIFT_USER"),
                        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))

  cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"), cdm_tables = "drug_exposure")
  df1 <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::filter(!is.na(days_supply)) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::mutate(n = dplyr::n()) %>%
    summarise_quantile(days_supply, probs = c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9))  %>%
    dplyr::arrange(desc(n)) %>%
    head(100) %>%
    dplyr::collect()

  df2 <- cdm$drug_exposure %>%
    dplyr::select(drug_concept_id, days_supply) %>%
    dplyr::collect()
  df2 <- df2 %>%
    dplyr::filter(drug_concept_id %in% unique(df1$drug_concept_id)) %>%
    dplyr::group_by(drug_concept_id) %>%
    dplyr::summarise(n = dplyr::n(),
                     quantiles = quantile(days_supply, probs = c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9), type = 1, na.rm = TRUE),
                     .groups = 'drop')

  unique_ids <-  unique(df1$drug_concept_id)
  for (c in unique_ids) {
    a <- df1 %>% dplyr::filter(drug_concept_id == c) %>% dplyr::select(-c(drug_concept_id, n)) %>% unlist()
    b <- df2 %>% dplyr::filter(drug_concept_id == c) %>% dplyr::pull(quantiles)
    expect_true(all.equal(a, b, check.attributes = FALSE), label = paste('Result for drug_concept_id ', as.character(c)))
  }

  DBI::dbDisconnect(con)
})
