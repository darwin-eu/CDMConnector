#
# to_vector <- function(df, group_id = NULL, group_colname = 'cyl') {
#   if (group_colname %in% colnames(df)) {
#     if (is.null(group_id)) {
#       rlang::abort("argument `group_id` cannot be zero for grouped dataset")
#     }
#     df <- df %>%
#       dplyr::filter(!!as.symbol(group_colname) == group_id)
#   }
#   df <- df %>% dplyr::select(starts_with("quant") | ends_with("quant"))
#   if ('quantiles' %in% colnames(df)) {
#     df %>% dplyr::pull(quantiles)
#   } else {
#     df %>% unlist()
#   }
# }
#
# test_that("summarise-quantile works ", {
#   skip_if_not_installed("duckdb")
#   con <- DBI::dbConnect(duckdb::duckdb())
#   on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
#   mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)
#
#   # summarise-quantile works without group by
#   df1 <- mtcars_tbl %>%
#     summarise_quantile(mpg, probs = round(seq(0, 1, 0.05), 2),
#                        name_suffix = "quant") %>%
#     dplyr::collect()
#
#   df2 <- mtcars %>%
#     dplyr::reframe(quantiles = quantile(mpg, round(seq(0, 1, 0.05), 2), type = 1))
#
#   expect_true(all.equal(to_vector(df1), to_vector(df2), check.attributes = FALSE))
#
#   # summarise-quantile works without group by (single value quantile)"
#   df1 <- mtcars_tbl %>%
#     summarise_quantile(mpg, probs = 0.05, name_suffix = "quant") %>%
#     dplyr::collect()
#
#   df2 <- mtcars %>%
#     dplyr::summarise(quantiles = quantile(mpg, 0.05, type = 1))
#
#   expect_true(all.equal(to_vector(df1), to_vector(df2), check.attributes = FALSE))
#
#
#   # summarise-quantile works with select
#   df1 <- mtcars_tbl %>%
#     dplyr::select(cyl, mpg) %>%
#     summarise_quantile(mpg, probs = round(seq(0, 1, 0.05), 2),
#                        name_suffix = "quant") %>%
#     dplyr::collect()
#
#   df2 <- mtcars %>%
#     dplyr::reframe(quantiles = quantile(mpg, round(seq(0, 1, 0.05) ,2), type = 1))
#
#   expect_true(all.equal(to_vector(df1), to_vector(df2), check.attributes = FALSE))
#
#   # summarise-quantile works with mutate
#   df1 <- mtcars_tbl %>%
#     dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
#     summarise_quantile(mpg, probs = round(seq(0, 1, 0.05), 2), name_suffix = "quant") %>%
#     dplyr::collect()
#
#   df2 <- mtcars %>%
#     dplyr::reframe(mean = mean(mpg, na.rm = TRUE),
#                    quantiles = quantile(mpg, round(seq(0, 1, 0.05), 2), type = 1))
#
#   expect_true(all.equal(to_vector(df1), to_vector(df2), check.attributes = FALSE))
#   expect_equal(df1$mean, unique(df2$mean))
#
#
#   # summarise-quantile works with select + mutate", {
#   df1 <- mtcars_tbl %>%
#     dplyr::select(cyl, mpg) %>%
#     dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
#     summarise_quantile(mpg, probs = round(seq(0, 1, 0.05), 2), name_suffix = "quant") %>%
#     dplyr::collect()
#
#   df2 <- mtcars %>%
#     dplyr::reframe(mean = mean(mpg, na.rm = TRUE),
#                    quantiles = quantile(mpg, round(seq(0, 1, 0.05), 2), type = 1))
#
#   expect_true(all.equal(to_vector(df1), to_vector(df2), check.attributes = FALSE))
#   expect_equal(df1$mean, unique(df2$mean))
# })
#
#
# test_that("summarise-quantile works with group by", {
#   skip_if_not_installed("duckdb")
#   con <- DBI::dbConnect(duckdb::duckdb())
#   on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
#   mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)
#
#   df1 <- mtcars_tbl %>%
#     dplyr::group_by(cyl) %>%
#     summarise_quantile(mpg, probs = round(seq(0, 1, 0.05), 2), name_suffix = "quant") %>%
#     dplyr::collect()
#
#   df2 <- mtcars %>%
#     dplyr::group_by(cyl) %>%
#     dplyr::reframe(quantiles = quantile(mpg, round(seq(0, 1, 0.05), 2), type = 1), .groups = 'drop')
#
#   for (n in unique(df1$cyl)) {
#     expect_true(all.equal(to_vector(df1, n), to_vector(df2, n), check.attributes = FALSE))
#   }
# })
#
# test_that("summarise-quantile works with select + group by", {
#   skip_if_not_installed("duckdb")
#   con <- DBI::dbConnect(duckdb::duckdb())
#   on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
#   mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)
#
#   df1 <- mtcars_tbl %>%
#     dplyr::select(cyl, mpg) %>%
#     dplyr::group_by(cyl) %>%
#     summarise_quantile(mpg, probs = round(seq(0, 1, 0.05), 2), name_suffix = "quant") %>%
#     dplyr::collect()
#
#   df2 <- mtcars %>%
#     dplyr::select(cyl, mpg) %>%
#     dplyr::group_by(cyl) %>%
#     dplyr::reframe(quantiles = quantile(mpg, round(seq(0, 1, 0.05), 2), type = 1), .groups = 'drop')
#
#   for (n in unique(df1$cyl)) {
#     expect_true(all.equal(to_vector(df1, n), to_vector(df2, n), check.attributes = FALSE))
#   }
# })
#
#
# test_that("summarise-quantile works in combination with aggreagate functions", {
#   skip_if_not_installed("duckdb")
#   con <- DBI::dbConnect(duckdb::duckdb())
#   on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
#   mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)
#
#   df1 <- mtcars_tbl %>%
#     dplyr::group_by(cyl) %>%
#     dplyr::mutate(mean = mean(mpg, na.rm = TRUE),
#                   n = dplyr::n(),
#                   min = min(mpg, na.rm = TRUE),
#                   max = max(mpg, na.rm = TRUE)) %>%
#     summarise_quantile(mpg, probs = round(seq(0, 1, 0.05), 2), name_suffix = "quant") %>%
#     dplyr::collect()
#
#   df2 <- mtcars %>%
#     dplyr::group_by(cyl) %>%
#     dplyr::reframe(mean = mean(mpg, na.rm = TRUE),
#                    n = dplyr::n(),
#                    min = min(mpg, na.rm = TRUE),
#                    max = max(mpg, na.rm = TRUE),
#                    quantiles = quantile(mpg, round(seq(0, 1, 0.05), 2), type = 1), .groups = 'drop')
#
#   for (n in unique(df1$cyl)) {
#     expect_true(all.equal(to_vector(df1, n), to_vector(df2, n), check.attributes = FALSE))
#   }
# })
#
#
# test_that("summarise-quantile generates error when working in `summarise` context", {
#   skip_if_not_installed("duckdb")
#   con <- DBI::dbConnect(duckdb::duckdb())
#   on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
#   mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)
#
#   f <- function(){
#     df1 <- mtcars_tbl %>%
#       dplyr::group_by(cyl) %>%
#       dplyr::summarise(mean = mean(mpg, na.rm = TRUE)) %>%
#       summarise_quantile(mpg, probs = 0.05, name_suffix = "quant") %>%
#       dplyr::collect()
#     df1
#   }
#   expect_error(f())
# })
#
# test_that("summarise-quantile works with implicit (context) names", {
#   skip_if_not_installed("duckdb")
#   con <- DBI::dbConnect(duckdb::duckdb())
#   on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
#   mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)
#
#   df1 <- mtcars_tbl %>%
#     dplyr::group_by(cyl) %>%
#     dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
#     summarise_quantile(probs = 0.05, name_suffix = "quant") %>%
#     dplyr::collect()
#
#   df2 <- mtcars %>%
#     dplyr::group_by(cyl) %>%
#     dplyr::summarise(mean = mean(mpg, na.rm = TRUE),
#                      quantiles = quantile(mpg, 0.05, type = 1))
#
#   for (n in unique(df1$cyl)) {
#     expect_true(all.equal(to_vector(df1, n), to_vector(df2, n), check.attributes = FALSE))
#   }
# })
#
# test_that("summarise-quantile generates error when working with conflicting names", {
#   skip_if_not_installed("duckdb")
#   con <- DBI::dbConnect(duckdb::duckdb())
#   on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
#   mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)
#
#   f <- function(){
#     df1 <- mtcars_tbl %>%
#       dplyr::group_by(cyl) %>%
#       dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
#       summarise_quantile(cyl, probs = 0.05, name_suffix = "quant") %>%
#       dplyr::collect()
#     df1
#   }
#   expect_error(f())
# })
#
# test_that("summarise-quantile generates error when no names passed", {
#   skip_if_not_installed("duckdb")
#   con <- DBI::dbConnect(duckdb::duckdb())
#   on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
#   mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)
#
#   f <- function(){
#     df1 <- mtcars_tbl %>%
#       dplyr::group_by(cyl) %>%
#       summarise_quantile(probs = 0.05, name_suffix = "quant") %>%
#       dplyr::collect()
#     df1
#   }
#   expect_error(f())
# })
#
#
# test_that("summarise_quantile works on DuckDB", {
#   skip_if_not_installed("duckdb")
#   skip_if_not(eunomia_is_available())
#   skip("failing test")
#
#   con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#   on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
#
#   cdm <- cdm_from_con(con, cdm_schema = "main")
#
#   df1 <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::filter(!is.na(days_supply)) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::mutate(n = dplyr::n()) %>%
#     summarise_quantile(days_supply, probs = round(seq(0, 1, 0.05), 2), name_suffix = "quant")  %>%
#     dplyr::arrange(desc(n)) %>%
#     head(100) %>%
#     dplyr::collect()
#
#   df2 <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::collect()
#   df2 <- df2 %>%
#     dplyr::filter(drug_concept_id %in% unique(df1$drug_concept_id)) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::reframe(n = dplyr::n(),
#                    quantiles = quantile(days_supply, probs = round(seq(0, 1, 0.05), 2), type = 1, na.rm = TRUE),
#                    .groups = 'drop')
#
#   unique_ids <- unique(df1$drug_concept_id)
#   for (n in unique_ids) {
#     expect_true(all.equal(to_vector(df1, n, 'drug_concept_id'), to_vector(df2, n, 'drug_concept_id'), check.attributes = FALSE),
#                 label = paste('Result for drug_concept_id ', as.character(n)))
#   }
# })
#
#
# test_that("summarise_quantile works on Postgres", {
#   skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
#   con <- DBI::dbConnect(RPostgres::Postgres(),
#                         dbname =   Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
#                         host =     Sys.getenv("CDM5_POSTGRESQL_HOST"),
#                         user =     Sys.getenv("CDM5_POSTGRESQL_USER"),
#                         password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
#
#   cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
#   df1 <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::filter(!is.na(days_supply)) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::mutate(n = dplyr::n()) %>%
#     summarise_quantile(days_supply, probs = round(seq(0, 1, 0.05), 2), name_suffix = "quant")  %>%
#     dplyr::arrange(desc(n)) %>%
#     head(100) %>%
#     dplyr::collect()
#
#   df2 <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::collect()
#   df2 <- df2 %>%
#     dplyr::filter(drug_concept_id %in% unique(df1$drug_concept_id)) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::reframe(n = dplyr::n(),
#                    quantiles = quantile(days_supply, probs = round(seq(0, 1, 0.05), 2), type = 1, na.rm = TRUE),
#                    .groups = 'drop')
#
#   unique_ids <- unique(df1$drug_concept_id)
#   for (n in unique_ids) {
#     expect_true(all.equal(to_vector(df1, n, 'drug_concept_id'), to_vector(df2, n, 'drug_concept_id'), check.attributes = FALSE),
#                 label = paste('Result for drug_concept_id ', as.character(n)))
#   }
#
#   DBI::dbDisconnect(con)
# })
#
#
# test_that("summarise_quantile works on SQL Server", {
#   skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
#   con <- DBI::dbConnect(odbc::odbc(),
#                         Driver   = Sys.getenv("SQL_SERVER_DRIVER"),
#                         Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
#                         Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
#                         UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
#                         PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
#                         TrustServerCertificate = "yes",
#                         Port     = 1433)
#
#   cdm <- cdm_from_con(con, cdm_schema = c("CDMV5", "dbo"))
#   df1 <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::filter(!is.na(days_supply)) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::mutate(n = dplyr::n()) %>%
#     summarise_quantile(days_supply, probs = round(seq(0, 1, 0.05), 2), name_suffix = "quant")  %>%
#     dplyr::arrange(desc(n)) %>%
#     head(100) %>%
#     dplyr::collect()
#
#   df2 <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::collect()
#   df2 <- df2 %>%
#     dplyr::filter(drug_concept_id %in% unique(df1$drug_concept_id)) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::reframe(n = dplyr::n(),
#                    quantiles = quantile(days_supply, probs = round(seq(0, 1, 0.05), 2), type = 1, na.rm = TRUE),
#                    .groups = 'drop')
#
#   unique_ids <- unique(df1$drug_concept_id)
#   for (n in unique_ids) {
#     expect_true(all.equal(to_vector(df1, n, 'drug_concept_id'), to_vector(df2, n, 'drug_concept_id'), check.attributes = FALSE),
#                 label = paste('Result for drug_concept_id ', as.character(n)))
#   }
#
#   DBI::dbDisconnect(con)
# })
#
#
# test_that("summarise_quantile works on Redshift", {
#   skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")
#   skip("failing test")
#   con <- DBI::dbConnect(RPostgres::Redshift(),
#                         dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
#                         host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
#                         port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
#                         user     = Sys.getenv("CDM5_REDSHIFT_USER"),
#                         password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
#
#   cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))
#   df1 <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::filter(!is.na(days_supply)) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::mutate(n = dplyr::n()) %>%
#     summarise_quantile(days_supply, probs = round(seq(0, 1, 0.05), 2), name_suffix = "quant")  %>%
#     dplyr::arrange(desc(n)) %>%
#     head(100) %>%
#     dplyr::collect()
#
#   df2 <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::collect()
#   df2 <- df2 %>%
#     dplyr::filter(drug_concept_id %in% unique(df1$drug_concept_id)) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::reframe(n = dplyr::n(),
#                    quantiles = quantile(days_supply, probs = round(seq(0, 1, 0.05), 2), type = 1, na.rm = TRUE),
#                    .groups = 'drop')
#
#   unique_ids <- unique(df1$drug_concept_id)
#   for (n in unique_ids) {
#     expect_true(all.equal(to_vector(df1, n, 'drug_concept_id'), to_vector(df2, n, 'drug_concept_id'), check.attributes = FALSE),
#                 label = paste('Result for drug_concept_id ', as.character(n)))
#   }
#
#   DBI::dbDisconnect(con)
# })
#
#
#
# # Test dbplyr quantile translation -----
# # The following test show what syntax works and does not work on each platform
#
# test_that("quantile translation works on postgres", {
#   skip_if(Sys.getenv("CDM5_POSTGRESQL_USER") == "")
#   skip("manual test")
#
#   con <- DBI::dbConnect(RPostgres::Postgres(),
#                         dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
#                         host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
#                         user = Sys.getenv("CDM5_POSTGRESQL_USER"),
#                         password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
#
#
#
#   cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"))
#
#   # fails
#   # df <- cdm$drug_exposure %>%
#   #   dplyr::select(drug_concept_id, days_supply) %>%
#   #   dplyr::group_by(drug_concept_id) %>%
#   #   dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#   #   dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#   #   dplyr::collect()
#   #
#   # expect_s3_class(df, "data.frame")
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con)
# })
#
# test_that("quantile translation works on sql server", {
#   skip_if(Sys.getenv("CDM5_SQL_SERVER_USER") == "")
#   skip("manual test")
#
#   con <- DBI::dbConnect(odbc::odbc(),
#                         Driver   = "ODBC Driver 18 for SQL Server",
#                         Server   = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
#                         Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
#                         UID      = Sys.getenv("CDM5_SQL_SERVER_USER"),
#                         PWD      = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
#                         TrustServerCertificate = "yes",
#                         Port     = 1433)
#
#   cdm <- cdm_from_con(con, cdm_schema = c("CDMV5", "dbo"))
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   # fails
#   # df <- cdm$drug_exposure %>%
#   #   dplyr::select(drug_concept_id, days_supply) %>%
#   #   dplyr::group_by(drug_concept_id) %>%
#   #   dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#   #   dplyr::collect()
#
#   # expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con)
# })
#
# test_that("quantile translation works on redshift", {
#   skip_if(Sys.getenv("CDM5_REDSHIFT_USER") == "")
#   skip("manual test")
#
#   con <- DBI::dbConnect(RPostgres::Redshift(),
#                         dbname   = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
#                         host     = Sys.getenv("CDM5_REDSHIFT_HOST"),
#                         port     = Sys.getenv("CDM5_REDSHIFT_PORT"),
#                         user     = Sys.getenv("CDM5_REDSHIFT_USER"),
#                         password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"))
#
#   cdm <- cdm_from_con(con, cdm_schema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"))
#
#   # df <- cdm$drug_exposure %>%
#   #   dplyr::select(drug_concept_id, days_supply) %>%
#   #   dplyr::group_by(drug_concept_id) %>%
#   #   dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#   #   dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#   #   dplyr::collect()
#   #
#   # expect_s3_class(df, "data.frame")
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con)
# })
#
# test_that("quantile translation works on Oracle", {
#   skip_on_ci()
#   skip_on_cran()
#   skip_if_not("OracleODBC-19" %in% odbc::odbcListDataSources()$name)
#   skip("manual test")
#
#   con <- DBI::dbConnect(odbc::odbc(), "OracleODBC-19")
#
#   cdm <- cdm_from_con(con, cdm_schema = "CDMV5")
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con)
# })
#
# test_that("quantile translation works on Spark", {
#   skip_if_not("Databricks" %in% odbc::odbcListDataSources()$name)
#   skip("manual test")
#
#   con <- DBI::dbConnect(odbc::odbc(), dsn = "Databricks", bigint = "numeric")
#
#   cdm <- cdm_from_con(con, cdm_schema = "omop531")
#
#   # df <- cdm$drug_exposure %>%
#   #   dplyr::select(drug_concept_id, days_supply) %>%
#   #   dplyr::group_by(drug_concept_id) %>%
#   #   dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#   #   dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#   #   dplyr::collect()
#   #
#   # expect_s3_class(df, "data.frame")
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con)
# })
#
# test_that("quantile translation works on duckdb", {
#   skip_if_not(rlang::is_installed("duckdb"))
#   skip_if_not(eunomia_is_available())
#   skip("manual test")
#
#   con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#
#   cdm <- cdm_from_con(con, cdm_schema = "main")
#
#   # df <- cdm$drug_exposure %>%
#   #   dplyr::select(drug_concept_id, days_supply) %>%
#   #   dplyr::group_by(drug_concept_id) %>%
#   #   dplyr::mutate(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#   #   dplyr::distinct(drug_concept_id, q05_days_supply) %>%
#   #   dplyr::collect()
#
#   # expect_s3_class(df, "data.frame")
#
#   df <- cdm$drug_exposure %>%
#     dplyr::select(drug_concept_id, days_supply) %>%
#     dplyr::group_by(drug_concept_id) %>%
#     dplyr::summarise(q05_days_supply = quantile(.data$days_supply, 0.05, na.rm = T)) %>%
#     dplyr::collect()
#
#   expect_s3_class(df, "data.frame")
#
#   DBI::dbDisconnect(con, shutdown = TRUE)
# })
#
#
#
