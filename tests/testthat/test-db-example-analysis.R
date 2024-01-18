
prepare_cdm <- function(con, write_schema) {

  # eunomia cdm
  eunomia_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
  eunomia_cdm <- cdm_from_con(eunomia_con, cdm_schema = "main") %>%
    cdm_select_tbl("person", "observation_period")

  cdm <- copyCdmTo(con = con,
                   cdm = eunomia_cdm,
                   schema = write_schema,
                   overwrite = TRUE)

  return(cdm)
}

check_counts <- function(cdm) {
  nm <- cdm$person %>%
          dplyr::count() %>%
          collect() %>%
          colnames()

  expect_true("n" %in% nm)

  nm <- cdm$person %>%
    dplyr::group_by(.data$gender_concept_id, .data$month_of_birth) %>%
    dplyr::count() %>%
    collect() %>%
    colnames()

   expect_true(all(c("gender_concept_id", "month_of_birth", "n") %in% nm))
}

check_mutate <- function(cdm) {

  expect_true(unique(cdm$person %>%
    dplyr::mutate(new_var = 1L) %>%
    dplyr::pull("new_var")) == 1L)

  expect_true(unique(cdm$person %>%
                       dplyr::mutate(new_var = 1.55) %>%
                       dplyr::pull("new_var")) == 1.55)

  expect_true(all(c("F","M") %in%
   unique(cdm$person %>%
    dplyr::mutate(new_var = dplyr::if_else(gender_concept_id == 8532,
                                           "F", "M")) %>%
    dplyr::pull("new_var"))))

 expect_no_error(cdm$observation_period %>%
    dplyr::mutate(observation_period_start_date =
                    as.Date(observation_period_start_date),
                  new_date = as.Date("2000-01-01"),
                  missing_date = as.Date(NA)))

 expect_no_error(cdm$person %>%
                   dplyr::mutate(gender_concept_id =
                                   as.integer(gender_concept_id)))

 # expect_no_error(cdm$person %>%
 #    dplyr::mutate(year_1 = as.integer(1990),
 #                  month_1 = as.integer(01),
 #                  day_1 = as.integer(01)) %>%
 #    dplyr::mutate(date_1=!!asDate(paste0(
 #      year_1, "/", month_1, "/", day_1))) %>%
 #    dplyr::select(date_1))


}

check_summarise_dplyr <- function(cdm) {

  expect_true(all(c("min",  "mean", "max") %in%
                   colnames(cdm$person %>%
                             dplyr::summarise(min = min(year_of_birth, na.rm = TRUE),
                                              mean = mean(year_of_birth, na.rm = TRUE),
                                              max = max(year_of_birth, na.rm = TRUE),
                                              .groups = "drop") %>%
                               dplyr::collect())))


  expect_true(all(c("gender_concept_id", "month_of_birth",
                    "min",  "mean", "max") %in%
  colnames(cdm$person %>%
    dplyr::group_by(gender_concept_id, month_of_birth) %>%
    dplyr::summarise(min = min(year_of_birth, na.rm = TRUE),
                     mean = mean(year_of_birth, na.rm = TRUE),
                     max = max(year_of_birth, na.rm = TRUE),
                     .groups = "drop") %>%
    dplyr::ungroup() %>%
    dplyr::collect())))

}

check_summarise_quantiles <- function(cdm){

  expect_true(all(c("p0_quant","p20_quant","p40_quant","p50_quant",
                    "p60_quant","p80_quant","p100_quant") %in%
                    colnames(
  cdm$person %>%
   summarise_quantile(year_of_birth,
                      probs = c(0, 0.2, 0.4, 0.5, 0.6, 0.8, 1),
                      name_suffix = "quant") %>%
   dplyr::collect())))

  expect_true(all(c("gender_concept_id", "month_of_birth",
                    "p0_quant","p20_quant","p40_quant","p50_quant",
                    "p60_quant","p80_quant","p100_quant") %in%
                    colnames(
  cdm$person %>%
    dplyr::group_by(gender_concept_id, month_of_birth) %>%
    summarise_quantile(year_of_birth,
                       probs = c(0, 0.2, 0.4, 0.5, 0.6, 0.8, 1),
                       name_suffix = "quant")  %>%
    dplyr::collect())))

}

check_joins <- function(cdm){

  expect_no_error(cdm$person %>%
  dplyr::left_join(cdm$observation_period,
    by = "person_id"
  ))

  expect_no_error(cdm$person %>%
    dplyr::inner_join(cdm$observation_period,
                     by = "person_id"))

    expect_no_error( cdm$person %>%
    dplyr::full_join(cdm$observation_period,
                      by = "person_id"))

    expect_no_error(cdm$person %>%
    dplyr::anti_join(cdm$observation_period,
                     by = "person_id"))

}

check_dates <- function(cdm){

  d1 <- cdm$observation_period %>%
    dplyr::mutate(date_var = !!dateadd("observation_period_start_date",
                                       1,
                                       interval = "year"
    )) %>%
    dplyr::pull("date_var")
  expect_true(inherits(d1, c("Date", "POSIXt")))

  d2 <-  cdm$observation_period %>%
    dplyr::mutate(date_var = !!dateadd("observation_period_start_date",
                                       1,
                                       interval = "day"
    )) %>%
    dplyr::pull("date_var")
  expect_true(inherits(d2, c("Date", "POSIXt")))

  d3 <- cdm$observation_period %>%
    dplyr::mutate(date_var = !!datediff("observation_period_start_date",
                                        "observation_period_end_date"
    )) %>%
    dplyr::pull("date_var")
  expect_true(is.integer(d3) || is.numeric(d3))

  d4 <- cdm$observation_period %>%
    dplyr::mutate(year = !!datepart("observation_period_start_date", "year"),
                  month = !!datepart("observation_period_start_date", "month"),
                  day = !!datepart("observation_period_start_date", "day")) %>%
    dplyr::collect()
  expect_true(is.integer(d4$year) || is.numeric(d3))
  expect_true(is.integer(d4$month) || is.numeric(d3))
  expect_true(is.integer(d4$day) || is.numeric(d3))

}

check_row_number <-  function(cdm){

  expect_no_error(cdm$person %>%
                    dplyr::filter(row_number() == 1))

  expect_no_error(cdm$person %>%
    dplyr::group_by(person_id) %>%
      dplyr::filter(row_number() == 1))

  expect_no_error(cdm$observation_period %>%
                  dbplyr::window_order(observation_period_start_date) %>%
                  dplyr::filter(row_number() == 1))

  expect_no_error(cdm$observation_period %>%
                    dplyr::group_by(person_id) %>%
                    dbplyr::window_order(observation_period_start_date) %>%
                    dplyr::filter(row_number() == 1))

}

checks <- list()

for (dbtype in dbToTest) {
  if (dbtype == "bigquery") next
  require(dplyr)
  test_that(glue::glue("{dbtype} - checking common usage"), {
    if (dbtype != "duckdb") skip_on_cran() else skip_if_not_installed("duckdb")
    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_prefix <- paste0("test", as.integer(Sys.time()), "_")
    write_schema <- get_write_schema(dbtype, prefix = write_prefix)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))

    cdm <- prepare_cdm(con, write_schema)
    check_counts(cdm)
    check_mutate(cdm)
    check_summarise_dplyr(cdm)
    check_summarise_quantiles(cdm)
    check_joins(cdm)
    check_dates(cdm)
    check_row_number(cdm)

    checks[[dbtype]] <<- dplyr::tibble(dbtype = dbtype,
                                      "dplyr::count" = 1,
                                      "dplyr::tally" = 1,
                                      "dplyr::mutate(as.integer(x))" = 1,
                                      "dplyr::mutate(as.Date(x))" = 1,
                                      "CDMConnector::asDate" = 0,
                                      "dplyr::if_else" = 1,
                                      "dplyr::summaise(min(x))" = 1,
                                      "dplyr::summaise(mean(x))" = 1,
                                      "dplyr::summaise(max(x))" = 1,
                                      "CDMConnector::summariseQuantile" = 1,
                                      "dplyr::left_join" = 1,
                                      "dplyr::right_join" = 1,
                                      "dplyr::inner_join" = 1,
                                      "dplyr::anti_join" = 1,
                                      "CDMConnector::dateadd" = 0,
                                      "CDMConnector::datediff" = 0,
                                      "CDMConnector::datepart" = 0,
                                      "dplyr::row_number"  = 1)


    rm(cdm)

    # clean up
    cdm <- cdm_from_con(con,
                        cdm_schema = cdm_schema,
                        write_schema = write_schema)

    expect_s3_class(cdm, "cdm_reference")
    CDMConnector::dropTable(cdm, dplyr::contains(write_prefix))
    disconnect(con)
  })
}
# checks <- dplyr::bind_rows(checks)
# plot_levels <- rev(colnames(checks))

# plot <- checks %>%
#   tidyr::pivot_longer(
#     cols = !dbtype,
#     names_to = "check",
#     values_to = "value",
#     cols_vary = 'slowest',
#   ) %>%
#   dplyr::mutate(check=factor(check,
#                           levels = plot_levels)) %>%
#   ggplot2::ggplot(ggplot2::aes(x = dbtype,
#            y = check,
#            fill = as.character(value))) +
#   ggplot2::geom_tile(colour="grey", alpha=0.8, width=1) +
#   ggplot2::xlab(label = "Sample")+
#   ggplot2::theme_bw() +
#   ggplot2::theme(legend.position="none") +
#   ggplot2::scale_x_discrete(expand=c(0,0))+
#   ggplot2::scale_y_discrete(expand=c(0,0)) +
#   ggplot2::scale_fill_manual(values = c("red", "green"))+
#   ggplot2::xlab("DBMS")+
#   ggplot2::ylab("")+
#   ggplot2::ggtitle("Analytic functionality checked in continuous integration")

# ggplot2::ggsave("dbms_coverage/tests.png", plot)
