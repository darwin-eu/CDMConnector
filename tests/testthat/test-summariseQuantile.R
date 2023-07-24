# dbToTest <- c(
#   "duckdb"
#   ,"postgres"
#   ,"redshift"
#   ,"sqlserver"
#   # ,"oracle"
#   # ,"snowflake"
#   # ,"bigquery"
# )

prepare_cdm <- function(con,
                        write_schema,
                        write_prefix) {

  # eunomia cdm
  eunomia_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
  eunomia_cdm <- cdm_from_con(eunomia_con, cdm_schema = "main") %>%
    cdm_select_tbl("person", "observation_period")

  cdm <- copyCdmTo(con = con,
                   prefix = write_prefix,
                   cdm = eunomia_cdm,
                   schema = write_schema,
                   overwrite = TRUE)

  return(cdm)

}

check_summariseQuantile <- function(cdm){

  # summariseQuantile without group by
  expect_true(all(sort(as.integer(t(cdm$person %>%
    summarise_quantile(year_of_birth,
                       probs = round(seq(0, 1, 0.05), 2),
                       name_suffix = "quant") %>%
    dplyr::collect()))) ==
    sort(as.integer(unlist(cdm$person %>%
    dplyr::collect() %>%
    dplyr::reframe(quantiles = quantile(year_of_birth,
                                        round(seq(0, 1, 0.05), 2),
                                        type = 1)))))))

  # summariseQuantile with group by
 db_q <- cdm$person %>%
   dplyr::group_by(gender_concept_id) %>%
   summarise_quantile(x = year_of_birth,
                    probs = 0.5) %>%
    collect()
 local_q <-  cdm$person %>%
    collect() %>%
    dplyr::group_by(gender_concept_id) %>%
    dplyr::summarise(p50_value = quantile(year_of_birth, 0.5))

 expect_true(db_q %>%
  dplyr::filter(gender_concept_id == 8507) %>%
   dplyr::pull("p50_value") ==
 local_q %>%
   dplyr::filter(gender_concept_id == 8507) %>%
   dplyr::pull("p50_value"))

 expect_true(db_q %>%
               dplyr::filter(gender_concept_id == 8532) %>%
               dplyr::pull("p50_value") ==
               local_q %>%
               dplyr::filter(gender_concept_id == 8532) %>%
               dplyr::pull("p50_value"))


}

for (dbtype in dbToTest) {
  test_that(glue::glue("{dbtype} - checking summariseQuantile"), {

    con <- get_connection(dbtype)
    cdm_schema <- get_cdm_schema(dbtype)
    write_schema <- get_write_schema(dbtype)
    skip_if(any(write_schema == "") || any(cdm_schema == "") || is.null(con))

    write_prefix <- paste0("test", as.integer(Sys.time()), "_")


    cdm <- prepare_cdm(con, write_schema, write_prefix)
    check_summariseQuantile(cdm)

    rm(cdm)

    # clean up
    listTables(con = con, schema = write_schema)
    cdm <- cdm_from_con(con,
                        cdm_schema = cdm_schema,
                        write_schema = write_schema)
    CDMConnector::dropTable(cdm, dplyr::contains(write_prefix))
    disconnect(con)
  })
}

