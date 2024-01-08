
#' Copy a cdm object from one database to another
#'
#' It may be helpful to be able to easily copy a small test cdm from a local
#' database to a remote for testing. copy_cdm_to takes a cdm object and a connection.
#' It copies the cdm to the remote database connection. CDM tables can be prefixed
#' in the new database allowing for multiple cdms in a single shared database
#' schema.
#'
#' `r lifecycle::badge("experimental")`
#'
#' @param con A DBI datbase connection created by `DBI::dbConnect`
#' @param cdm A cdm reference object created by `CDMConnector::cdmFromCon` or `CDMConnector::cdm_from_con`
#' @param schema schema name in the remote database where the user has write permission
#' @param overwrite Should the cohort table be overwritten if it already exists? TRUE or FALSE (default)
#'
#' @return A cdm reference object pointing to the newly created cdm in the remote database
#' @export
copy_cdm_to <- function(con, cdm, schema, overwrite = FALSE) {

  checkmate::assertTRUE(DBI::dbIsValid(con))
  checkmate::assertClass(cdm, "cdm_reference")
  if (dbms(con) == "bigquery") rlang::abort("copy_cdm_to on BigQuery is not yet supported!")
  checkmate::assertCharacter(schema, min.len = 1, max.len = 3, all.missing = F)
  checkmate::assertLogical(overwrite, len = 1)

  # create a new source
  newSource <- dbSource(con = con, writeSchema = schema)

  # insert person and observation_period
  cdmTables <- list()
  for (tab in c("person", "observation_period")) {
    cdmTables[[tab]] <- omopgenerics::insertTable(
      cdm = newSource,
      name = tab,
      table = cdm[[tab]] |> dplyr::collect() |> dplyr::as_tibble(),
      overwrite = overwrite
    )
  }

  # create cdm object
  newCdm <- omopgenerics::cdmReference(
    tables = cdmTables, cdmName = omopgenerics::cdmName(cdm)
  )

  # copy all other tables
  tables_to_copy <- names(cdm)
  tables_to_copy <- tables_to_copy[
    !tables_to_copy %in% c("person", "observation_period")
  ]
  for (i in cli::cli_progress_along(tables_to_copy)) {
    table_name <- tables_to_copy[i]
    cohort <- inherits(cdm[[table_name]], "generated_cohort_set")
    if (cohort) {
      set <- omopgenerics::settings(cdm[[table_name]]) |> dplyr::as_tibble()
      att <- omopgenerics::attrition(cdm[[table_name]]) |> dplyr::as_tibble()
      newCdm <- omopgenerics::insertTable(
        cdm = newCdm, name = paste0(table_name, "_set"), table = set,
        overwrite = overwrite
      )
      newCdm <- omopgenerics::insertTable(
        cdm = newCdm, paste0(table_name, "_attrition"), table = att,
        overwrite = overwrite
      )
    }
    newCdm <- omopgenerics::insertTable(
      cdm = newCdm,
      name = table_name,
      table = cdm[[table_name]] |> dplyr::collect() |> dplyr::as_tibble(),
      overwrite = overwrite
    )
    if (cohort) {
      newCdm[[table_name]] <- omopgenerics::cohortTable(
        cohortRef = newCdm[[table_name]],
        cohortSetRef = newCdm[[paste0(table_name, "_set")]],
        cohortAttritionRef = newCdm[[paste0(table_name, "_attrition")]],
        overwrite = overwrite
      )
      newCdm[[paste0(table_name, "_set")]] <- NULL
      newCdm[[paste0(table_name, "_attrition")]] <- NULL
    }
  }

  return(newCdm)
}

#' @rdname copy_cdm_to
#' @export
copyCdmTo <- copy_cdm_to
