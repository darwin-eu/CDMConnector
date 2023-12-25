
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

  # get source
  src <- attr(cdm, "cdm_source")

  # create a new source
  newSource <- dbSource(
    con = con, cdmName = cdmName(cdm), cdmSchema = schema, writeSchema = schema,
    achillesSchema = schema
  )

  # insert person and observation_period
  cdmTables <- list()
  cdmTables[["person"]] <- omopgenerics::insertTable(
    cdm = newSource,
    name = "person",
    table = cdm$person |> dplyr::collect(),
    overwrite = overwrite
  )
  cdmTables[["observation_period"]] <- omopgenerics::insertTable(
    cdm = newSource,
    name = "observation_period",
    table = cdm$observation_period |> dplyr::collect(),
    overwrite = overwrite
  )

  # create cdm object
  newCdm <- omopgenerics::cdmReference(
    cdmTables = cdmTables, cdmSource = newSource
  )

  # copy all other tables
  tables_to_copy <- names(cdm)
  tables_to_copy <- tables_to_copy[
    !tables_to_copy %in% c("person", "observation_period")
  ]
  for (i in cli::cli_progress_along(tables_to_copy)) {
    table_name <- tables_to_copy[i]
    newCdm <- omopgenerics::insertTable(
      cdm = newCdm,
      name = table_name,
      table = cdm[[table_name]] |> dplyr::collect(),
      overwrite = overwrite
    )
  }

  return(newCdm)
}

#' @rdname copy_cdm_to
#' @export
copyCdmTo <- copy_cdm_to
