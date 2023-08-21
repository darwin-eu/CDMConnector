
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

  if ("prefix" %in% names(schema)) {
    prefix <- unname(schema["prefix"])
    checkmate::assertCharacter(prefix, len = 1, min.chars = 1, pattern = "[a-zA-Z0-9_]+", any.missing = FALSE)

    schema_without_prefix <- schema[names(schema) != "prefix"]
    checkmate::assertCharacter(schema_without_prefix, min.len = 1, max.len = 2, pattern = "[a-zA-Z0-9_]+", any.missing = FALSE)
  } else {
    prefix = ""
  }

  checkmate::assertLogical(overwrite, len = 1)

  tables_to_copy <- names(cdm)

  specs <- spec_cdm_field[[attr(cdm, "cdm_version")]] %>%
    dplyr::mutate(cdmDatatype = dplyr::if_else(.data$cdmDatatype == "varchar(max)", "varchar(2000)", .data$cdmDatatype)) %>%
    dplyr::mutate(cdmFieldName = dplyr::if_else(.data$cdmFieldName == '"offset"', "offset", .data$cdmFieldName)) %>%
    # TODO come up with a better way to store datatype mappings
    dplyr::mutate(cdmDatatype = dplyr::case_when(
      dbms(con) == "postgresql" & .data$cdmDatatype == "datetime" ~ "timestamp",
      dbms(con) == "redshift" & .data$cdmDatatype == "datetime" ~ "timestamp",
      dbms(con) == "bigquery" & stringr::str_detect(.data$cdmDatatype, "varchar") ~ "string",
      TRUE ~ cdmDatatype)) %>%
    tidyr::nest(col = -"cdmTableName") %>%
    dplyr::mutate(col = purrr::map(col, ~setNames(as.character(.$cdmDatatype), .$cdmFieldName)))

  tables_in_database <- list_tables(con, schema = schema)

  for (i in cli::cli_progress_along(tables_to_copy)) {
    table_name <- tables_to_copy[i]
    table_name_prefixed <- paste0(prefix, table_name)
    local_tbl <- dplyr::collect(cdm[[table_name]])

    fields <- specs %>%
      dplyr::filter(.data$cdmTableName == table_name) %>%
      dplyr::pull(.data$col) %>%
      unlist()

    # TODO truncate character columns to max length based on specification in example cdms
    varchars <- fields[stringr::str_detect(fields, "varchar")]
    cols <- names(varchars)
    widths <- stringr::str_extract(varchars, "[0-9]+") %>% as.numeric()

    for (j in seq_along(cols)) {
      if (!is.na(widths[j])) {
        local_tbl[[cols[j]]] <- stringr::str_trunc(local_tbl[[cols[j]]], widths[j], ellipsis = "")
      }
    }

    if (table_name %in% tables_in_database) {
      if (overwrite) {
        DBI::dbRemoveTable(con, inSchema(schema, table_name, dbms = dbms(con)))
      } else {
        rlang::abort(glue::glue("{table_name_prefixed} already exists in the database!"))
      }
    }

    if (dbms(con) != "snowflake") {
      DBI::dbCreateTable(con, inSchema(schema, table_name, dbms = dbms(con)), fields = fields)

      # note that dbExists table is not working on snowflake with DBI::Id
      # dbAppendTable calls dbExists
      if (nrow(local_tbl) > 0) {
        DBI::dbAppendTable(con, inSchema(schema, table_name, dbms = dbms(con)), value = local_tbl)
      }
    } else {
      # TODO fix dbExists so dbAppend works with snowflake
      DBI::dbWriteTable(con, inSchema(schema, table_name, dbms = dbms(con)), value = local_tbl)
    }
  }

  cdm_from_con(con,
               cdm_schema = schema,
               cdm_version = attr(cdm, "cdm_version"),
               cdm_name = attr(cdm, "cdm_name"))
}

#' @rdname copy_cdm_to
#' @export
copyCdmTo <- copy_cdm_to
