#' List tables in a schema
#'
#' DBI::dbListTables can be used to get all tables in a database but not always in a
#' specific schema. `listTables` will list tables in a schema.
#'
#' @param con A DBI connection to a database
#' @param schema The name of a schema in a database. If NULL, returns DBI::dbListTables(con).
#'
#' @return A character vector of table names
#' @export
#' @importFrom rlang .data
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' listTables(con, schema = "main")
#' }
listTables <- function(con, schema = NULL) {
  checkmate::assert_character(schema, null.ok = TRUE, min.len = 1, max.len = 2, min.chars = 1)
  if (is.null(schema)) return(DBI::dbListTables(con))
  withr::local_options(list(arrow.pull_as_vector = TRUE))

  if (is(con, "DatabaseConnectorJdbcConnection")) {
    DBI::dbListTables(con, databaseSchema = paste0(schema, collapse = "."))

      } else if (is(con, "PqConnection") || is(con, "RedshiftConnection")) {
    sql <- glue::glue_sql("select table_name from information_schema.tables where table_schema = {schema[[1]]};", .con = con)
    DBI::dbGetQuery(con, sql) %>%
      dplyr::pull(.data$table_name)

  } else if (is(con, "duckdb_connection")) {
    sql <- glue::glue_sql("select table_name from information_schema.tables where table_schema = {schema[[1]]};", .con = con)
    DBI::dbGetQuery(con, sql) %>%
      dplyr::pull(.data$table_name)

  } else if (is(con, "Spark SQL")) {
    # spark odbc connection
    sql <- paste("SHOW TABLES", if (!is.null(schema)) paste("IN", schema[[1]]))
    DBI::dbGetQuery(con, sql) %>%
      dplyr::filter(.data$isTemporary == FALSE) %>%
      dplyr::pull(.data$tableName)

  } else if (is(con, "OdbcConnection")) {
    if (length(schema) == 1) {
      DBI::dbListTables(con, schema_name = schema)
    } else {
      DBI::dbListTables(con, catalog_name = schema[[1]], schema_name = schema[[2]])
    }
  } else if (is(con, "OraConnection")) {
    checkmate::assert_character(schema, null.ok = TRUE, len = 1, min.chars = 1)
    DBI::dbListTables(con, schema = schema)
  } else if (is(con, "BigQueryConnection")) {
    checkmate::assert_character(schema, null.ok = TRUE, len = 1, min.chars = 1)

    DBI::dbGetQuery(con,
                    glue::glue("SELECT table_name
                                FROM `{schema}`.INFORMATION_SCHEMA.TABLES
                                WHERE table_schema = '{schema}'"))[[1]]
  } else {
    rlang::abort(paste(paste(class(con), collapse = ", "), "connection not supported"))
  }
}

