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
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
#' listTables(con, schema = "main")
#' }
listTables <- function(con, schema = NULL) {
  checkmate::assert_character(schema, null.ok = TRUE, min.len = 1, max.len = 2, min.chars = 1)
  if (is.null(schema)) return(DBI::dbListTables(con))

  switch (class(con),
    PqConnection = list_tables_postgresql(con, schema[[1]]),
    duckdb_connection = list_tables_postgresql(con, schema[[1]]),
    "Microsoft SQL Server" = list_tables_mssql(con, schema),
    rlang::abort(paste(class(con), "connection not supported"))
  )
}

list_tables_postgresql <- function(con, schema) {
  glue::glue_sql("select table_name from information_schema.tables where table_schema = {schema};", .con = con) %>%
    DBI::dbGetQuery(con, .) %>%
    dplyr::pull(table_name)
}

list_tables_duckdb <- function(con, schema) {
  glue::glue_sql("select table_name from information_schema.tables where table_schema = {schema};", .con = con) %>%
    DBI::dbGetQuery(con, .) %>%
    dplyr::pull(table_name)
}

list_tables_mssql <- function(con, schema) {
  if (length(schema) == 1) {
    DBI::dbListTables(con, schema_name = schema)
  } else {
    # length(schema) must be 2
    DBI::dbListTables(con, catalog_name = schema[[1]], schema_name = schema[[2]])
  }
}
