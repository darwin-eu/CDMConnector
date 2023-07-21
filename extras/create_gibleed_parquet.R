library(DatabaseConnector)
con <- connect(Eunomia::getEunomiaConnectionDetails())

# missing 2 tables
stringr::str_subset(getTableNames(con, databaseSchema = "main"),
                    "attribute_definition|cohort_definition")

table_names <- getTableNames(con)

for (i in seq_along(table_names)) {
  nm <- table_names[i]
  df <- querySql(con, glue::glue("select * from main.{nm}")) %>%
    rename_all(tolower)

  if ("reveue_code_source_value" %in% colnames(df)) {
    df <- dplyr::rename(df, revenue_code_source_value = "reveue_code_source_value")
  }
  arrow::write_parquet(df, here::here("scratch", "GiBleed_5.3", paste0(nm, ".parquet")))
}

disconnect(con)


# load into duckdb from parquet

load_cdm <- function(path) {

  con <- DBI::dbConnect(duckdb::duckdb())

  specs <- spec_cdm_field[[cdmVersion]] %>%
    dplyr::mutate(cdmDatatype = dplyr::if_else(.data$cdmDatatype == "varchar(max)", "varchar(2000)", .data$cdmDatatype)) %>%
    dplyr::mutate(cdmFieldName = dplyr::if_else(.data$cdmFieldName == '"offset"', "offset", .data$cdmFieldName)) %>%
    dplyr::mutate(cdmDatatype = dplyr::case_when(
      dbms(con) == "postgresql" & .data$cdmDatatype == "datetime" ~ "timestamp",
      dbms(con) == "redshift" & .data$cdmDatatype == "datetime" ~ "timestamp",
      TRUE ~ cdmDatatype)) %>%
    tidyr::nest(col = -"cdmTableName") %>%
    dplyr::mutate(col = purrr::map(col, ~setNames(as.character(.$cdmDatatype), .$cdmFieldName)))

  files_in_path <- tools::file_path_sans_ext(basename(list.files(path)))

  for (i in cli::cli_progress_along(specs$cdmTableName)) {

    if (isFALSE(tables[i] %in% files_in_path)) {
      # rlang::warn(glue::glue("{tables[i]} not in archive!"))
      next
    }

    fields <- specs %>%
      dplyr::filter(.data$cdmTableName == specs$cdmTableName[i]) %>%
      dplyr::pull(.data$col) %>%
      unlist()

    DBI::dbCreateTable(con, inSchema(cdm_schema, specs$cdmTableName[i], dbms = dbms(con)), fields = fields)
    cols <- paste(glue::glue('"{names(fields)}"'), collapse = ", ")

    table_path <- file.path(path, paste0(specs$cdmTableName[i], ".parquet"))
    sql <- glue::glue("INSERT INTO {cdm_schema}.{tables[i]}({cols}) SELECT {cols} FROM '{table_path}'")
    DBI::dbExecute(con, sql)
  }
  return(con)
}

path <- file.path("~", "darwin", "CDMConnector", "scratch", "GiBleed_5.3")

con <- load_cdm(path)

cdm_from_con(con)$concept %>%
  collect
