
# Create Eunomia duckdb test database
# Duckdb file format is not yet stable so the file created is tied to a specific version of duckdb.
# install.packages("DatabaseConnector")


export_path <- "~/GiBleed"
dir.create(export_path)
library(dplyr)

# devtools::install_github("ohdsi/Eunomia", "v1.0.2")
cd <- Eunomia::getEunomiaConnectionDetails()
con <- DatabaseConnector::connect(cd)

tables_to_copy <- DatabaseConnector::dbListTables(con)

specs <- spec_cdm_field[["5.3"]] %>%
  dplyr::mutate(cdmDatatype = dplyr::if_else(.data$cdmDatatype == "varchar(max)", "varchar(2000)", .data$cdmDatatype)) %>%
  dplyr::mutate(cdmFieldName = dplyr::if_else(.data$cdmFieldName == '"offset"', "offset", .data$cdmFieldName)) %>%
  tidyr::nest(col = -"cdmTableName") %>%
  dplyr::mutate(col = purrr::map(col, ~setNames(as.character(.$cdmDatatype), .$cdmFieldName)))


targetdb <- DBI::dbConnect(duckdb::duckdb())
i=3
for (i in seq_along(tables_to_copy)) {
# for (i in 1:10) {
  table_name <- tables_to_copy[i]
  if (table_name %in% c("cohort", "cohort_attribute")) next

  local_tbl <- DatabaseConnector::dbReadTable(con, table_name) %>%
    rename_all(tolower) %>%
    tibble()

  # TODO fix this in eunomia dataset
  if ("reveue_code_source_value" %in% colnames(local_tbl)) {
    local_tbl <- dplyr::rename(local_tbl, revenue_code_source_value = "reveue_code_source_value")
  }

  fields <- specs %>%
    dplyr::filter(.data$cdmTableName == table_name) %>%
    dplyr::pull(.data$col) %>%
    unlist()

  if (is.null(fields)) {
    message(glue::glue("skipping {table_name}"))
    next
  }

  DBI::dbCreateTable(targetdb, inSchema("main", table_name, dbms = "duckdb"), fields = fields)

  if (nrow(local_tbl) > 0) {
    DBI::dbAppendTable(targetdb, inSchema("main", table_name, dbms = "duckdb"), value = local_tbl)
  }
  print(glue::glue("{table_name} done."))
}

cdm <- cdmFromCon(targetdb, "main")

cdm$condition_occurrence %>%
  filter(person_id == 32, condition_concept_id == 192671)

tables <- listTables(targetdb, "main")

for (tbl in tables) {
  # export to parquet
  sql <- glue::glue("COPY (SELECT * FROM {tbl}) TO '~/GiBleed/{tbl}.parquet' (FORMAT 'parquet')")
  DBI::dbExecute(targetdb, sql)
}

# zip and upload to server.


# library(CDMConnector)
# library(dplyr)
# con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir("GiBleed"))
# cdm <- cdmFromCon(con, "main", "main")
#
# df <- cdm$condition_occurrence  %>%
#   filter(person_id == 32, condition_concept_id == 192671) %>%
#   left_join(cdm$observation_period, by = "person_id") %>%
#   dplyr::collect() %>%
#   tibble()

