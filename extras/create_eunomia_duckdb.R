
# Create Eunomia duckdb test database
# Duckdb file format is not yet stable so the file created is tied to a specific version of duckdb.
# install.packages("DatabaseConnector")

cd <- Eunomia::getEunomiaConnectionDetails()

csvPath <- here::here("extras", "EunomiaCdm")
unlink(csvPath, recursive = TRUE)
Eunomia::exportToCsv(csvPath)
csvPaths <- list.files(csvPath, full.names = T)


duckdbPath <- here::here("extras", "cdm.duckdb")
unlink(duckdbPath)
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = duckdbPath)

library(dplyr)
for(p in csvPaths) {
  table_name <- tolower(stringr::str_remove(basename(p), ".csv$"))
  print(paste("loading", p))
  table <- readr::read_csv(p)
  colnames(table) <- tolower(colnames(table))
  # table <- mutate(table, across(matches("_id"), ~ifelse(is.numeric(.), as.integer(.), .)))
  DBI::dbWriteTable(con, table_name, table, overwrite = TRUE)
  # sql <- glue::glue("CREATE TABLE {tolower(table_name)} AS SELECT * FROM '{nm}';")
  # DBI::dbExecute(con, sql)
}

tables <- DBI::dbListTables(con)
for (tbl in tables) {
  print(tibble::tibble(DBI::dbGetQuery(con, paste("select * from ", tbl, "limit 10"))))
}

DBI::dbDisconnect(con, shutdown = TRUE)
unlink(csvPath)

# compress
o <- setwd(here::here("extras"))
tar("cdm.duckdb.tar.xz", "cdm.duckdb", compression = "xz", compression_level = 9L) # best option
# tar("cdm.duckdb.tar.gz", "cdm.duckdb", compression = "gzip", compression_level = 9L)
# tar("cdm.duckdb.tar.bzip2", "cdm.duckdb", compression = "bzip2", compression_level = 9L)
setwd(o)


