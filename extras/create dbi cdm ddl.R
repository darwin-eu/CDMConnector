list.files(system.file("csv", package = "CommonDataModel"))


df <- readr::read_csv(system.file("csv", "OMOP_CDMv5.3_Table_Level.csv", package = "CommonDataModel"))
df



sql <- CommonDataModel::createDdl("5.3") |>
  SqlRender::render(cdmDatabaseSchema = "main") |>
  SqlRender::translate("duckdb") |>
  SqlRender::splitSql()

cat(sql[1:3])

# run the ddl
con <- DBI::dbConnect(duckdb::duckdb())

for (i in seq_along(sql)) {
  DBI::dbExecute(con, sql[i])
}

DBI::dbGetQuery(con, "select * from main.person")

DBI::dbDisconnect(con, shutdown = TRUE)

