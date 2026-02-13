# remotes::install_github("ohdsi/SqlRender", "develop")

con <- DBI::dbConnect(duckdb::duckdb())

DBI::dbWriteTable(con, "date_table", data.frame(event_date = as.Date("2020-01-01")))

# This sql which is used in Circe does not work
ohdsi_sql <- "select event_date, DATEADD(day,-1 * 7, event_date) as new_date from date_table;"
duckdb_sql <- SqlRender::translate(ohdsi_sql, "duckdb")
cat(duckdb_sql)

DBI::dbGetQuery(con, duckdb_sql)

DBI::dbDisconnect(con, shutdown = T)
