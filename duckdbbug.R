library(dplyr, warn.conflicts = F)

# duckdb
con <- DBI::dbConnect(duckdb::duckdb())

DBI::dbWriteTable(con, DBI::Id(schema = "main", table = "cars"), cars)

tbl(con, from = DBI::Id(schema = "main", table = "cars"))

tbl(con, from = DBI::Id(schema = "main", table = "cars")) %>%
  collect()

src <- dbplyr::src_dbi(con, auto_disconnect = FALSE)
tbl(src, from = DBI::Id(schema = "main", table = "cars"))

tbl(src, from = DBI::Id(schema = "main", table = "cars")) %>% collect()

tbl(con, "cars") %>% collect()

tbl(con, "main.cars") %>% collect()

tbl(con, dbplyr::in_schema("main", "cars"))


# sqlite
DBI::dbDisconnect(con, shutdown = T)

con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

DBI::dbWriteTable(con, DBI::Id(schema = "main", table = "cars"), cars)

tbl(con, from = DBI::Id(schema = "main", table = "cars"))

tbl(con, from = DBI::Id(schema = "main", table = "cars")) %>% collect()

src <- dbplyr::src_dbi(con, auto_disconnect = FALSE)

tbl(src, from = DBI::Id(schema = "main", table = "cars"))

tbl(src, from = DBI::Id(schema = "main", table = "cars")) %>%
  collect()

tbl(con, "cars") %>% collect()

tbl(con, dbplyr::in_schema("main", "cars"))

DBI::dbDisconnect(con)



