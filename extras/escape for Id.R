
library(dplyr, warn.conflicts = F)
con <- DBI::dbConnect(duckdb::duckdb())
DBI::dbWriteTable(con, "cars", cars)
dplyr::tbl(con, "cars") %>% dplyr::collect()
dplyr::tbl(con, "main.cars") %>% dplyr::collect()
dplyr::tbl(con, DBI::Id(schema = "main", table = "cars")) %>% dplyr::collect()

# define escape for Id and rerun
escape <- function(x, parens = NA, collapse = " ", con = NULL) {UseMethod("escape")}
escape.Id <- function(x, parens = FALSE, collapse = ".", con = NULL) {
  dbplyr::sql(paste(DBI::dbQuoteIdentifier(con, attr(x, "name")), collapse = collapse))
}

dplyr::tbl(con, "cars") %>% dplyr::collect()
dplyr::tbl(con, "main.cars") %>% dplyr::collect()
dplyr::tbl(con, DBI::Id(schema = "main", table = "cars")) %>% dplyr::collect()


DBI::dbDisconect(con, shutdown = T)

packageVersion("dbplyr")
packageVersion("dplyr")
packageVersion("duckdb")

