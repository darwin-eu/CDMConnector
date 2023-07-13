#
# library(CDMConnector)
# library(pool)
#
# pool <- dbPool(
#   drv = duckdb::duckdb(),
#   dbdir = eunomia_dir()
# )
#
# cdm <- cdm_from_con(pool, "main")
#
#
# dbms(pool)
#
#
# cdm$person
#
#
# poolClose(pool)
