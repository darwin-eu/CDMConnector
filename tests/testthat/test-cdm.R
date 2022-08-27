test_that("cdm reference works", {
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname = "cdm",
                        host = "localhost",
                        user = "postgres",
                        password = "")

  cdm <- cdm_from_con(con, cdm_schema = "synthea1k", select = tbl_group("vocab"))

  names(cdm)

  DBI::dbDisconnect(con)
})




# library(DBI)
# connectionDetails <- dbConnectDetails(RPostgres::Postgres(),
#                                       dbname = "cdm",
#                                       host = "localhost",
#                                       user = "postgres",
#                                       password = Sys.getenv("password"))
#
#
# con <- dbConnect(connectionDetails)
# dbGetQuery(con, "select count(*) as n from synthea1k.person")
# DBI::dbDisconnect(con)
#
# selfContainedQuery <- function(dbConnectDetails) {
#    con <- dbConnect(connectionDetails)
#    on.exit(dbDisconnect(con))
#    dbGetQuery(con, "select count(*) as n from synthea1k.person")
#  }
#
# selfContainedQuery(connectionDetails)




