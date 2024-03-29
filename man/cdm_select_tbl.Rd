% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/cdm.R
\name{cdm_select_tbl}
\alias{cdm_select_tbl}
\title{Select a subset of tables in a cdm reference object}
\usage{
cdm_select_tbl(cdm, ...)
}
\arguments{
\item{cdm}{A cdm reference object created by \code{cdm_from_con}}

\item{...}{One or more table names of the tables of the \code{cdm} object.
\code{tidyselect} is supported, see \code{dplyr::select()} for details on the semantics.}
}
\value{
A cdm reference object containing the selected tables
}
\description{
This function uses syntax similar to \code{dplyr::select} and can be used to
subset a cdm reference object to a specific tables
}
\examples{
\dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())

cdm <- cdm_from_con(con, "main")

cdm_select_tbl(cdm, person)
cdm_select_tbl(cdm, person, observation_period)
cdm_select_tbl(cdm, tbl_group("vocab"))
cdm_select_tbl(cdm, "person")

DBI::dbDisconnect(con)
}
}
