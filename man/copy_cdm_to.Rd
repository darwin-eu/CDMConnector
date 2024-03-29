% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/copy_cdm_to.R
\name{copy_cdm_to}
\alias{copy_cdm_to}
\alias{copyCdmTo}
\title{Copy a cdm object from one database to another}
\usage{
copy_cdm_to(con, cdm, schema, overwrite = FALSE)

copyCdmTo(con, cdm, schema, overwrite = FALSE)
}
\arguments{
\item{con}{A DBI datbase connection created by \code{DBI::dbConnect}}

\item{cdm}{A cdm reference object created by \code{CDMConnector::cdmFromCon} or \code{CDMConnector::cdm_from_con}}

\item{schema}{schema name in the remote database where the user has write permission}

\item{overwrite}{Should the cohort table be overwritten if it already exists? TRUE or FALSE (default)}
}
\value{
A cdm reference object pointing to the newly created cdm in the remote database
}
\description{
It may be helpful to be able to easily copy a small test cdm from a local
database to a remote for testing. copy_cdm_to takes a cdm object and a connection.
It copies the cdm to the remote database connection. CDM tables can be prefixed
in the new database allowing for multiple cdms in a single shared database
schema.
}
\details{
\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#experimental}{\figure{lifecycle-experimental.svg}{options: alt='[Experimental]'}}}{\strong{[Experimental]}}
}
