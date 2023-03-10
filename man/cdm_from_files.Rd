% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/cdm.R
\name{cdm_from_files}
\alias{cdm_from_files}
\alias{cdmFromFiles}
\title{Create a CDM reference from a folder containing parquet, csv, or feather
files}
\usage{
cdm_from_files(path, format = "auto", as_data_frame = TRUE)

cdmFromFiles(path, format = "auto", asDataFrame = TRUE)
}
\arguments{
\item{path}{A folder where an OMOP CDM v5.4 instance is located.}

\item{format}{What is the file format to be read in? Must be "auto"
(default), "parquet", "csv", "feather".}

\item{as_data_frame}{TRUE (default) will read files into R as dataframes.
FALSE will read files into R as Arrow Datasets.}

\item{asDataFrame}{TRUE (default) will read files into R as dataframes.
FALSE will read files into R as Arrow Datasets.}
}
\value{
A list of dplyr database table references pointing to CDM tables

A list of dplyr database table references pointing to CDM tables
}
\description{
Create a CDM reference from a folder containing parquet, csv, or feather
files

Create a CDM reference from a folder containing parquet, csv, or feather
files
}
