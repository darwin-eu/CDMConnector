# Create a CDM reference from a folder containing parquet, csv, or feather files

Create a CDM reference from a folder containing parquet, csv, or feather
files

## Usage

``` r
cdm_from_files(
  path,
  format = "auto",
  cdm_version = "5.3",
  cdm_name = NULL,
  as_data_frame = TRUE
)

cdmFromFiles(
  path,
  format = "auto",
  cdmVersion = "5.3",
  cdmName = NULL,
  asDataFrame = TRUE
)
```

## Arguments

- path:

  A folder where an OMOP CDM v5.4 instance is located.

- format:

  What is the file format to be read in? Must be "auto" (default),
  "parquet", "csv", "feather".

- cdm_version, cdmVersion:

  The version of the cdm (5.3 or 5.4)

- cdm_name, cdmName:

  A name to use for the cdm.

- as_data_frame, asDataFrame:

  TRUE (default) will read files into R as dataframes. FALSE will read
  files into R as Arrow Datasets.

## Value

A list of dplyr database table references pointing to CDM tables
