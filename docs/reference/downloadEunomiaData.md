# Download Eunomia data files

Download the Eunomia data files from
https://github.com/darwin-eu/EunomiaDatasets

## Usage

``` r
downloadEunomiaData(
  datasetName = "GiBleed",
  cdmVersion = "5.3",
  pathToData = Sys.getenv("EUNOMIA_DATA_FOLDER"),
  overwrite = FALSE
)
```

## Arguments

- datasetName:

  The data set name as found on
  https://github.com/darwin-eu/EunomiaDatasets. The data set name
  corresponds to the folder with the data set ZIP files

- cdmVersion:

  The OMOP CDM version. This version will appear in the suffix of the
  data file, for example: synpuf_5.3.zip. Must be '5.3' (default) or
  '5.4'.

- pathToData:

  The path where the Eunomia data is stored on the file system., By
  default the value of the environment variable "EUNOMIA_DATA_FOLDER" is
  used.

- overwrite:

  Control whether the existing archive file will be overwritten should
  it already exist.

## Value

Invisibly returns the destination if the download was successful.

## Examples

``` r
if (FALSE) { # \dontrun{
downloadEunomiaData("GiBleed")
} # }
```
