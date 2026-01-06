# Require eunomia to be available. The function makes sure that you can later create a eunomia database with `eunomiaDir()`.

Require eunomia to be available. The function makes sure that you can
later create a eunomia database with [`eunomiaDir()`](eunomiaDir.md).

## Usage

``` r
requireEunomia(datasetName = "GiBleed", cdmVersion = "5.3")
```

## Arguments

- datasetName:

  Name of the Eunomia dataset to check. Defaults to "GiBleed".

- cdmVersion:

  Version of the Eunomia dataset to check. Must be "5.3" or "5.4".

## Value

Path to eunomia database.
