# Read a set of cohort definitions into R

A "cohort set" is a collection of cohort definitions. In R this is
stored in a dataframe with cohort_definition_id, cohort_name, and cohort
columns. On disk this is stored as a folder with a CohortsToCreate.csv
file and one or more json files. If the CohortsToCreate.csv file is
missing then all of the json files in the folder will be used,
cohort_definition_id will be automatically assigned in alphabetical
order, and cohort_name will match the file names.

## Usage

``` r
readCohortSet(path)
```

## Arguments

- path:

  The path to a folder containing Circe cohort definition json files and
  optionally a csv file named CohortsToCreate.csv with columns cohortId,
  cohortName, and jsonPath.
