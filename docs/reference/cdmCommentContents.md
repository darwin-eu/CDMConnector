# Insert flattened CDM records as an aligned R comment in the active editor

Flattens a CDM using [`cdmFlatten()`](cdmFlatten.md), collects the data
into R, optionally filters by one or more `person_id` values, and
inserts an aligned, copy-pasteable comment block directly below the
current cursor line in the active RStudio document.

## Usage

``` r
cdmCommentContents(cdm, personIds = NULL)
```

## Arguments

- cdm:

  A CDM reference object created with CDMConnector.

- personIds:

  Optional numeric vector of `person_id` values to filter on. If `NULL`,
  all persons in the flattened CDM are used (use with care).

## Value

Invisibly returns the collected flattened CDM as a data.frame. The
primary side effect is insertion of commented text into the active
RStudio source editor.

## Details

This is intended as a lightweight debugging and documentation helper
when inspecting patient-level timelines (e.g. cohort inclusion, outcome
validation, or study review notes).

The inserted output is formatted as aligned R comments:

    # person_id | observation_concept_id | start_date | end_date   | domain
    # 12        | 2211751                 | 2021-01-13 | 2021-01-13 | procedure_occurrence

The function requires an interactive RStudio session and will error if
`rstudioapi` is not available.

## See also

[`cdmFlatten()`](cdmFlatten.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Insert patient timeline directly into your script
cdmCommentContents(cdm, 12)

# Insert multiple patients
cdmCommentContents(cdm, c(12, 22))
} # }
```
