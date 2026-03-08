# Parse cohort expression from JSON

Load a cohort expression from a JSON string. Handles both camelCase and
PascalCase field names for Java/Atlas compatibility.

## Usage

``` r
cohort_expression_from_json(json_str)
```

## Arguments

- json_str:

  Character string containing cohort definition JSON

## Value

List structure representing the cohort expression (validated)
