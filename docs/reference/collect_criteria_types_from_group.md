# Recursively collect criteria types (domains) from a criteria group. Uses list accumulator internally to avoid O(n^2) vector growth when called repeatedly across many cohorts.

Recursively collect criteria types (domains) from a criteria group. Uses
list accumulator internally to avoid O(n^2) vector growth when called
repeatedly across many cohorts.

## Usage

``` r
collect_criteria_types_from_group(group, acc = NULL)
```

## Arguments

- group:

  List - criteria group with CriteriaList / Groups

- acc:

  List accumulator (internal, for recursive calls)

## Value

Character vector of criteria type names
