# Using CDM attributes

## Set up

Let’s again load required packages and connect to our Eunomia dataset in
duckdb.

``` r
library(CDMConnector)
library(omopgenerics)
library(dplyr)

write_schema <- "main"
cdm_schema <- "main"

con <- DBI::dbConnect(duckdb::duckdb(), 
                      dbdir = eunomiaDir())
cdm <- cdmFromCon(con, 
                    cdmName = "eunomia", 
                    cdmSchema = cdm_schema, 
                    writeSchema = write_schema, 
                    cdmVersion = "5.3")
```

## CDM reference attributes

Our cdm reference has various attributes associated with it. These can
be useful both when programming and when developing analytic packages on
top of CDMConnector.

### CDM name

It’s a requirement that every cdm reference has name associated with it.
This is particularly useful for network studies so that we can associate
results with a particular cdm. We can use `cdmName` (or it’s snake case
equivalent `cdm_name`) to get the cdm name.

``` r
cdmName(cdm)
#> [1] "eunomia"
```

### CDM version

The OMOP CDM has various versions. We also have an attribute giving the
version of the cdm we have connected to.

``` r
cdmVersion(cdm)
#> [1] "5.3"
```

### Database connection

We also have an attribute identifying the database connection underlying
the cdm reference.

``` r
cdmCon(cdm)
#> <duckdb_connection 1e620 driver=<duckdb_driver dbdir='/private/var/folders/2j/8z0yfn1j69q8sxjc7vj9yhz40000gp/T/RtmplLBK3N/file1252161d027.duckdb' read_only=FALSE bigint=numeric>>
```

This can be useful, for example, if we want to make use of DBI functions
to work with the database. For example we could use `dbListTables` to
list the names of remote tables accessible through the connection,
`dbListFields` to list the field names of a specific remote table, and
`dbGetQuery` to returns the result of a query

``` r
DBI::dbListTables(cdmCon(cdm))
#>  [1] "care_site"             "cdm_source"            "concept"              
#>  [4] "concept_ancestor"      "concept_class"         "concept_relationship" 
#>  [7] "concept_synonym"       "condition_era"         "condition_occurrence" 
#> [10] "cost"                  "death"                 "device_exposure"      
#> [13] "domain"                "dose_era"              "drug_era"             
#> [16] "drug_exposure"         "drug_strength"         "fact_relationship"    
#> [19] "location"              "measurement"           "metadata"             
#> [22] "note"                  "note_nlp"              "observation"          
#> [25] "observation_period"    "payer_plan_period"     "person"               
#> [28] "procedure_occurrence"  "provider"              "relationship"         
#> [31] "source_to_concept_map" "specimen"              "visit_detail"         
#> [34] "visit_occurrence"      "vocabulary"
DBI::dbListFields(cdmCon(cdm), "person")
#>  [1] "person_id"                   "gender_concept_id"          
#>  [3] "year_of_birth"               "month_of_birth"             
#>  [5] "day_of_birth"                "birth_datetime"             
#>  [7] "race_concept_id"             "ethnicity_concept_id"       
#>  [9] "location_id"                 "provider_id"                
#> [11] "care_site_id"                "person_source_value"        
#> [13] "gender_source_value"         "gender_source_concept_id"   
#> [15] "race_source_value"           "race_source_concept_id"     
#> [17] "ethnicity_source_value"      "ethnicity_source_concept_id"
DBI::dbGetQuery(cdmCon(cdm), "SELECT * FROM person LIMIT 5")
#>   person_id gender_concept_id year_of_birth month_of_birth day_of_birth
#> 1         6              8532          1963             12           31
#> 2       123              8507          1950              4           12
#> 3       129              8507          1974             10            7
#> 4        16              8532          1971             10           13
#> 5        65              8532          1967              3           31
#>   birth_datetime race_concept_id ethnicity_concept_id location_id provider_id
#> 1     1963-12-31            8516                    0          NA          NA
#> 2     1950-04-12            8527                    0          NA          NA
#> 3     1974-10-07            8527                    0          NA          NA
#> 4     1971-10-13            8527                    0          NA          NA
#> 5     1967-03-31            8516                    0          NA          NA
#>   care_site_id                  person_source_value gender_source_value
#> 1           NA 001f4a87-70d0-435c-a4b9-1425f6928d33                   F
#> 2           NA 052d9254-80e8-428f-b8b6-69518b0ef3f3                   M
#> 3           NA 054d32d5-904f-4df4-846b-8c08d165b4e9                   M
#> 4           NA 00444703-f2c9-45c9-a247-f6317a43a929                   F
#> 5           NA 02a3dad9-f9d5-42fb-8074-c16d45b4f5c8                   F
#>   gender_source_concept_id race_source_value race_source_concept_id
#> 1                        0             black                      0
#> 2                        0             white                      0
#> 3                        0             white                      0
#> 4                        0             white                      0
#> 5                        0             black                      0
#>   ethnicity_source_value ethnicity_source_concept_id
#> 1            west_indian                           0
#> 2                italian                           0
#> 3                 polish                           0
#> 4               american                           0
#> 5              dominican                           0
```

## Cohort attributes

### Generated cohort set

When we generate a cohort in addition to the cohort table itself we also
have various attributes that can be useful for subsequent analysis.

Here we create a cohort table with a single cohort.

``` r
cdm <- generateConceptCohortSet(cdm = cdm, 
                                conceptSet = list("gi_bleed" = 192671,
                                                  "celecoxib" = 1118084), 
                                name = "study_cohorts",
                                overwrite = TRUE)

cdm$study_cohorts %>% 
  glimpse()
```

We have a cohort set attribute that gives details on the settings
associated with the cohorts (along with utility functions to make it
easier to access this attribute).

``` r
settings(cdm$study_cohorts)
```

We have a cohort_count attribute with counts for each of the cohorts.

``` r
cohortCount(cdm$study_cohorts)
```

And we also have an attribute, cohort attrition, with a summary of
attrition when creating the cohorts.

``` r
attrition(cdm$study_cohorts)
```

### Creating a bespoke cohort

Say we create a custom GI bleed cohort with the standard cohort
structure

``` r
cdm$gi_bleed <- cdm$condition_occurrence %>% 
  filter(condition_concept_id == 192671) %>% 
  mutate(cohort_definition_id = 1) %>% 
  select(
    cohort_definition_id, 
    subject_id = person_id, 
    cohort_start_date = condition_start_date, 
    cohort_end_date = condition_start_date
  ) %>% 
  compute(name = "gi_bleed", temporary = FALSE, overwrite = TRUE)

cdm$gi_bleed %>% 
  glimpse()
#> Rows: ??
#> Columns: 4
#> Database: DuckDB 1.4.4 [root@Darwin 25.3.0:R 4.5.1//private/var/folders/2j/8z0yfn1j69q8sxjc7vj9yhz40000gp/T/RtmplLBK3N/file1252161d027.duckdb]
#> $ cohort_definition_id <dbl> 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1…
#> $ subject_id           <int> 273, 61, 351, 579, 549, 116, 163, 304, 326, 285, …
#> $ cohort_start_date    <date> 2011-10-10, 2005-09-15, 2018-06-28, 1999-11-06, …
#> $ cohort_end_date      <date> 2011-10-10, 2005-09-15, 2018-06-28, 1999-11-06, …
```

We can add the required attributes using the `newCohortTable` function.
The minimum requirement for this is that we also define the cohort set
to associate with our set of custom cohorts.

``` r
GI_bleed_cohort_ref <- tibble(cohort_definition_id = 1, cohort_name = "custom_gi_bleed")

cdm$gi_bleed <- omopgenerics::newCohortTable(
  table = cdm$gi_bleed, cohortSetRef = GI_bleed_cohort_ref
)
```

Now our custom cohort GI_bleed has the same attributes associated with
it as if it had been created by `generateConceptCohortSet`. This will
allow it to be used by analytic packages designed to work with cdm
cohorts.

``` r
settings(cdm$gi_bleed)
#> # A tibble: 1 × 2
#>   cohort_definition_id cohort_name    
#>                  <dbl> <chr>          
#> 1                    1 custom_gi_bleed
cohortCount(cdm$gi_bleed)
#> # A tibble: 1 × 3
#>   cohort_definition_id number_records number_subjects
#>                  <int>          <int>           <int>
#> 1                    1            479             479
attrition(cdm$gi_bleed)
#> # A tibble: 1 × 7
#>   cohort_definition_id number_records number_subjects reason_id reason          
#>                  <int>          <int>           <int>     <int> <chr>           
#> 1                    1            479             479         1 Initial qualify…
#> # ℹ 2 more variables: excluded_records <int>, excluded_subjects <int>
```
