# Working with cohorts

Cohorts are a fundamental building block for observational health data
analysis. A “cohort” is a set of persons satisfying a one or more
inclusion criteria for a duration of time. If you are familiar with the
idea of sets in math then a cohort can be nicely represented as a set of
person-days. In the OMOP Common Data Model we represent cohorts using a
table with four columns.

| cohort_definition_id | subject_id | cohort_start_date | cohort_end_date |
|----------------------|------------|-------------------|-----------------|
| 1                    | 1000       | 2020-01-01        | 2020-05-01      |
| 1                    | 1000       | 2021-06-01        | 2020-07-01      |
| 1                    | 2000       | 2020-03-01        | 2020-09-01      |
| 2                    | 1000       | 2020-02-01        | 2020-03-01      |

An example cohort table

A cohort table can contain multiple cohorts and each cohort can have
multiple persons. There can even be multiple records for the same person
in a single cohort as long as the date ranges do not overlap. In the
same way that an element is either in a set or not, a single person-day
is either in a cohort or not. For a more comprehensive treatment of
cohorts in OHDSI check out the Cohorts chapter in [The Book of
OHDSI](https://ohdsi.github.io/TheBookOfOhdsi/Cohorts.html).

## Cohort Generation

The $`n*4`$ cohort table is created through the process of cohort
*generation*. To generate a cohort on a specific CDM dataset means that
we combine a *cohort definition* with CDM to produce a cohort table. The
standardization provided by the OMOP CDM allows researchers to generate
the same cohort definition on any OMOP CDM dataset.

A cohort definition is an expression of the rules governing the
inclusion/exclusion of person-days in the cohort. There are three common
ways to create cohort definitions for the OMOP CDM.

1.  The Atlas cohort builder

2.  The Capr R package

3.  Custom SQL and/or R code

Atlas is a web application that provides a graphical user interface for
creating cohort definitions. . To get started with Atlas check out the
free course on [Ehden
Academy](https://academy.ehden.eu/course/index.php) and the demo at
<https://atlas-demo.ohdsi.org/>.

Capr is an R package that provides a code-based interface for creating
cohort definitions. The options available in Capr exactly match the
options available in Atlas and the resulting cohort tables should be
identical.

There are times when more customization is needed and it is possible to
use bespoke SQL or dplyr code to build a cohort. CDMConnector provides
the `generate_concept_cohort_set` function for quickly building simple
cohorts that can then be a starting point for further subsetting.

Atlas cohorts are represented using json text files. To “generate” one
or more Atlas cohorts on a cdm object use the `read_cohort_set` function
to first read a folder of Atlas cohort json files into R. Then create
the cohort table with `generate_cohort_set`. There can be an optional
csv file called “CohortsToCreate.csv” in the folder that specifies the
cohort IDs and names to use. If this file doesn’t exist IDs will be
assigned automatically using alphabetical order of the filenames.

``` r
pathToCohortJsonFiles <- system.file("cohorts1", package = "CDMConnector")
list.files(pathToCohortJsonFiles)

readr::read_csv(file.path(pathToCohortJsonFiles, "CohortsToCreate.csv"),
                show_col_types = FALSE)
```

### Atlas cohort definitions

First we need to create our CDM object. Note that we will need to
specify a `write_schema` when creating the object. Cohort tables will go
into the CDM’s `write_schema`.

``` r
library(CDMConnector)
pathToCohortJsonFiles <- system.file("example_cohorts", package = "CDMConnector")
list.files(pathToCohortJsonFiles)

con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir("GiBleed"))
cdm <- cdmFromCon(con, cdmName = "eunomia", cdmSchema = "main", writeSchema = "main")

cohortSet <- readCohortSet(pathToCohortJsonFiles) |>
  mutate(cohort_name = snakecase::to_snake_case(cohort_name))

cohortSet

cdm <- generateCohortSet(
  cdm = cdm, 
  cohortSet = cohortSet,
  name = "study_cohorts"
)

cdm$study_cohorts
```

The generated cohort has associated metadata tables. We can access these
with utility functions.

- `cohort_count` contains the person and record counts for each cohort
  in the cohort set
- `settings` table contains the cohort id and cohort name
- `attrition` table contains the attrition information (persons, and
  records dropped at each sequential inclusion rule)

``` r
cohortCount(cdm$study_cohorts)
settings(cdm$study_cohorts)
attrition(cdm$study_cohorts)
```

Note the this cohort table is still in the database so it can be quite
large. We can also join it to other CDM table or subset the entire cdm
to just the persons in the cohort.

``` r
cdm_gibleed <- cdm %>% 
  cdmSubsetCohort(cohortTable = "study_cohorts")
```

### Subset a cohort

Suppose you have a generated cohort and you would like to create a new
cohort that is a subset of the first. This can be done using the

First we will generate an example cohort set and then create a new
cohort based on filtering the Atlas cohort.

``` r
library(CDMConnector)
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main")

cohortSet <- readCohortSet(system.file("cohorts3", package = "CDMConnector"))


cdm <- generateCohortSet(cdm, cohortSet, name = "cohort") 

cdm$cohort

cohortCount(cdm$cohort)
```

As an example we will take only people in the cohort that have a cohort
duration that is longer than 4 weeks. Using dplyr we can write this
query and save the result in a new table in the cdm.

``` r
library(dplyr)

cdm$cohort_subset <- cdm$cohort %>% 
  # only keep persons who are in the cohort at least 28 days
  filter(!!datediff("cohort_start_date", "cohort_end_date") >= 28) %>% 
  compute(name = "cohort_subset", temporary = FALSE, overwrite = TRUE) %>% 
  newCohortTable()

cohortCount(cdm$cohort_subset)
```

In this case we can see that cohorts 1 and 5 were dropped completely and
some patients were dropped from cohorts 2, 3, and 4.

Let’s confirm that everyone in cohorts 1 and 5 were in the cohort for
less than 28 days.

``` r
daysInCohort <- cdm$cohort %>% 
  filter(cohort_definition_id %in% c(1,5)) %>% 
  mutate(days_in_cohort = !!datediff("cohort_start_date", "cohort_end_date")) %>% 
  count(cohort_definition_id, days_in_cohort) %>% 
  collect()

daysInCohort
```

We have confirmed that everyone in cohorts 1 and 5 were in the cohort
less than 10 days.

Now suppose we would like to create a new cohort table with three
different versions of the cohorts in the original cohort table. We will
keep persons who are in the cohort at 2 weeks, 3 weeks, and 4 weeks. We
can simply write some custom dplyr to create the table and then call
`new_generated_cohort_set` just like in the previous example.

``` r

cdm$cohort_subset <- cdm$cohort %>% 
  filter(!!datediff("cohort_start_date", "cohort_end_date") >= 14) %>% 
  mutate(cohort_definition_id = 10 + cohort_definition_id) %>% 
  union_all(
    cdm$cohort %>%
    filter(!!datediff("cohort_start_date", "cohort_end_date") >= 21) %>% 
    mutate(cohort_definition_id = 100 + cohort_definition_id)
  ) %>% 
  union_all(
    cdm$cohort %>% 
    filter(!!datediff("cohort_start_date", "cohort_end_date") >= 28) %>% 
    mutate(cohort_definition_id = 1000 + cohort_definition_id)
  ) %>% 
  compute(name = "cohort_subset", temporary = FALSE, overwrite = TRUE) # %>% 
  # newCohortTable() # this function creates the cohort object and metadata

cdm$cohort_subset %>% 
  mutate(days_in_cohort = !!datediff("cohort_start_date", "cohort_end_date")) %>% 
  group_by(cohort_definition_id) %>% 
  summarize(mean_days_in_cohort = mean(days_in_cohort, na.rm = TRUE)) %>% 
  collect() %>% 
  arrange(mean_days_in_cohort)
```

This is an example of creating new cohorts from existing cohorts using
CDMConnector. There is a lot of flexibility with this approach. Next we
will look at completely custom cohort creation which is quite similar.

### Custom Cohort Creation

Sometimes you may want to create cohorts that cannot be easily expressed
using Atlas or Capr. In these situations you can create implement cohort
creation using SQL or R. See the chapter in [The Book of
OHDSI](https://ohdsi.github.io/TheBookOfOhdsi/Cohorts.html#implementing-the-cohort-using-sql)
for details on using SQL to create cohorts. CDMConnector provides a
helper function to build simple cohorts from a list of OMOP concepts.
`generate_concept_cohort_set` accepts a named list of concept sets and
will create cohorts based on those concept sets. While this function
does not allow for inclusion/exclusion criteria in the initial
definition, additional criteria can be applied “manually” after the
initial generation.

``` r

library(dplyr, warn.conflicts = FALSE)

cdm <- generateConceptCohortSet(
  cdm, 
  conceptSet = list(gibleed = 192671), 
  name = "gibleed2", # name of the cohort table
  limit = "all", # use all occurrences of the concept instead of just the first
  end = 10 # set explicit cohort end date 10 days after start
)

cdm$gibleed2 <- cdm$gibleed2 %>% 
  semi_join(
    filter(cdm$person, gender_concept_id == 8507), 
    by = c("subject_id" = "person_id")
  ) %>% 
  recordCohortAttrition(reason = "Male")
  
attrition(cdm$gibleed2) 
```

In the above example we built a cohort table from a concept set. The
cohort essentially captures patient-time based off of the presence or
absence of OMOP standard concept IDs. We then manually applied an
inclusion criteria and recorded a new attrition record in the cohort. To
learn more about this approach to building cohorts check out the
[PatientProfiles](https://darwin-eu.github.io/PatientProfiles/) R
package.

You can also create a generated cohort set using any method you choose.
As long as the table is in the CDM database and has the four required
columns it can be added to the CDM object as a generated cohort set.

Suppose for example our cohort table is

``` r
cohort <- dplyr::tibble(
  cohort_definition_id = 1L,
  subject_id = 1L,
  cohort_start_date = as.Date("1999-01-01"),
  cohort_end_date = as.Date("2001-01-01")
)

cohort
```

First make sure the table is in the database and create a dplyr table
reference to it and add it to the CDM object.

``` r
library(omopgenerics)
cdm <- insertTable(cdm = cdm, name = "cohort", table = cohort, overwrite = TRUE)

cdm$cohort
```

To make this a true generated cohort object use the `cohort_table`

``` r
cdm$cohort <- newCohortTable(cdm$cohort)
```

We can see that this cohort is now has the class “cohort_table” as well
as the various metadata tables.

``` r
cohortCount(cdm$cohort)
settings(cdm$cohort)
attrition(cdm$cohort)
```

If you would like to override the attribute tables then pass additional
dataframes to cohortTable

``` r
cdm <- insertTable(cdm = cdm, name = "cohort2", table = cohort, overwrite = TRUE)
cdm$cohort2 <- newCohortTable(cdm$cohort2)
settings(cdm$cohort2)

cohort_set <- data.frame(cohort_definition_id = 1L,
                         cohort_name = "made_up_cohort")
cdm$cohort2 <- newCohortTable(cdm$cohort2, cohortSetRef = cohort_set)

settings(cdm$cohort2)
```

``` r
DBI::dbDisconnect(con, shutdown = TRUE)
```

Cohort building is a fundamental building block of observational health
analysis and CDMConnector supports different ways of creating cohorts.
As long as your cohort table is has the required structure and columns
you can add it to the cdm with the `new_generated_cohort_set` function
and use it in any downstream OHDSI analytic packages.
