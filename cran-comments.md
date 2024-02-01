## R CMD check results

This is a new minor release. This package will break tests in DrugUtilisation,
IncidencePrevalence, PatientProfiles, DrugExposureDiagnostics. The maintiners have 
been notified and will updated these packages as soon as this version of CDMConnector is
accepted by CRAN. Even though tests in these packages break we still consider this release
non-breaking because the external user interface for is still supported for the most part.

One NOTE is expected:
Suggests or Enhances not in mainstream repositories: CirceR, Capr

RCMD check passed on the following platforms with 0 errors, 0 warnings, and 0 notes 

 Github Actions runners
  - {os: macOS-latest,   r: 'release'}
  - {os: windows-latest, r: 'release'}
  - {os: ubuntu-latest,   r: 'release'}
  - {os: ubuntu-latest,   r: 'devel'}

Mac OS 12.6 running R 4.3.1

I also checked on winbuilder.

