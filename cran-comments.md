## R CMD check results

This is a new minor release that fixes the failing test seen for the CRAN check on r-devel-linux-x86_64-fedora-gcc. 

One NOTE is expected:
Suggests or Enhances not in mainstream repositories: Capr

RCMD check passed on the following platforms with 0 errors, 0 warnings, and 0 notes 

 Github Actions runners
  - {os: macOS-latest,   r: 'release'}
  - {os: windows-latest, r: 'release'}
  - {os: ubuntu-latest,   r: 'release'}
  - {os: ubuntu-latest,   r: 'devel'}

Mac OS 12.6 running R 4.3.1

I also checked on winbuilder.

