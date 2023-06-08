## R CMD check results

This is a new major release.

One NOTE is expected:
Suggests or Enhances not in mainstream repositories: CirceR

RCMD check passed on the following platforms with 0 errors, 0 warnings, and 0 notes 

 Github Actions runners
  - {os: macOS-latest,   r: 'release'}
  - {os: windows-latest, r: 'release'}
  - {os: ubuntu-latest,   r: 'release'}
  - {os: ubuntu-latest,   r: 'devel'}

Mac OS 12.6 running R 4.2.2

I also checked on winbuilder and saw an unexpected note (copied below)

checking dependencies in R code ... NOTE
		#STDOFF	2:05:08.9
		#STDOFF	8:05:43.2
		#STDOFF	7:36:41.7
		#STDOFF	-0:25:21.1
		...
		
I think this is coming from the arrow package. 
See https://github.com/apache/arrow/issues/35594
Let me know if I need to do something about it. Thank you.


