library(covr)
cat("Running coverage measurement...\n")
cov <- package_coverage(type = "tests", quiet = FALSE)

# Save as Cobertura XML for codecov upload
covr::to_cobertura(cov, filename = "coverage.xml")
cat("\nSaved coverage.xml for codecov upload\n")

pct <- percent_coverage(cov)
cat(sprintf("\nOverall coverage: %.1f%%\n\n", pct))

df <- tally_coverage(cov)
for (fn in sort(unique(df$filename))) {
  sub <- df[df$filename == fn, ]
  total <- nrow(sub)
  covered <- sum(sub$value > 0)
  cat(sprintf("  %-45s %5.1f%% (%d/%d)\n", fn, 100*covered/total, covered, total))
}
