if("RCurl" %in% rownames(installed.packages()) == FALSE) {
  install.packages("RCurl", lib=.Library.site[1], repos="http://cran.us.r-project.org")
}
if("RJSONIO" %in% rownames(installed.packages()) == FALSE) {
  install.packages("RJSONIO", lib=.Library.site[1], repos="http://cran.us.r-project.org")
}
if("testthat" %in% rownames(installed.packages()) == FALSE) {
  install.packages("testthat", lib=.Library.site[1], repos="http://cran.us.r-project.org")
}

if("digest" %in% rownames(installed.packages()) == FALSE) {
  install.packages("digest", lib=.Library.site[1], repos="http://cran.us.r-project.org")
}
if("stringr" %in% rownames(installed.packages()) == FALSE) {
  install.packages("stringr", lib=.Library.site[1], repos="http://cran.us.r-project.org")
}
if("brew" %in% rownames(installed.packages()) == FALSE) {
  install.packages("brew", lib=.Library.site[1], repos="http://cran.us.r-project.org")
}
download.file(
  "http://cran.r-project.org/src/contrib/Archive/roxygen2/roxygen2_2.2.2.tar.gz",
  "roxygen2_2.2.2"
)
if("roxygen2" %in% rownames(installed.packages()) == FALSE) {
  install.packages("roxygen2_2.2.2", lib=.Library.site[1], repos=NULL, type="source")
}
