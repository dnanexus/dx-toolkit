if("RCurl" %in% rownames(installed.packages()) == FALSE) {
  install.packages("RCurl", lib=.Library.site[1], repos="http://cran.us.r-project.org")
}
if("RJSONIO" %in% rownames(installed.packages()) == FALSE) {
  install.packages("RJSONIO", lib=.Library.site[1], repos="http://cran.us.r-project.org")
}
if("roxygen2" %in% rownames(installed.packages()) == FALSE) {
  install.packages("roxygen2", lib=.Library.site[1], repos="http://cran.us.r-project.org")
}
if("testthat" %in% rownames(installed.packages()) == FALSE) {
  install.packages("testthat", lib=.Library.site[1], repos="http://cran.us.r-project.org")
}
