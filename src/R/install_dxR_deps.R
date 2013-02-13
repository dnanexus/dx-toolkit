if("RCurl" %in% rownames(installed.packages()) == FALSE) {
  install.packages("RCurl", lib=.Library.site[1], dependencies=TRUE, repos="http://cran.us.r-project.org")
}
if("RJSONIO" %in% rownames(installed.packages()) == FALSE) {
  install.packages("RJSONIO", lib=.Library.site[1], dependencies=TRUE, repos="http://cran.us.r-project.org")
}
if("roxygen2" %in% rownames(installed.packages()) == FALSE) {
  install.packages("roxygen2", lib=.Library.site[1], dependencies=TRUE, repos="http://cran.us.r-project.org")
}
