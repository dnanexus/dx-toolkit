if("devtools" %in% rownames(installed.packages()) == FALSE) {
  install.packages("devtools", lib=.Library.site[1], repos="http://cran.us.r-project.org")
}

if("staticdocs" %in% rownames(installed.packages()) == FALSE) {
  library("devtools")
  local({r <- getOption("repos")
         r["CRAN"] <- "http://cran.r-project.org" 
         options(repos=r)
  })
  if("highlight" %in% rownames(installed.packages()) == FALSE) {
    install_url("http://cran.r-project.org/src/contrib/Archive/highlight/highlight_0.3.2.tar.gz")
  }
  install_github(username="kjlai", repo="staticdocs", ref="2b91c0bc66a5e6e30f8b76b9885fcfe2dcdf544d")
#  install_github(username="hadley", repo="staticdocs", ref="d0cc8d1b5dacdaa5ba58888e9673527458b002c4")
}
