library(dxR)

context("DXGTable-integration")

idsToDestroy <- vector()

# TODO: make this test even better by replacing the API wrapper usage
# with something better.  Also, column specs.
test_that("describe and names works", {
  inputHash <- list(project=dxR:::dxEnv$DEFAULT_PROJECT,
                    columns=list(colDesc("foo", "string")))
  id <- gtableNew(inputHash)[["id"]]
  idsToDestroy <<- c(idsToDestroy, id)
  handler <- DXGTable(id)
  expect_that(handler@desc[["id"]], equals(id))
  expect_that(length(names(handler)), equals(1))
  expect_that(names(handler)[1], equals("foo"))
})

test_that("newDXGTable works", {
  handler <- newDXGTable(columns=list(colDesc("foo", "string"),
                           colDesc("bar", "int")))
  idsToDestroy <<- c(idsToDestroy, id(handler))
  expect_that(length(names(handler)), equals(2))
  expect_that(names(handler)[1], equals("foo"))
  expect_that(names(handler)[2], equals("bar"))
})

test_that("addRow and flushRows work for single-column gtables", {
  handler <- newDXGTable(columns=list(colDesc("foo", "string")))
  idsToDestroy <<- c(idsToDestroy, id(handler))

  # Flush a single row
  addRow(handler, list("foo"))
  flushRows(handler)

  # Flush multiple rows
  addRow(handler, list("bar"))
  addRow(handler, list("baz"))
  flushRows(handler)

  closeObj(handler, block=TRUE)

  desc(handler) <- describe(handler)
  expect_that(nrow(handler), equals(3))
  df <- getRows(handler)

  expectedFooCol <- c("foo", "bar", "baz")
  for (i in 1:nrow(handler)) {
    expect_that(as.character(df$foo[i]), equals(expectedFooCol[i]))
  }
})

test_that("addRow and flushRows work for single-column gtables; allowing use of c instead of list", {
  handler <- newDXGTable(columns=list(colDesc("foo", "string")))
  idsToDestroy <<- c(idsToDestroy, id(handler))

  # Flush a single row
  addRow(handler, c("foo"))
  flushRows(handler)

  # Flush multiple rows
  addRow(handler, c("bar"))
  addRow(handler, c("baz"))
  flushRows(handler)

  closeObj(handler, block=TRUE)

  desc(handler) <- describe(handler)
  expect_that(nrow(handler), equals(3))
  df <- getRows(handler)

  expectedFooCol <- c("foo", "bar", "baz")
  for (i in 1:nrow(handler)) {
    expect_that(as.character(df$foo[i]), equals(expectedFooCol[i]))
    # Test that row IDs as names works
    expect_that(as.character(df[[as.character(i-1), "foo"]]), equals(expectedFooCol[i]))
  }
})

test_that("addRow and flushRows work for multi-column gtables", {
  handler <- newDXGTable(columns=list(colDesc("foo", "string"),
                           colDesc("anint", "int")))
  idsToDestroy <<- c(idsToDestroy, id(handler))

  # Flush a single row
  addRow(handler, list("foo", 0))
  flushRows(handler)

  # Flush multiple rows
  addRow(handler, list("bar", 1))
  addRow(handler, list("baz", 2))
  flushRows(handler)

  closeObj(handler, block=TRUE)

  desc(handler) <- describe(handler)
  expect_that(nrow(handler), equals(3))
  df <- getRows(handler)

  expectedFooCol <- c("foo", "bar", "baz")
  expectedAnintCol <- 0:2
  for (i in 1:nrow(handler)) {
    expect_that(as.character(df$foo[i]), equals(expectedFooCol[i]))
    expect_that(df$anint[i], equals(expectedAnintCol[i]))
  }
})

test_that("addRows works for single-column gtables", {
  handler <- newDXGTable(columns=list(colDesc("foo", "string")))
  idsToDestroy <<- c(idsToDestroy, id(handler))

  # Flush a single row
  addRows(handler, data.frame(foo=c("foo")))
  flushRows(handler)

  # Flush multiple rows
  addRows(handler, data.frame(foo=c("bar", "baz")))
  addRows(handler, data.frame(foo=c("quux")))
  flushRows(handler)

  addRows(handler, data.frame(foo=c("quux")))
  addRows(handler, data.frame(foo=c("bar", "baz")))
  flushRows(handler)

  closeObj(handler, block=TRUE)

  desc(handler) <- describe(handler)
  expect_that(nrow(handler), equals(7))
  expect_that(ncol(handler), equals(1))
  df <- getRows(handler)

  expectedFooCol <- c("foo", "bar", "baz", "quux", "quux", "bar", "baz")
  for (i in 1:nrow(handler)) {
    expect_that(as.character(df$foo[i]), equals(expectedFooCol[i]))
  }
})

test_that("addRows works for multi-column gtables", {
  handler <- newDXGTable(columns=list(colDesc("foo", "string"),
                           colDesc("anint", "int")))
  idsToDestroy <<- c(idsToDestroy, id(handler))

  # Flush a single row
  addRows(handler, data.frame(foo=c("foo"), anint=c(0)))
  flushRows(handler)

  # Flush multiple rows
  addRows(handler, data.frame(foo=c("bar", "baz"), anint=c(1, 2)))
  addRows(handler, data.frame(foo=c("quux"), anint=c(3)))
  flushRows(handler)

  addRows(handler, data.frame(foo=c("quux"), anint=c(4)))
  addRows(handler, data.frame(foo=c("bar", "baz"), anint=c(5, 6)))
  flushRows(handler)

  closeObj(handler, block=TRUE)

  desc(handler) <- describe(handler)
  expect_that(nrow(handler), equals(7))
  expect_that(ncol(handler), equals(2))
  df <- getRows(handler)

  expectedFooCol <- c("foo", "bar", "baz", "quux", "quux", "bar", "baz")
  expectedAnintCol <- 0:6
  for (i in 1:nrow(handler)) {
    expect_that(as.character(df$foo[i]), equals(expectedFooCol[i]))
    expect_that(df$anint[i], equals(expectedAnintCol[i]))
  }
})

test_that("closeObj can be called without block set nor adding any rows", {
  handler <- newDXGTable(columns=list(colDesc("foo", "string"),
                           colDesc("anint", "int")))
  idsToDestroy <<- c(idsToDestroy, id(handler))
  closeObj(handler)
})

test_that("getRows works for columns, starting, and limit", {
  # Create a simple gtable
  # Get rows from it
  warning("TODO: WRITE ME (getRows test)", call.=FALSE)
})

test_that("GRI table can be created and queried", {
  handler <- newDXGTable(columns=list(colDesc("chr", "string"),
                           colDesc("lo", "int"),
                           colDesc("hi", "int")),
                         indices=list(genomicRangeIndex("chr", "lo", "hi")))
  idsToDestroy <<- c(idsToDestroy, id(handler))
  df <- data.frame(chr=c("chrII", "chrI", "chrI"), lo=c(1000, 500, 300), hi=c(1010, 800, 600),
                   stringsAsFactors=FALSE)
  addRows(handler, df)
  closeObj(handler, block=TRUE)
  storedRows <- getRows(handler)
  expect_that(storedRows["0", "chr"], equals("chrI"))
  expect_that(storedRows[3, "chr"], equals("chrII"))
})

test_that("lexicographic indexed table can be created and queried", {
  handler <- newDXGTable(columns=list(colDesc("gene", "string"),
                           colDesc("anint", "int")),
                         indices=list(lexicographicIndex(
                           columns=list(lexicographicIndexColumn("gene", caseSensitive=FALSE)),
                           name="myindex")))
  idsToDestroy <<- c(idsToDestroy, id(handler))
  df <- data.frame(gene=c("Baa", "BCC", "aAA", "Abb"), anint=1:4)
  addRows(handler, df)
  closeObj(handler, block=TRUE)
  storedRows <- getRows(handler)
  expect_that(storedRows$anint, equals(c(3, 4, 1, 2)))
})

test_that("head returns first n rows (nrow < n)", {
  # Test when there are < n rows
  handler <- newDXGTable(columns=list(colDesc("foo", "string"),
                           colDesc("anint", "int")))
  idsToDestroy <<- c(idsToDestroy, id(handler))
  addRows(handler, data.frame(foo=c("bar", "baz"), anint=c(1, 2)))
  closeObj(handler, block=TRUE)

  headResult <- head(handler)
  expect_that(nrow(headResult), equals(2))
  expect_that(ncol(headResult), equals(2))
  headResult <- head(handler, n=3)
  expect_that(nrow(headResult), equals(2))
})

test_that("head returns first n rows (nrow > n)", {
  # Test when there are > n rows
  handler <- newDXGTable(columns=list(colDesc("foo", "string"),
                           colDesc("anint", "int")))
  idsToDestroy <<- c(idsToDestroy, id(handler))
  addRows(handler, data.frame(foo=sapply(1:10, as.character), anint=1:10))
  closeObj(handler, block=TRUE)

  headResult <- head(handler)
  expect_that(nrow(headResult), equals(6))
  for (i in 1:6) {
    expect_that(headResult$foo[i], equals(as.character(i)))
    expect_that(headResult$anint[i], equals(i))
  }
  headResult <- head(handler, n=3)
  expect_that(nrow(headResult), equals(3))
  for (i in 1:3) {
    expect_that(headResult$foo[i], equals(as.character(i)))
    expect_that(headResult$anint[i], equals(i))
  }

  # Test when n = 0
  headResult <- head(handler, n=0)
  expect_that(nrow(headResult), equals(0))
  expect_that(ncol(headResult), equals(2))
  expect_that(names(headResult)[1], equals("foo"))
  expect_that(names(headResult)[2], equals("anint"))
})

test_that("head returns first nrow + n rows (n < 0)", {
  # Test when n < 0
  handler <- newDXGTable(columns=list(colDesc("foo", "string"),
                           colDesc("anint", "int")))
  idsToDestroy <<- c(idsToDestroy, id(handler))
  addRows(handler, data.frame(foo=sapply(1:10, as.character), anint=1:10))
  closeObj(handler, block=TRUE)

  headResult <- head(handler, n=-3)
  expect_that(nrow(headResult), equals(7))
  for (i in 1:7) {
    expect_that(headResult$foo[i], equals(as.character(i)))
    expect_that(headResult$anint[i], equals(i))
  }

  headResult <- head(handler, n=-10)
  expect_that(nrow(headResult), equals(0))
  expect_that(ncol(headResult), equals(2))
  expect_that(names(headResult)[1], equals("foo"))
  expect_that(names(headResult)[2], equals("anint"))

  headResult <- head(handler, n=-12)
  expect_that(nrow(headResult), equals(0))
  expect_that(ncol(headResult), equals(2))
  expect_that(names(headResult)[1], equals("foo"))
  expect_that(names(headResult)[2], equals("anint"))
})

test_that("tail returns last n rows (nrow < n)", {
  # Test when there are < n rows
  handler <- newDXGTable(columns=list(colDesc("foo", "string"),
                           colDesc("anint", "int")))
  idsToDestroy <<- c(idsToDestroy, id(handler))
  addRows(handler, data.frame(foo=c("bar", "baz"), anint=c(1, 2)))
  closeObj(handler, block=TRUE)

  tailResult <- tail(handler)
  expect_that(nrow(tailResult), equals(2))
  expect_that(ncol(tailResult), equals(2))
  tailResult <- tail(handler, n=3)
  expect_that(nrow(tailResult), equals(2))
})

test_that("tail returns last n rows (nrow > n)", {
  # Test when there are > n rows
  handler <- newDXGTable(columns=list(colDesc("foo", "string"),
                           colDesc("anint", "int")))
  idsToDestroy <<- c(idsToDestroy, id(handler))
  addRows(handler, data.frame(foo=sapply(1:10, as.character), anint=1:10))
  closeObj(handler, block=TRUE)

  tailResult <- tail(handler)
  expect_that(nrow(tailResult), equals(6))
  for (i in 1:6) {
    expect_that(tailResult$foo[i], equals(as.character(i + 4)))
    expect_that(tailResult$anint[i], equals(i + 4))
  }
  tailResult <- tail(handler, n=3)
  expect_that(nrow(tailResult), equals(3))
  for (i in 1:3) {
    expect_that(tailResult$foo[i], equals(as.character(i + 7)))
    expect_that(tailResult$anint[i], equals(i + 7))
  }

  # Test when n = 0
  tailResult <- tail(handler, n=0)
  expect_that(nrow(tailResult), equals(0))
  expect_that(ncol(tailResult), equals(2))
  expect_that(names(tailResult)[1], equals("foo"))
  expect_that(names(tailResult)[2], equals("anint"))
})

test_that("tail returns last nrow + n rows (n < 0)", {
  # Test when n < 0
  # Test when there are > n rows
  handler <- newDXGTable(columns=list(colDesc("foo", "string"),
                           colDesc("anint", "int")))
  idsToDestroy <<- c(idsToDestroy, id(handler))
  addRows(handler, data.frame(foo=sapply(1:10, as.character), anint=1:10))
  closeObj(handler, block=TRUE)

  tailResult <- tail(handler, n=-3)
  expect_that(nrow(tailResult), equals(7))
  for (i in 1:7) {
    expect_that(tailResult$foo[i], equals(as.character(i + 3)))
    expect_that(tailResult$anint[i], equals(i + 3))
  }

  tailResult <- tail(handler, n=-10)
  expect_that(nrow(tailResult), equals(0))
  expect_that(ncol(tailResult), equals(2))
  expect_that(names(tailResult)[1], equals("foo"))
  expect_that(names(tailResult)[2], equals("anint"))

  tailResult <- tail(handler, n=-12)
  expect_that(nrow(tailResult), equals(0))
  expect_that(ncol(tailResult), equals(2))
  expect_that(names(tailResult)[1], equals("foo"))
  expect_that(names(tailResult)[2], equals("anint"))
})

############
# TEARDOWN #
############

# Do NOT put any tests after this line.

# Ignore any errors
tryCatch({
  dxHTTPRequest(paste("/", dxR:::dxEnv$DEFAULT_PROJECT, "/removeObjects", sep=""), list(objects=I(idsToDestroy)))
}, error=function(e) {
  NULL
})
