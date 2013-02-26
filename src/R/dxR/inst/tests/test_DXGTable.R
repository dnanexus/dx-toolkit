context("DXGTable")

test_that("constructor works", {
  dxgtable <- DXGTable("gtable-12345678901234567890abcd",
                       describe=FALSE)
  expect_that(dxgtable@id, equals("gtable-12345678901234567890abcd"))

  dxgtable <- DXGTable("gtable-12345678901234567890abcd",
                       project="project-12345678901234567890dcba",
                       describe=FALSE)
  expect_that(dxgtable@id, equals("gtable-12345678901234567890abcd"))
  expect_that(dxgtable@project, equals("project-12345678901234567890dcba"))

  dxgtable <- DXGTable("gtable-12345678901234567890abcd",
                       project="container-12345678901234567890dcba",
                       describe=FALSE)
  expect_that(dxgtable@id, equals("gtable-12345678901234567890abcd"))
  expect_that(dxgtable@project, equals("container-12345678901234567890dcba"))
})

test_that("validator works", {
  expect_that(DXGTable("gtable-12345678901234567890abc!", describe=FALSE),
              throws_error("invalid class"))
  expect_that(DXGTable("foo-12345678901234567890abcd", describe=FALSE),
              throws_error("invalid class"))
  expect_that(DXGTable("gtable-12345678901234567890abcd",
                       project="gtable-12345678901234567890abcd",
                       describe=FALSE),
              throws_error("invalid class"))
})

test_that("id method works", {
  dxgtable <- DXGTable("gtable-12345678901234567890abcd", describe=FALSE)
  expect_that(id(dxgtable), equals("gtable-12345678901234567890abcd"))
})

test_that("desc<- and desc methods work", {
  dxgtable <- DXGTable("gtable-12345678901234567890abcd", describe=FALSE)
  desc(dxgtable) <- list(foo="foo", bar="bar")
  expect_that(desc(dxgtable)[["foo"]], equals("foo"))
  expect_that(desc(dxgtable)[["bar"]], equals("bar"))
})

test_that("colDesc makes a valid column descriptor", {
  someCol <- colDesc("colname", "int")
  expect_that(someCol$name, equals("colname"))
  expect_that(someCol$type, equals("int"))
})
