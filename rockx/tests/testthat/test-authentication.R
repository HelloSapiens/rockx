testthat::test_that("set_username_and_password stores credentials", {
  suppressMessages(set_username_and_password("test_user", "test_pass", "http://test.my.url"))
  expect_equal(Sys.getenv("ROCKX_USERNAME"), "test_user")
  expect_equal(Sys.getenv("ROCKX_PASSWORD"), "test_pass")
  expect_equal(Sys.getenv("ROCKX_SERVER_URL"), "http://test.my.url") 
})


testthat::test_that("set_username_and_password updates credentials", {
  suppressMessages(set_username_and_password("test_user", "test_pass", "http://test.my.url"))
  expect_equal(Sys.getenv("ROCKX_USERNAME"), "test_user")
  expect_equal(Sys.getenv("ROCKX_PASSWORD"), "test_pass")
  expect_equal(Sys.getenv("ROCKX_SERVER_URL"), "http://test.my.url") 
  suppressMessages(set_username_and_password("test_userB", "test_passB", "http://test.my.urlB"))
  expect_equal(Sys.getenv("ROCKX_USERNAME"), "test_userB")
  expect_equal(Sys.getenv("ROCKX_PASSWORD"), "test_passB")
  expect_equal(Sys.getenv("ROCKX_SERVER_URL"), "http://test.my.urlb") #server gets lowercased
  
})
