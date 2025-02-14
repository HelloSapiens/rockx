test_that("get_credentials retrieves stored values", {
  Sys.setenv(ROCKX_USERNAME = "test_user", ROCKX_PASSWORD = "test_pass", ROCKX_SERVER_URL = "http://test.my.url")
  credentials <- get_credentials()
  expect_equal(credentials$username, "test_user")
  expect_equal(credentials$password, "test_pass")
  expect_equal(credentials$server_url, "http://test.my.url")
  
})
