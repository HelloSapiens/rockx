test_that("set_username_and_password stores credentials", {
  set_username_and_password("test_user", "test_pass", "http://test.my.url")
  expect_equal(Sys.getenv("ROCKX_USERNAME"), "test_user")
  expect_equal(Sys.getenv("ROCKX_PASSWORD"), "test_pass")
  expect_equal(Sys.getenv("ROCKX_SERVER_URL"), "http://test.my.url")
  
})
