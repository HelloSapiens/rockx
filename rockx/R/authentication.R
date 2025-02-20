#' Set Username, Password, and Server URL for API Authentication
#'
#' This function sets the environment variables `ROCKX_USERNAME`, `ROCKX_PASSWORD`, and `ROCKX_SERVER_URL` for authentication and specifying the ODK-X Sync Endpoint.
#'
#' @param username A character string containing the API username.
#' @param password A character string containing the API password.
#' @param server_url A character string containing the URL of the ODK-X sync endpoint (e.g., "https://my.sync-endpoint.com").
#'
#' @return A message confirming that credentials and server URL have been set.
#' @examples
#' rockx::set_username_and_password("my_user", "my_password", "https://my.sync-endpoint.com")
#' 
#' @export
set_username_and_password <- function(username, password, server_url) {
  if (missing(username) || missing(password) || missing(server_url)) {
    stop("Username, password, and server_url are required.")
  }  
  
  # Optionally, check that server_url is a non-empty string.
  if (!is.character(server_url) || length(server_url) != 1 || nchar(server_url) == 0) {
    stop("'server_url' must be a non-empty string.")
  }
  
  server_url <- tolower(server_url) |> gsub("/odktables.*|/+$", "", x = _)
  Sys.setenv(ROCKX_USERNAME = username)
  Sys.setenv(ROCKX_PASSWORD = password)
  Sys.setenv(ROCKX_SERVER_URL = server_url)
  
  message("Credentials and server URL have been set for the session.")
}

#' Retrieve API Credentials
#'
#' This function fetches the stored API credentials from environment variables.
#'
#' @return A list containing username, password, and server_url.
#' @export
get_credentials <- function() {
  username <- Sys.getenv("ROCKX_USERNAME", unset = NA)
  password <- Sys.getenv("ROCKX_PASSWORD", unset = NA)
  server_url <- Sys.getenv("ROCKX_SERVER_URL", unset = NA)
  
  if (is.na(username) || is.na(password) || is.na(server_url)) {
    stop("Username, password, or server URL not set. Please call rockx::set_username_and_password().")
  }
  
  list(username = username, password = password, server_url = server_url)
}
