# rockx - R package for ODK-X Sync Endpoint

This repo contains the source code for the R package `rockx`.

## How to:

```r
install.packages('rockx')
library(rockx)
```

## Authentication

Before making any API calls, you need to set your API credentials and the sync endpoint URL. Use the `set_username_and_password()` function to configure these details:

```r
# Set your credentials and server URL
set_username_and_password("my_user", "my_password", "https://my.sync-endpoint.com")

# You can then verify your credentials and check that the user is authorized to download data (must be a SITE ADMIN) with has_access():
if (!has_access()) {
  stop("User is not authorized")
}
```

This function stores your username, password, and server URL as environment variables for use in subsequent API calls.

## Fetching Tables

After authentication, you can retrieve a list of tables from the sync endpoint. The get_tables() function makes a paginated GET request to the API and returns a tibble containing details for each table:
```r
# Retrieve the list of tables from the sync endpoint
tables <- get_tables()
print(tables)

```

## Fetching rows
```r
# Retrieve the rows of the table my_table. The function handles pagination automatically by
# iteratively requesting additional pages if the server indicates there are more results.
# It will then do some basic type casting to numbers, chr, etc.
rows <- get_all_rows("my_table")

```

## Fetching images/attachments
```r

# In order for the download mechanism to be efficient, we will first get the
# meta data for the table, and store it in an object we pass to the download function
meta <- get_table_metadata("my_table")

# The command below will download attachments for all rows in 'rows' and save them to
# the folder "./data". 
# The argument skip_if_instance_folder_exists means that if a folder in "./data/<id of row>"
# exists, the function will skip downloading for that row.
download_attachments(rows, meta, "data", skip_if_instance_folder_exists = TRUE)

```

## Conclusion  

rockx aims to provide a seamless interface for interacting with ODK-X Sync Endpoints in R. By abstracting the details of API communication, the package allows you to focus on analyzing your data. This vignette has provided an overview of the current functionality, and we welcome feedback as we continue to develop and enhance the package.

Enjoy using rockx, and happy data exploring!