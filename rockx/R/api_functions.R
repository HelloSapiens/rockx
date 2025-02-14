#' @importFrom dplyr filter mutate select as_tibble everything ends_with pull bind_cols
#' @importFrom tidyr pivot_longer
#' @importFrom purrr map_df pmap
#' @importFrom rlang .data

# Helper functions -------------------------------------------------------

.format_date <- function(date) {
  return(format(date, "%Y-%m-%dT%H:%M:%OS3"))
}


# Internal helper for handling HTTP errors.
.handle_response <- function(response) {
  if (httr::status_code(response) != 200) {
    stop("Failed to retrieve data. HTTP status: ", 
         httr::status_code(response),
         ". Message: ", httr::content(response, as = "text", encoding = "UTF-8"))
  }
}

# Internal helper for paginated GET requests.
.paginate_get <- function(endpoint, username, password, extract_fn, cursor_fn) {
  all_results <- list()
  cursor <- NULL
  has_more <- TRUE
  
  while (has_more) {
    url <- if (is.null(cursor)) {
      endpoint
    } else {
      paste0(endpoint, "?cursor=", utils::URLencode(cursor, reserved = TRUE))
    }
    
    response <- httr::GET(url,
                          httr::authenticate(username, password, type = "basic"),
                          httr::add_headers(
                            `User-Agent` = "rockx-odkx-client",
                            `Content-Type` = "application/json"
                          ))
    
    .handle_response(response)
    
    parsed <- jsonlite::fromJSON(httr::content(response, as = "text", encoding = "UTF-8"))
    
    all_results <- c(all_results, extract_fn(parsed))
    
    has_more <- isTRUE(parsed$hasMoreResults)
    cursor <- if (has_more) cursor_fn(parsed) else NULL
  }
      
  return(all_results)
}

.convert_column_list <- function(all_results) {
  ordered_columns <- all_results$orderedColumns |> map_df(~ pivot_wider(.x, names_from = column, values_from = value))
  other_columns <- all_results[names(all_results) != "orderedColumns" & names(all_results) != "filterScope"] |>  as_tibble()
  all_results <- bind_cols(other_columns, ordered_columns)
  return (all_results)
}

.get_table_schema <- function(table_meta) {
  creds <- get_credentials()
  username <- creds$username
  password <- creds$password
  
  # Get the schema from the definitionUri endpoint.
  schema_response <- httr::GET(
    table_meta$definitionUri,
    httr::authenticate(username, password, type = "basic"),
    httr::add_headers(
      `User-Agent` = "rockx-odkx-client",
      `Content-Type` = "application/json"
    )
  )
  .handle_response(schema_response)
  schema <- jsonlite::fromJSON(httr::content(schema_response, as = "text", encoding = "UTF-8"))
  return(schema)
}

.parse_schema_types <- function(table_meta, flattened_rows) {
  schema <- .get_table_schema(table_meta)

  # Iterate over each column in the schema.
  pmap(schema$orderedColumns, function(elementKey, elementName, elementType, listChildElementKeys) {
    key <- elementKey
    type <- elementType
    child_keys <- jsonlite::fromJSON(listChildElementKeys)  # Parse JSON string
    
    # If there are composed (child) elements...
    if (length(child_keys) > 0) {
      if (type == "geopoint") {
        # For geopoint, combine child elements (latitude and longitude) into a WKT representation.
        lat_key <- child_keys[grepl("latitude", child_keys, ignore.case = TRUE)]
        lon_key <- child_keys[grepl("longitude", child_keys, ignore.case = TRUE)]
        if (length(lat_key) > 0 && length(lon_key) > 0 &&
            lat_key %in% names(flattened_rows) &&
            lon_key %in% names(flattened_rows)) {
          # Create a new column using a WKT "POINT(lon lat)" format.
          flattened_rows[[key]] <- sprintf("POINT(%s %s)", 
                                           flattened_rows[[lon_key]], 
                                           flattened_rows[[lat_key]])
          # Remove the individual child columns.
          #flattened_rows <<- flattened_rows[, !names(flattened_rows) %in% child_keys, drop = FALSE]
        }
      } else if (type == "mimeUri") {
        # For mimeUri, ignore the composition and leave the child elements as is.
        return(NULL)
      } else {
        # For other composed types we don't know, default to text.
        return(NULL)
      }
    } else {
      # For non-composed elements, if the column exists, cast based on its type.
      if (key %in% names(flattened_rows)) {
        if (type == "number") {
          flattened_rows[[key]] <<- as.numeric(flattened_rows[[key]])
        } else if (type == "date") {
          # Assuming an ISO-like date/time string; adjust format/tz as needed.
          flattened_rows[[key]] <<- as.POSIXct(flattened_rows[[key]], tz = "UTC")
        } else {
          # For string, assign, note, or unknown types, use character representation.
          flattened_rows[[key]] <<- as.character(flattened_rows[[key]])
        }
      }
    }
  })
  
  flattened_rows
}

.get_rows_from_uri <- function(table_meta) {
  data_uri <- table_meta$dataUri
  
  # Retrieve stored credentials (needed for pagination).
  creds <- get_credentials()
  username <- creds$username
  password <- creds$password
  
  # Define extraction and cursor functions for pagination.
  extract_rows <- function(parsed) {
    if (!is.null(parsed$rows)) parsed$rows else list()
  }
  
  extract_cursor <- function(parsed) {
    parsed$webSafeResumeCursor
  }
  
  # Retrieve all rows using the internal pagination helper and flatted the orderedColumns list.
  all_rows <- .paginate_get(data_uri, username, password, extract_rows, extract_cursor) |> .convert_column_list() 

  # Parse based on schema.
  all_rows <- .parse_schema_types(table_meta, all_rows)

  return(all_rows)
}

.download_attachments_for_row <- function(row_id, table_meta, save_to_directory, lookup_table = NULL, skip_if_instance_folder_exists = FALSE) {
  # Ensure the save directory exists.
  save_to_directory <- file.path(save_to_directory, table_meta$tableId)
  row_directory <- file.path(save_to_directory, sub("^uuid:", "",row_id))  

  if (!dir.exists(save_to_directory)) {
    dir.create(save_to_directory, recursive = TRUE)
  } else {
    # Check if a directory with row_id already exists - if so maybe make a french exit
    if (skip_if_instance_folder_exists & dir.exists(row_directory)) {
      message("Skipped downloading attachments because parent folder already existed: ", row_directory)
      return()
    }
  }
  
  # Retrieve stored credentials.
  creds <- get_credentials()
  username <- creds$username
  password <- creds$password

  manifest_uri <- paste0(table_meta$definitionUri, "/attachments/", row_id, "/manifest")  
  response <- httr::GET(
    manifest_uri,
    httr::authenticate(username, password, type = "basic"),
    httr::add_headers(
      `User-Agent` = "rockx-odkx-client",
      `Content-Type` = "application/json"
    )
  )
  .handle_response(response)
  manifest <- jsonlite::fromJSON(httr::content(response, as = "text", encoding = "UTF-8"))
  file_list <- if (!is.null(manifest$files) && is.data.frame(manifest$files) && nrow(manifest$files) > 0) {
    split(manifest$files, seq_len(nrow(manifest$files)))
  } else {
    list()  # Return an empty list if there are no files
  }
  
  # Iterate over each file and download it.
  lapply(file_list, function(file) {
    download_url <- file$downloadUrl
    filename <- file$filename
      
    if (!dir.exists(row_directory)) {
      dir.create(row_directory, recursive = TRUE)
    }
    
    file_response <- httr::GET(
      download_url,
      httr::authenticate(username, password, type = "basic"),
      httr::add_headers(`User-Agent` = "rockx-odkx-client")
    )
    .handle_response(file_response)
    
    dest_path <- file.path(row_directory, filename)
    if (!is.null(lookup_table)) {
      name_override <- lookup_table |> 
        filter(.data$urivalue == filename) |> 
        pull(.data$fieldname)
      if (length(name_override)>0) {
        extension <- sub(".*\\.(.*)$", "\\1", filename)
        name_override <- paste0(name_override, ".", extension)
        dest_path <- file.path(row_directory, name_override)
      }
    }
    writeBin(httr::content(file_response, as = "raw"), dest_path)
    message("Downloaded attachment: ", dest_path)
    dest_path
  })
}
# API Functions ----------------------------------------------------------


#' Retrieve Tables from ODK-X Sync Endpoint
#'
#' This function retrieves the list of tables from the ODK-X Sync Endpoint by making a paginated GET request to
#' `(<server_url>)/odktables/default/tables`. It uses BASIC authentication with the credentials stored via
#' `rockx::set_username_and_password()`. 
#' 
#' @importFrom utils URLencode
#' @return A tibble containing the list of found tables.
#' @export
get_tables <- function() {
  # Retrieve stored credentials.
  creds <- get_credentials()
  base_url <- creds$server_url
  username <- creds$username
  password <- creds$password
  
  # Construct the base endpoint URL, ensuring no trailing slash.
  endpoint <- paste0(gsub("/+$", "", base_url), "/odktables/default/tables")
  
  # Define a function to extract the tables from the parsed JSON.
  extract_tables <- function(parsed) {
    if (!is.null(parsed$tables)) parsed$tables else list()
  }
  
  # Define a function to extract the cursor from the parsed JSON.
  extract_cursor <- function(parsed) {
    parsed$webSafeResumeCursor
  }
  
  # Retrieve all pages of results using the internal pagination helper.
  all_tables <- .paginate_get(endpoint, username, password, extract_tables, extract_cursor)
  
  # Combine the list of tables into a tibble.
  all_tables |> dplyr::bind_rows()
}


#' Retrieve table metadata from ODK-X Sync Endpoint
#'
#' This function retrieves the table metadata for a given table from the ODK-X Sync Endpoint.
#'
#' @param table_name String. The name of the table.
#' @return A list containing table metadata.
#' @export
get_table_metadata <- function(table_name) {
  if (!is.character(table_name) || nchar(trimws(table_name)) < 1) {
    stop("`table_name` must be a non-empty string.")
  }

  # Retrieve stored credentials.
  creds <- get_credentials()
  base_url <- creds$server_url
  username <- creds$username
  password <- creds$password
  
  # Construct the endpoint URL.
  table_endpoint <- paste0(gsub("/+$", "", base_url), "/odktables/default/tables/", table_name)
  
  meta_response <- httr::GET(
    table_endpoint,
    httr::authenticate(username, password, type = "basic"),
    httr::add_headers(
      `User-Agent` = "rockx-odkx-client",
      `Content-Type` = "application/json"
    )
  )
  .handle_response(meta_response)
  table_meta <- jsonlite::fromJSON(httr::content(meta_response, as = "text", encoding = "UTF-8"))
  
  if (is.null(table_meta$dataUri)) {
    stop("dataUri not found in table metadata")
  }
  
  table_meta
}

#' Retrieve all rows from a table
#'
#' This function retrieves all rows for a given table from the ODK-X Sync Endpoint
#' and tries to parse known types based on the table's schema definition.
#'
#' @param table_name String. The name of the table from which to retrieve rows.
#' @return A tibble containing all rows.
#' @export
get_all_rows <- function(table_name) {
  if (!is.character(table_name) || nchar(trimws(table_name)) < 1) {
    stop("`table_name` must be a non-empty string.")
  }
  
  table_meta <- get_table_metadata(table_name)
  all_rows <- .get_rows_from_uri(table_meta)
  return(all_rows)
}

#' Download attachments for table rows
#'
#' This function downloads attachments for the specified table rows and saves them 
#' in a structured directory. Each row with attachments will have a dedicated folder 
#' under the table-specific subdirectory.
#'
#' @param rows A tibble or data frame containing the table rows to download attachments for.
#' @param table_meta List. Metadata for the table, as returned by `get_table_metadata(table_name)`.
#' @param save_to_directory String. The parent directory where attachments will be saved. 
#' A subdirectory with the name of the table will be created, and each row with attachments 
#' will have its own subfolder under this directory.
#' @param skip_if_instance_folder_exists Logical. If `TRUE`, rows with an existing instance folder 
#' will be skipped to avoid re-downloading attachments. Defaults to `FALSE`.
#' 
#' @return Invisibly returns `NULL`. Downloads the attachments as a side effect.
#' @export
download_attachments <- function(rows, table_meta, save_to_directory, skip_if_instance_folder_exists = FALSE) { 
  for (i in 1:nrow(rows)) {
    row <- rows[i, ]
    lookup_table <- row |> 
      select(ends_with("_uriFragment")) |>      
      pivot_longer(dplyr::everything(),
                   names_to = "fieldname", 
                   values_to = "urivalue") |>
      mutate(fieldname = sub("_uriFragment$", "", .data$fieldname)) #using .data$ pronoun to satisfy linter...
    .download_attachments_for_row(row$id, table_meta, save_to_directory, lookup_table, skip_if_instance_folder_exists)
  }
}



## The function below works, however it appears the sync-endpoint doesn't work as specified as it returns rows modified before the provided timestamp as well.
## Once that is fixed, this function can be included in the API
# #' Retrieve all rows from a table modified after a given timestamp
# #'
# #' This function retrieves all rows for a given table from the ODK-X Sync Endpoint
# #' that were created or updated after the specified timestamp. The function parses known
# #' types based on the table's schema definition.
# #'
# #' @param table_name String. The name of the table from which to retrieve rows.
# #' @param time_stamp POSIXct or Date. A timestamp indicating the starting point for retrieving rows.
# #' It should be a valid date-time object. If it is not already a `POSIXct` object, it will be coerced.
# #' @return A tibble containing all rows created or updated since the given `time_stamp`.
# #' @export
# get_rows_since <- function(table_name, time_stamp) {
#   if (!is.character(table_name) || nchar(trimws(table_name)) < 1) {
#     stop("`table_name` must be a non-empty string.")
#   }
  
#   if (!inherits(time_stamp, "POSIXct")) {
#     if (inherits(time_stamp, "Date")) {
#       time_stamp <- as.POSIXct(time_stamp)
#     } else {
#       stop("`time_stamp` must be a POSIXct or Date object.")
#     }
#   }
  
#   date_param <- .format_date(time_stamp)
#   table_meta <- get_table_metadata(table_name)
#   uri <- paste0(table_meta$definitionUri, "/query/savepointTimestamp?startTime=", date_param)  
#   #uri <- paste0(table_meta$definitionUri, "/query/lastUpdateDate?startTime=", date_param)
#   all_rows <- .get_rows_from_uri(uri)
  
#   return(all_rows)
# }
