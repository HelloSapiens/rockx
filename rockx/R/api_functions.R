#' @importFrom dplyr filter mutate select as_tibble everything ends_with pull bind_cols
#' @importFrom tidyr pivot_longer
#' @importFrom purrr map_df pmap
#' @importFrom rlang .data

# Helper functions -------------------------------------------------------

.format_date <- function(date) {
  return(format(date, "%Y-%m-%dT%H:%M:%OS3"))
}


# Internal helper for handling HTTP errors.
.handle_response_status <- function(response) {
  status <- httr::status_code(response)
  
  if (status == 200) return(invisible(response))
  
  message <- httr::content(response, as = "text", encoding = "UTF-8")
  
  error_msg <- dplyr::case_when(
    status == 401 ~ "Unauthorized: Check your credentials.",
    status == 403 ~ "Forbidden: You don't have permission to access this resource.",
    status == 404 ~ "Not Found: The requested resource was not found.",
    status == 500 ~ "Internal Server Error: The server encountered an error.",
    TRUE ~ paste("HTTP error", status, ":", message)
  )
  
  stop(error_msg, call. = FALSE)
}


# Internal helper for paginated GET requests.
.paginate_get <- function(endpoint, extract_fn, cursor_fn, apply_flattening = FALSE) {
  all_results <- list()
  flat_res <- dplyr::as_tibble(NULL)
  cursor <- NULL
  has_more <- TRUE
  
  while (has_more) {
    url <- if (is.null(cursor)) {
      endpoint
    } else {
      paste0(endpoint, "?cursor=", utils::URLencode(cursor, reserved = TRUE))
    }

    parsed <- .get_response(url)

    all_results <- c(all_results, extract_fn(parsed))
    if (apply_flattening) {
      flat_res <- dplyr::bind_rows(flat_res, .convert_column_list(all_results))
      all_results <- list()
    }
    
    has_more <- isTRUE(parsed$hasMoreResults)
    cursor <- if (has_more) cursor_fn(parsed) else NULL
  }
  
  if (apply_flattening) {
    return(flat_res)
  }
  return(all_results)
}

.convert_column_list <- function(all_results) {
  ordered_columns <- all_results$orderedColumns |> purrr::map_df(~ tidyr::pivot_wider(.x, names_from = column, values_from = value))
  other_columns <- all_results[names(all_results) != "orderedColumns" & names(all_results) != "filterScope"] |>  dplyr::as_tibble()
  all_results <- dplyr::bind_cols(other_columns, ordered_columns)
  return (all_results)
}

.get_table_schema <- function(table_meta) {
  schema <- .get_response(table_meta$definitionUri)
  return(schema)
}

.parse_schema_types <- function(table_meta, flattened_rows) {
  schema <- .get_table_schema(table_meta)

  # Iterate over each column in the schema.
  purrr::pmap(schema$orderedColumns, function(elementKey, elementName, elementType, listChildElementKeys) {
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
  
  # Define extraction and cursor functions for pagination.
  extract_rows <- function(parsed) {
    if (!is.null(parsed$rows)) parsed$rows else list()
  }
  
  extract_cursor <- function(parsed) {
    parsed$webSafeResumeCursor
  }
  
  # Retrieve all rows using the internal pagination helper and flattened
  all_rows <- .paginate_get(data_uri, extract_rows, extract_cursor, TRUE)

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

  manifest_uri <- paste0(table_meta$definitionUri, "/attachments/", row_id, "/manifest")  
  
  manifest <- .get_response(manifest_uri)

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
    
    .get_response(download_url, write_to_path = dest_path)

    message("Downloaded attachment: ", dest_path)
    dest_path
  })
}

.reset_auth <- function() {  
  creds <- get_credentials()
  base_url <- creds$server_url  
  httr::handle_reset(base_url)

}

.get_response <- function(url_segment, write_to_path = NULL) {
  creds <- get_credentials()
  base_url <- creds$server_url
  username <- creds$username
  password <- creds$password

  complete_url <- startsWith(url_segment, base_url)

  url <- ''
  if (startsWith(url_segment, base_url)) {
    url <- url_segment
  } else {
    url_segment <- paste0("/", url_segment) |> 
      gsub("^/+", "/", x = _) |> 
      gsub("^/(?!odktables)", "/odktables/", x = _, perl = TRUE)
  
    url <- paste0(base_url, url_segment)
  }
  
  
  response <- tryCatch(
    {
      httr::GET(
        url,
        httr::authenticate(username, password, type = "basic"),
        httr::add_headers(
          `User-Agent` = "rockx-odkx-client",
          `Content-Type` = "application/json"
        )
      )
    },
    error = function(e) {
      warning("Failed to fetch data from: ", url, "\nError message: ", conditionMessage(e))    
      stop("Unable to connect to sync endpoint", call. = FALSE)
    }
  )
  

  .handle_response_status(response)

  if (is.null(write_to_path)) {
    parsed <- jsonlite::fromJSON(httr::content(response, as = "text", encoding = "UTF-8"))
    return(parsed)
  } else {
    writeBin(httr::content(response, as = "raw"), write_to_path)
    return(write_to_path)
  }
  
}

# API Functions ----------------------------------------------------------

#' Verifies the user has sufficient permissions to download data
#'
#' Before calling this function, first set username, password, and host with:
#' `rockx::set_username_and_password()`. 
#' 
#' @return TRUE or FALSE
#' @export
has_access <- function() {
  .reset_auth()
  auth <- .get_response("/odktables/default/privilegesInfo")
  if (is.null(auth) || !is.list(auth) || !"roles" %in% names(auth) || is.null(auth$roles) || all(is.na(auth$roles))) {
    return(FALSE)
  }

  adm_role <- "ROLE_SITE_ACCESS_ADMIN"
  has_admin <- adm_role %in% auth$roles # For now we require an admin user  
  if (!has_admin) {
    warning(paste0("User is authenticated, but doesn't have the required permissions (", adm_role ,")."), call. = FALSE)
  }
  return(has_admin)  
}

#' Get Table Metadata
#'
#' Retrieves metadata for a given table from the ODK-X Sync Endpoint.
#'
#' @param table_name A character string specifying the table name.
#' @return A list containing table metadata.
#' @details This function calls the ODK-X Sync Endpoint to fetch metadata 
#' about a specific table. If the `dataUri` field is missing in the response, 
#' an error is thrown.
#' @export
get_table_metadata <- function(table_name) {
  if (!is.character(table_name) || nchar(trimws(table_name)) < 1) {
    stop("`table_name` must be a non-empty string.")
  }

  table_meta <- .get_response(paste0("/odktables/default/tables/", table_name))
  
  if (is.null(table_meta$dataUri)) {
    stop("dataUri not found in table metadata")
  }
  
  table_meta
}

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
  creds <- get_credentials()
  base_url <- creds$server_url
  username <- creds$username
  password <- creds$password
  
  endpoint <- paste0(base_url, "/odktables/default/tables")
  
  # Define a function to extract the tables from the parsed JSON.
  extract_tables <- function(parsed) {
    if (!is.null(parsed$tables)) parsed$tables else list()
  }
  
  # Define a function to extract the cursor from the parsed JSON.
  extract_cursor <- function(parsed) {
    parsed$webSafeResumeCursor
  }
  
  # Retrieve all pages of results using the internal pagination helper.
  all_tables <- .paginate_get("/odktables/default/tables", extract_tables, extract_cursor)
  
  # Combine the list of tables into a tibble.
  all_tables |> dplyr::bind_rows()
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
      dplyr::select(ends_with("_uriFragment")) |>      
      tidyr::pivot_longer(dplyr::everything(),
                   names_to = "fieldname", 
                   values_to = "urivalue") |>
      dplyr::mutate(fieldname = sub("_uriFragment$", "", .data$fieldname)) #using .data$ pronoun to satisfy linter...
    .download_attachments_for_row(row$id, table_meta, save_to_directory, lookup_table, skip_if_instance_folder_exists)
  }
}