#' @title qa_toxval_url_validation
#' @description Function to pull URLs and log their HTTP statuses
#' @param tbl_list List of ToxVal tables to pull URLs from, Default: c("toxval", "record_source", "source_info")
#' @param db ToxVal database name to pull URLs from
#' @param log_suffix SUffix to add to end of log file to uniquely identify file, Default: Date stamp from Sys.Date()
#' @return None. Log file is generated
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  qa_toxval_url_validation(tbl_list = c("toxval", "record_source", "source_info"), db="res_toxval_v94", log_suffix="2023-05-01")
#'  }
#' }
#' @seealso
#'  \code{\link[readxl]{read_excel}}
#'  \code{\link[dplyr]{filter}}, \code{\link[dplyr]{mutate}}, \code{\link[dplyr]{reexports}}, \code{\link[dplyr]{bind}}
#'  \code{\link[tidyr]{pivot_longer}}
#'  \code{\link[purrr]{keep}}
#'  \code{\link[httr]{GET}}, \code{\link[httr]{status_code}}
#'  \code{\link[writexl]{write_xlsx}}
#' @rdname qa_toxval_url_validation
#' @export
#' @importFrom readxl read_xlsx
#' @importFrom dplyr filter mutate contains bind_rows
#' @importFrom tidyr pivot_longer
#' @importFrom purrr compact
#' @importFrom httr GET status_code
#' @importFrom writexl write_xlsx
qa_toxval_url_validation <- function(tbl_list = c("toxval", "record_source", "source_info"),
                                     db, log_suffix){
  # Check for log suffix
  if(is.null(log_suffix) || is.na(log_suffix)){
    log_suffix <- Sys.Date()
  }

  # Create log file name (use toxval.config Repo path)
  out_file <- paste0(toxval.config()$datapath, "URL Validation Logs/toxval_url_log_",log_suffix,".xlsx")

  # Check if already exists
  if(file.exists(out_file)){
    # Pull if exists
    url_list <- readxl::read_xlsx(out_file)
  } else {
    # If no log present, start a new log
    # PUll all URL fields from tables
    url_list <- lapply(tbl_list, function(t_name){
      # Pull all fields with "url"
      fields <- runQuery(paste0("desc ",t_name),db) %>%
        # Filter to all table fields that are URLs
        dplyr::filter(grepl("url", Field))

      # Pull url data if exists
      if(nrow(fields)){
        message("Pulling url fields for: ", t_name)
        tmp <- paste0("SELECT DISTINCT ",
                      toString(fields$Field),
                      " FROM ", t_name) %>%
          # Use toxxaldb package runQuery
          runQuery(db=db) %>%
          dplyr::mutate(table = t_name) %>%
          tidyr::pivot_longer(dplyr::contains("url"),
                              names_to = "field_name", values_to = "url") %>%
          # Filter out non-URLs
          dplyr::filter(!url %in% c("-", "source_url", "subsource_url", "Unknown", ""))
        return(tmp)
      }
      # Return NULL if no URL fields
      return(NULL)

    }) %>%
      # Remove NULL entries
      purrr::compact() %>%
      dplyr::bind_rows() %>%
      # Add default columns
      dplyr::mutate(url_status = NA,
             check_time = NA)
  }

  # Filter to only those in need of checking
  urls_to_check <- url_list %>%
    dplyr::filter(is.na(url_status))

  message("Begin checking urls...")
  # Check all URL fields and output a log
  for(r in seq_len(length(unique(urls_to_check$url)))){
    # Rest between requests
    Sys.sleep(0.25)
    url <- urls_to_check$url[r]
    # Get check time
    time <- Sys.time()
    # Try to check url status (tryCatch)
    status <- tryCatch(
      {
        httr::GET(url) %>%
          httr::status_code()
      },
      error=function(e) {
        e <- paste0("Error checking url: ", e)
        message(e)
        return(e)
      }
    )

    # Update status and time for URL in log
    url_list$url_status[url_list$url == url] <- status
    url_list$check_time[url_list$url == url] <- time

    # Save every 25
    if(r %% 25 == 0){
      message("Checked ", r, " of ", length(unique(urls_to_check$url)))
      writexl::write_xlsx(url_list, out_file)
    }
  }

  # Interpret status (200 and 403 are valid URLs)
  url_list$url_valid <- ifelse(url_list$url_status %in% c(200, 403), 1, 0)

  message("Done...", Sys.time())
  # Final write to ensure log was saved
  writexl::write_xlsx(url_list, out_file)
}
