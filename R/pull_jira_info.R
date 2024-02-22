#' @title pull_jira_info
#' @description Script to process CSV export of Jira into a status log
#' @param jira_project Jira project code (e.g. TOXVAL)
#' @param download_bulk Boolean whether to bulk download ticket attachments, Default: FALSE.
#' @param auth_token Authorization token for Jira
#' @return Summary DataFrame of Jira tickets by Epic, Label, and Status
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  out = pull_jira_info(jira_project="project_name")
#'  }
#' }
#' @seealso
#'  [download.file][utils::download.file], [unzip][utils::unzip]
#'  [read_csv][readr::read_csv], [cols][readr::cols]
#'  [select][dplyr::select], [contains][dplyr::contains], [mutate][dplyr::mutate], [everything][dplyr::everything], [filter][dplyr::filter], [distinct][dplyr::distinct], [left_join][dplyr::left_join], [group_by][dplyr::group_by], [summarise][dplyr::summarise], [n][dplyr::n]
#'  [unite][tidyr::unite]
#'  [str_squish][stringr::str_squish]
#' @rdname pull_jira_info
#' @export
#' @importFrom utils download.file unzip
#' @importFrom readr read_csv cols
#' @importFrom dplyr select contains mutate everything filter distinct left_join group_by summarise n
#' @importFrom tidyr unite
#' @importFrom stringr str_squish
pull_jira_info <- function(jira_project, in_file = NULL, auth_token = NULL, status_filter = "Done"){

  # Format headers
  if(!is.null(auth_token)){
    headers <- c(Authorization = paste0("Bearer ", auth_token))
  } else {
    headers <- NULL
  }

  if(is.null(in_file)){
    # Pull CSV export from Jira
    url = paste0("https://jira.epa.gov/sr/jira.issueviews:searchrequest-csv-all-fields/temp/SearchRequest.csv?jqlQuery=project+%3D+", jira_project)
    in_data_url <- load_file_from_api(url=url, headers=headers, file_type="csv")
  } else {
    # Load input file
    in_data_url <- readr::read_csv(in_file,
                                   col_types = readr::cols())
  }

  # Check if input Jira file loaded
  if(is.null(in_data_url)){
    stop("Either could not pull directly from Jira or 'in_file' loading error...")
  }

  # Process loaded data
  in_data <- in_data_url %>%
    dplyr::select(Summary,
                  `Issue key`,
                  `Issue id`,
                  `Issue Type`,
                  `Assignee`,
                  Status,
                  Created,
                  dplyr::contains("Labels"),
                  Description,
                  `Epic Link`=`Custom field (Epic Link)`) %>%
    # Join labels
    tidyr::unite(dplyr::contains("Labels"), col="Labels", sep=", ", na.rm = TRUE) %>%
    # Remove extraneous label
    dplyr::mutate(Labels = gsub(", PKWG", "", Labels)) %>%
    dplyr::select(`Issue key`, dplyr::everything())

  return(in_data)
}

load_file_from_api <- function(url, headers, file_type, mode = "w"){
  temp_in <- tempfile(fileext = paste0(".", file_type))
  out <- tryCatch({
    utils::download.file(url = url,
                         destfile = temp_in,
                         headers = headers,
                         mode = mode)
    if(file_type == "csv"){
      readr::read_csv(temp_in,
                      col_types = readr::cols()) %>%
        dplyr::mutate(dplyr::across(where(is.character), stringr::str_squish)) %>%
        return()
    } else if(file_type == "xlsx"){
      sheet_ls = readxl::excel_sheets(temp_in)
      lapply(sheet_ls, function(s_name){
        readxl::read_xlsx(temp_in,
                          sheet=s_name) %>%
          dplyr::mutate(dplyr::across(where(is.character), stringr::str_squish))
      }) %T>% {
        names(.) <- sheet_ls
      }
    } else {
      stop("'load_file_from_api()' unsupported file_type '", file_type,"'")
    }
  }, error=function(e) {
    message(e)
    return(NULL)
  },
  finally = { unlink(temp_in) }
  )
  return(out)
}
