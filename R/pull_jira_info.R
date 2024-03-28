#' @title pull_jira_info
#' @description Script to process CSV export of Jira into a status log
#' @param jira_project Jira project code (e.g. CVTDB)
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
pull_jira_info <- function(jira_project="TOXVAL", in_file = NULL, auth_token = NULL, status_filter = "Done"){

  # Format headers
  if(!is.null(auth_token)){
    headers <- c(Authorization = paste0("Bearer ", auth_token))
  } else {
    headers <- NULL
  }

  if(is.null(in_file)){
    # Pull CSV export from Jira
    url = paste0("https://jira.epa.gov/sr/jira.issueviews:searchrequest-csv-all-fields/temp/SearchRequest.csv?jqlQuery=project+%3D+", jira_project)
    in_data_url <- jira_load_file_from_api(url=url, headers=headers, file_type="csv", mode='wb')
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

  # Filter to templates to load
  ticket_attachment_metadata <- in_data_url %>%
    dplyr::filter(`Issue key` %in% in_data$`Issue key`) %>%
    dplyr::select(`Issue key`, dplyr::starts_with("Attachment")) %>%
    tidyr::pivot_longer(cols=dplyr::starts_with("Attachment"),
                        names_to="attachment_name") %>%
    dplyr::filter(!is.na(value)) %>%
    tidyr::separate(value, sep=";",
                    into = c("date", "uploaded_by", "filename", "jira_link")) %>%
    dplyr::mutate(file_ext = tools::file_ext(filename),
                  attachment_name = attachment_name %>%
                    gsub("Attachment...", "", .) %>%
                    as.numeric()) %>%
    dplyr::left_join(in_data %>%
                       select(Summary, `Issue key`, Labels),
                     by = "Issue key")

  res0 <- ticket_attachment_metadata %>%
    filter(str_detect(Summary, " QC"))

  recent_files <- res0 %>%
    filter(str_detect(jira_link, "toxval_qc"))%>%
    group_by(Summary) %>%
    slice_max(date)

  res <- data.frame()
  for(i in seq_len(nrow(recent_files))){
    jira_link <- recent_files$jira_link[i]
    file_ext <- recent_files$file_ext[i]
    file <- jira_load_file_from_api(url = jira_link, headers = headers, file_type = file_ext, mode='wb')
    df <- data.frame(file$Sheet1)
    tryCatch({
      res0 <- df %>%
        select(source, source_hash, qc_status)
      res <- bind_rows(res, res0)
    }, error = function(e){
      print(paste("Error:", conditionMessage(e)))
    })

  }

  return(list(in_data=in_data,
              #out_summary=out_summary,
              ticket_attachment_metadata=ticket_attachment_metadata,
              hashes=res))
}
