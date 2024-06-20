#' @title pull_jira_info
#' @description Script to process CSV export of Jira into a status log
#' @param jira_project Jira project code (e.g. CVTDB)
#' @param in_file File path to Jira ticket summary CSV.
#' @param source The source to set a qc_category for
#' @param source_table Name of the source table associated with the source
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
pull_jira_info <- function(jira_project="TOXVAL", in_file = NULL, source = NULL, source_table = NULL, auth_token = NULL){

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

  # Filter to input source_table
  if(!is.null(source_table)){
    in_data_url = in_data_url %>%
      dplyr::filter(grepl(!!source_table, Summary))
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

  qc_files <- ticket_attachment_metadata %>%
    dplyr::filter(stringr::str_detect(Summary, " QC")) %>%
    # Use all QC files from Jira
    dplyr::filter(grepl("toxval_qc|mrls_QC", jira_link),
                  # Filter out DAT pushed records
                  !grepl("QC_push", jira_link)) #%>%
    # group_by(Summary) %>%
    # slice_max(date)

  res <- data.frame()
  for(i in seq_len(nrow(qc_files))){
    jira_link <- qc_files$jira_link[i]
    file_ext <- qc_files$file_ext[i]
    file <- jira_load_file_from_api(url = jira_link, headers = headers, file_type = file_ext, mode='wb')
    # Run through each sheet in loaded file
    for(sheet in names(file)){
      df = file[[sheet]] %>%
        data.frame()
      tryCatch({
        res <- res %>%
          dplyr::bind_rows(df %>%
                             dplyr::select(dplyr::any_of(c("source", "source_hash", "qc_status")))) %>%
          dplyr::distinct()
      }, error = function(e){
        print(paste("Error:", conditionMessage(e)))
      })
    }
  }

  return(list(in_data=in_data,
              #out_summary=out_summary,
              ticket_attachment_metadata=ticket_attachment_metadata,
              hashes=res))
}
