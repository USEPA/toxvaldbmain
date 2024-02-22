#--------------------------------------------------------------------------------------
#' Function for assigning QC Categories to sources in toxval via the qc_category field
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source The source to set a qc_category for
#' @param access_token A personal access token for authentication in Confluence/Jira
#' @export
#--------------------------------------------------------------------------------------
set.qc.category.by.source <- function(toxval.db, source=NULL, confluence_access_token, jira_access_token){
  printCurrentFunction(toxval.db)
  if(!is.null(source)) {
    slist = source
  } else {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  }
  url <- "https://confluence.epa.gov/x/VuCkFg"
  # Retrieve Jira ticket data and confluence page data
  jira_tickets <- pull_jira_info(jira_project = "TOXVAL", in_file = NULL, auth_token = pat)
  response <- GET(url, add_headers(Authorization = paste("Bearer", access_token)))

  if (status_code(response) == 200) {
    confluence_page <- read_html(content(response, "text"))
  } else{
    print('authentication failed')
  }

  # Derive the table and rows
  tables <- html_nodes(confluence_page, "table")
  table <- tables[[2]]
  header_row <- html_nodes(confluence_page, "tr:nth-child(1) th")
  column_names <- html_text(header_row)
  table_data <- list()
  data_rows <- html_nodes(table, "tr:not(:first-child)")

  # Read the rows into a list
  for(i in seq_along(data_rows)) {
    row <- data_rows[[i]]
    cells <- html_nodes(row, "td")
    row_data <- html_text(cells)
    row_data <- gsub("[\r\n]", "", row_data)
    table_data <- c(table_data, list(row_data))
  }

  table_df <- as.data.frame(do.call(rbind, table_data), stringsAsFactors = FALSE)
  colnames(table_df) <- column_names

  old_qc_category = runQuery("select distinct source, source_table, qc_category from toxval",toxval.db)
  old_qc_category$qc_category[old_qc_category$qc_category == "-"] <- NA
  old_qc_category$assignee <- NA

  for (i in 1:nrow(old_qc_category)){
    source_table_qc <- paste0(old_qc_category$source_table[i], " QC")
    matching_rows <- jira_tickets[jira_tickets$Summary == source_table_qc | jira_tickets$Summary == old_qc_category$source_table[i],]
    if(nrow(matching_rows) > 0){
      old_qc_category$assignee[i] <- matching_rows$Assignee[1]
    }
  }

  # Only consider valid, desired sources
  tables_names <- unique(table_df$`Source Name`)
  valid_sources <- old_qc_category %>%
    filter(source %in% slist & source %in% tables_names)

  res0 <- data.frame()
  # Determine qc_category for each source
  for(src in valid_sources$source) {
    source_df <- subset(table_df, `Source Name` == src)
    existing_source <- old_qc_category %>% filter(source == src)

    curation_type <- unique(source_df$curation_type)
    qc_status <- unique(source_df$`QC Status`)

    if(curation_type == 'automated'){
      if(!grepl("Programmatically extracted from structured data source", existing_source$qc_category)){
        qc_category_new = "Programmatically extracted from structured data source"
      }
    } else if (curation_type == 'manual'){
      if(!grepl("Manually extracted from unstructured data source", existing_source$qc_category)){
        qc_category_new = "Manually extracted from unstructured data source"
      }
    }
    if(qc_status == "LV 1- In Review" & is.na(existing_source$assignee)){
      if(!grepl("Source overall passed QC, but this record was not manually checked", existing_source$qc_category)){
        if(qc_category_new == "-" | is.na(qc_category_new)){
          qc_category_new = "Source overall passed QC, but this record was not manually checked"
        } else{
          qc_category_new = paste0(qc_category_new, ", Source overall passed QC, but this record was not manually checked")
        }
      }
    }
    #--------------------------------------------------------------------------------------
    # TODO: Incorporate logic for adding additional qc_categories
    #--------------------------------------------------------------------------------------

    new_info <- c(src, qc_category_new)
    res0 <- rbind(res0, new_info)
  }
  # Prep columns for insertion
  colnames(res0) <- c("source", "qc_category_new")
  res1 <- merge(old_qc_category, res0, by="source")
  res <- res1 %>%
    tidyr::unite(col="qc_category", qc_category, qc_category_new, sep = ", ", na.rm = TRUE) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(qc_category = toString(unique(unlist(strsplit(qc_category,",\\s+"))))) %>%
    dplyr::select("source", "qc_category")

  # Update qc_category in toxval
  if(nrow(res)){
    updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                         "ON (a.source = b.source) SET a.qc_category = b.qc_category")
    # Run update query
    runUpdate(table="toxval", updateQuery=updateQuery, updated_df=res, db=toxval.db)
  }
}
