#--------------------------------------------------------------------------------------
#' Function for assigning QC Categories to sources in toxval via the qc_category field
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param source The source to set a qc_category for
#' @param jira_access_token A personal access token for authentication in Jira
#' @param confluence_access_token A personal access token for authentication in Confluence
#' @export
#--------------------------------------------------------------------------------------
set.qc.category.by.source <- function(toxval.db, source.db, source=NULL, confluence_access_token, jira_access_token){
  printCurrentFunction(toxval.db)
  if(!is.null(source)) {
    slist = source
  } else {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  }
  url <- "https://confluence.epa.gov/x/VuCkFg"
  # Retrieve Jira ticket data and confluence page data
  jira_tickets <- pull_jira_info(in_file = NULL, auth_token = jira_access_token) #%>%
    #dplyr::filter(`Epic Link` == "TOXVAL-296")
  response <- GET(url, httr::add_headers(Authorization = paste("Bearer", confluence_access_token)))

  if (httr::status_code(response) == 200) {
    confluence_page <- rvest::read_html(httr::content(response, "text"))
  } else{
    print('authentication failed')
  }

  # Derive the table and rows
  tables <- rvest::html_nodes(confluence_page, "table")
  table <- tables[[2]]
  header_row <- rvest::html_nodes(confluence_page, "tr:nth-child(1) th")
  column_names <- rvest::html_text(header_row)
  table_data <- list()
  data_rows <- rvest::html_nodes(table, "tr:not(:first-child)")

  # Read the rows into a list
  for(i in seq_along(data_rows)) {
    row <- data_rows[[i]]
    cells <- rvest::html_nodes(row, "td")
    row_data <- rvest::html_text(cells)
    row_data <- gsub("[\r\n]", "", row_data)
    table_data <- c(table_data, list(row_data))
  }

  table_df <- as.data.frame(do.call(rbind, table_data), stringsAsFactors = FALSE)
  colnames(table_df) <- column_names
  # Filter out those not reviewed
  table_df = table_df %>%
    dplyr::filter(!`QC Status` %in% c(NA, "Ice Box", "Icebox"))

  old_qc_category = runQuery("select distinct source, source_table, qc_category from toxval",toxval.db)
  old_qc_category$qc_category[old_qc_category$qc_category == "-"] <- NA
  old_qc_category$assignee <- NA

  in_data <- jira_tickets$in_data
  hashes <- jira_tickets$hashes
  for (i in seq_len(nrow(old_qc_category))){
    source_table_qc <- paste0(old_qc_category$source_table[i], " QC")
    matching_rows <- in_data[in_data$Summary == source_table_qc | in_data$Summary == old_qc_category$source_table[i],]
    if(nrow(matching_rows) > 0){
      old_qc_category$assignee[i] <- matching_rows$Assignee[1]
    }
  }

  # Only consider valid, desired sources
  tables_names <- unique(table_df$`Source Name`)
  valid_sources <- old_qc_category %>%
    dplyr::filter(source %in% slist & source %in% tables_names)

  res0 <- data.frame()
  # Determine qc_category for each source
  for(src in valid_sources$source) {
    source_df <- table_df %>%
      dplyr::filter(`Source Name` == src)
    existing_source <- old_qc_category %>% dplyr::filter(source == src)
    query = paste0("select distinct source_hash, source, qc_category from toxval where source = '", src, "'")
    in_toxval = runQuery(query, toxval.db) # %>%

    # Only certain sources have known multiple curation types
    if(length(unique(source_df$curation_type)) > 1){
      # Reconcile curation_type by source_hash
      if(src %in% c("ATSDR MRLs", "IRIS", "PPRTV (CPHEA)")){
        src_data = runQuery(paste0("SELECT source_hash, document_type FROM ", source_df$`Table Name`),
                            source.db)
        in_toxval = in_toxval %>%
          dplyr::left_join(src_data, by = "source_hash") %>%
          dplyr::mutate(curation_type = dplyr::case_when(
            grepl("PPRTV Summary|ATSDR MRLs Toxicological Profile|IRIS Summary", document_type) ~ "manual",
            TRUE ~ "automated"
          )) %>%
          dplyr::left_join(source_df %>%
                             dplyr::select(`QC Status`, curation_type),
                           by="curation_type")
      } else {
        stop(paste0("Source with multiple curation_types not in list: ", src))
      }
    }

    # TODO Pull source_hash from Jira ticket attachments
    # hashes %>% dplyr::filter(source == src)
    hash_list = c()

    in_toxval = in_toxval %>%
      dplyr::mutate(
        # Establish baseline
        qc_category_new = dplyr::case_when(
          curation_type == 'automated' & !grepl("Programmatically extracted from structured data source", qc_category) ~
            "Programmatically extracted from structured data source",
          curation_type == 'manual' & !grepl("Manually extracted from unstructured data source", qc_category) ~
            "Manually extracted from unstructured data source",
          TRUE ~ NA_character_
        ),
        # Account for qc_status and attachment file source_hash values
        qc_category_new = dplyr::case_when(
          (`QC Status` %in% c("LV 1 - In Review", "Done")) &
            source_hash %in% hash_list ~
            paste0(qc_category_new, ", Source overall passed QC, and this record was manually checked"),
          TRUE ~ paste0(qc_category_new,
                        ", Source overall passed QC, but this record was not manually checked")
        )
      )

    if((qc_stat == "LV 1- In Review" & is.na(existing_source$assignee)) | qc_stat == "Done"){
      # Set qc_category for entries present in QC sampling
      src_records <- hashes %>% dplyr::filter(source == src) %>%
        dplyr::mutate(
          qc_category_new = paste0(!!qc_category_new, ", Source overall passed QC, and this record was manually checked")
        )

      # Comment out for now - assume that all entries in the hashes DF have been manually checked
      # merged <- merge(in_toxval, src_records, by=c('source_hash','source'), all.x=TRUE)
      # merged <- merged %>% mutate(qc_status = ifelse(is.na(qc_status), "-", qc_status))
      # merged <- merged %>%
      #   mutate(
      #     qc_category_new = ifelse(tolower(qc_status) == 'pass', paste0(qc_category_new, ", Source overall passed QC, and this record was manually checked"),
      #                              paste0(qc_category_new, ", Source overall passed QC, but this record was not manually checked"))
      #   )

    } else {
      merged = in_toxval[0,]
    }
    #--------------------------------------------------------------------------------------
    # TODO: Incorporate logic for adding additional qc_categories
    #--------------------------------------------------------------------------------------
    in_toxval <- merged
    in_toxval <- in_toxval %>% dplyr::select(source, source_hash, qc_category_new)
    res0 <- rbind(res0, in_toxval)
  }
  # Prep columns for insertion
  colnames(res0) <- c("source", "source_hash", "qc_category_new")
  res1 <- merge(old_qc_category, res0, by="source")
  res <- res1 %>%
    tidyr::unite(col="qc_category", qc_category, qc_category_new, sep = ", ", na.rm = TRUE) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(qc_category = toString(unique(unlist(strsplit(qc_category,",\\s+"))))) %>%
    dplyr::select("source", "source_hash", "qc_category")

  # Update qc_category in toxval
  if(nrow(res)){
    updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                         "ON (a.source_hash = b.source_hash) SET a.qc_category = b.qc_category ",
                         "WHERE a.qc_category IS NOT NULL")
    # Run update query
    runUpdate(table="toxval", updateQuery=updateQuery, updated_df=res, db=toxval.db)
  }
}
