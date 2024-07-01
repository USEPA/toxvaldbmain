#--------------------------------------------------------------------------------------
#' Function for assigning QC Categories to sources in toxval via the qc_category field
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param source The source to set a qc_category for
#' @param confluence_url URL to QC tracking Confluence page
#' @param jira_access_token A personal access token for authentication in Jira
#' @param confluence_access_token A personal access token for authentication in Confluence
#' @export
#--------------------------------------------------------------------------------------
set.qc.category.by.source <- function(toxval.db, source.db, source=NULL,
                                      confluence_url = "https://confluence.epa.gov/x/VuCkFg",
                                      confluence_access_token, jira_access_token){
  if(is.null(confluence_access_token) || is.na(confluence_access_token)) stop("Must provide confluence_access_token")
  if(is.null(jira_access_token) || is.na(jira_access_token)) stop("Must provide jira_access_token")

  printCurrentFunction(toxval.db)
  if(!is.null(source)) {
    slist = source
  } else {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  }

  # Pull Confluence page table
  response <- httr::GET(confluence_url,
                        httr::add_headers(Authorization = paste("Bearer", confluence_access_token)))
  if (httr::status_code(response) == 200) {
    confluence_page <- rvest::read_html(httr::content(response, "text"))
  } else{
    print('authentication failed')
    browser()
  }

  # # Derive the table and rows
  table <- rvest::html_nodes(confluence_page, "table") %>%
    .[[2]]

  jira_ticket_nodes = table %>%
    rvest::html_nodes("table")
  # Remove Jira Status Macro - https://stackoverflow.com/questions/50768364/how-to-filter-out-nodes-with-rvest/50769954
  xml2::xml_remove(jira_ticket_nodes)

  table_df = table %>%
    rvest::html_table() %>%
    dplyr::select(-`Jira Status`) %>%
    # Filter out those not reviewed
    dplyr::filter(!`QC Status` %in% c(NA, "Ice Box", "Icebox"),
                  `Source Name` %in% slist)

  # Retrieve Jira ticket data
  jira_tickets <- pull_jira_info(in_file = NULL, auth_token = jira_access_token, ticket_filter_list = unique(table_df$`Jira Ticket`))

  # Filter to relevant Jira tickets
  in_data <- jira_tickets$in_data %>%
    dplyr::filter(`Epic Link` == "TOXVAL-296",
                  `Issue key` %in% table_df$`Jira Ticket`)
  # Get attachment file source_hash values
  hashes <- jira_tickets$hashes

  # Get old qc_category values and filter to selected source
  old_qc_category = runQuery(paste0("SELECT DISTINCT source, source_table, qc_category FROM toxval ",
                                    "WHERE source in ('",
                                    paste0(slist, collapse = "', '"),"')"),
                             toxval.db) %>%
    dplyr::mutate(qc_category = qc_category %>%
                    dplyr::na_if("-"),
                  source_table = source_table %>%
                    gsub("direct load|direct_load|Direct Load", "Direct Load", .)) %>%
    dplyr::left_join(table_df %>%
                       dplyr::select(`Source Name`, `Table Name`, `Jira Ticket`, curation_type),
                     by=c("source" = "Source Name", "source_table" = "Table Name")) %>%
    dplyr::left_join(in_data %>%
                       dplyr::select(`Issue key`, assignee = Assignee, ticket_status = Status),
                     by=c("Jira Ticket"="Issue key"))

  # Store new qc_category values
  res0 <- data.frame()
  # Determine qc_category for each source
  # Automated - hashes reviewed vs. not (generic automated category)
  # Manual - hashes reviewed by pushed qc_status vs. not (generic manual category)
  for(i in seq_len(nrow(old_qc_category))) {
    source_df <- old_qc_category[i,]
    src = source_df$source
    src_tbl = tolower(source_df$source_table)
    cat("Getting qc_category_new for source ", src, "(", i, " of ", nrow(old_qc_category), ")\n")
    query = paste0("SELECT DISTINCT source_hash, source, source_table, qc_category ",
                   "FROM toxval WHERE source = '", src, "' AND source_table = '", src_tbl, "'")
    in_toxval = runQuery(query, toxval.db) %>%
      dplyr::mutate(jira_ticket = source_df$`Jira Ticket`)

    # Check if any data available for selected source
    if(!nrow(in_toxval)){
      message("No toxval data found for source/source_table: ", src, " ", tolower(source_df$source_table))
      browser()
      next
    }

    # Add source table qc_status if not direct load, otherwise "undetermined" qc_status
    if(src_tbl == "direct load"){
      in_toxval$record_qc_status = "not determined"
    } else {
      in_toxval = in_toxval %>%
        dplyr::left_join(runQuery(paste0("SELECT source_hash, qc_status as record_qc_status FROM ",
                                         source_df$source_table),
                                  source.db),
                         by="source_hash")
    }

    # Reconcile curation_type by source_hash for special mixed source cases
    if(src %in% c("ATSDR MRLs", "IRIS", "PPRTV (CPHEA)")){
      src_data = runQuery(paste0("SELECT source_hash, document_type FROM ", source_df$source_table),
                          source.db)
      in_toxval = in_toxval %>%
        dplyr::left_join(src_data, by = "source_hash") %>%
        dplyr::mutate(curation_type = dplyr::case_when(
          grepl("PPRTV Summary|ATSDR MRLs Toxicological Profile|IRIS Summary", document_type) ~ "manual",
          TRUE ~ "automated"
        )) %>%
        dplyr::left_join(source_df %>%
                           dplyr::select(ticket_status, curation_type),
                         by="curation_type")
      tmp = in_toxval %>%
        dplyr::filter(curation_type == source_df$curation_type)
      # Handle edge case where loaded to ToxVal are source_hash values without a Confluence
      # page entry or QC Jira ticket
      if(nrow(tmp)){
        in_toxval = tmp
      }
    } else {
      in_toxval = in_toxval %>%
        dplyr::left_join(source_df %>%
                           dplyr::select(`Jira Ticket`, curation_type, ticket_status),
                         by=c("jira_ticket" = "Jira Ticket"))
    }

    # Pull source_hash from Jira ticket attachments
    if(nrow(hashes)){
      hash_list = hashes %>%
        dplyr::filter(source == src, !is.na(source_hash)) %>%
        dplyr::pull(source_hash)
    } else {
      hash_list = c()
    }

    if(src %in% c("ECOTOX", "ToxRefDB")){
      in_toxval$qc_category_new <- "Data source QC'd by data provider prior to ToxValDB import"
    } else{
      in_toxval = in_toxval %>%
        dplyr::mutate(
          # Establish baseline
          qc_category_new = dplyr::case_when(
            curation_type == 'automated' ~ "Programmatically extracted from structured data source",
            curation_type == 'manual' ~ "Manually extracted from unstructured data source",
            TRUE ~ NA_character_
          ),
          # Account for ticket status (overall pass QC if LV 1 - In Review or Done)
          qc_category_new = dplyr::case_when(
            ticket_status %in% c("QC Lv 1 - In Review", "Done") ~
              paste0(qc_category_new, "; Source overall passed QC"),
            TRUE ~ qc_category_new
          ),
          # Account for record qc_status and attachment file source_hash values
          qc_category_new = dplyr::case_when(
            # If source_hash ever in a ticket attachment or has a status of pass or fail
            # Then it was manually checked
            (source_hash %in% hash_list | grepl("pass", record_qc_status, ignore.case = TRUE) | grepl("fail", record_qc_status, ignore.case = TRUE)) &
              # Only apply if source marked as "passed QC"
              grepl("Source overall passed QC", qc_category_new) ~
              paste0(qc_category_new, ", and this record was manually checked"),
            # Source QC Passed, but no attachment hashes or qc_status of reviewed records
            grepl("Source overall passed QC", qc_category_new) ~ paste0(qc_category_new,
                                                                        ", but this record was not manually checked"),
            TRUE ~ qc_category_new
          )
        )
    }


    #--------------------------------------------------------------------------------------
    # TODO: Incorporate logic for adding additional qc_categories
    #--------------------------------------------------------------------------------------

    # Append records with qc_category
    res0 <- res0 %>%
      dplyr::bind_rows(in_toxval %>%
                         dplyr::select(source, source_table, jira_ticket, source_hash, qc_category_new))
  }

  # Check counts by source
  # View(res0 %>% select(-jira_ticket) %>% distinct() %>% group_by(source, qc_category_new) %>% summarise(n=n()))
  # Check for duplicate assignments
  # View(res0 %>% select(-jira_ticket) %>% distinct() %>% group_by(source_hash) %>% summarise(n=n()) %>% filter(n>1))
  # Check missing source_hash
  # missing_category = runQuery(paste0("SELECT source_hash, source, source_table FROM toxval ",
  #                                    "WHERE source_hash not in ('",
  #                                    paste0(res0$source_hash, collapse = "', '"),"')"),
  #                             toxval.db)

  # Prep columns for insertion
  res <- old_qc_category %>%
    dplyr::left_join(res0 %>%
                       dplyr::select(-source, -source_table),
                     by=c("Jira Ticket"="jira_ticket")) %>%
    group_by(source_hash) %>%
    # Combine unique categories that aren't NA
    dplyr::mutate(qc_category = qc_category_new) %>%
    dplyr::ungroup() %>%
    dplyr::select("source", "source_hash", "qc_category") %>%
    distinct()

  # Update qc_category in toxval
  if(nrow(res)){
    updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                         "ON (a.source_hash = b.source_hash) SET a.qc_category = b.qc_category ",
                         "WHERE a.qc_category IS NOT NULL")
    # Run update query
    runUpdate(table="toxval", updateQuery=updateQuery, updated_df=res, db=toxval.db)
  }
}
