#--------------------------------------------------------------------------------------
#' Function for assigning QC Categories to sources in toxval via the qc_category field
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source The source to set a qc_category for
#' @param access_token A personal access token for authentication in Confluence/Jira
#' @export
#--------------------------------------------------------------------------------------
set.qc.category.by.source <- function(toxval.db, source=NULL, access_token){
  printCurrentFunction(toxval.db)
  if(!is.null(source)) {
    slist = source
  } else {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  }
  library(httr)
  library(rvest)
  url <- "https://confluence.epa.gov/x/VuCkFg"

  access_token <- "MDMwODg0NDA2OTAyOqLv0SzpIbHeggQJJsV5DVV8wMrn"

  response <- GET(url, add_headers(Authorization = paste("Bearer", access_token)))

  if (status_code(response) == 200) {
    confluence_page <- read_html(content(response, "text"))
  } else{
    print('authentication failed')
  }

  tables <- html_nodes(confluence_page, "table")
  table <- tables[[2]]
  header_row <- html_nodes(confluence_page, "tr:nth-child(1) th")
  column_names <- html_text(header_row)

  table_data <- list()

  data_rows <- html_nodes(table, "tr:not(:first-child)")

  for(i in seq_along(data_rows)) {
    row <- data_rows[[i]]
    cells <- html_nodes(row, "td")
    row_data <- html_text(cells)
    row_data <- gsub("[\r\n]", "", row_data)
    table_data <- c(table_data, list(row_data))
  }

  table_df <- as.data.frame(do.call(rbind, table_data), stringsAsFactors = FALSE)
  colnames(table_df) <- column_names

  res <- data.frame()

  old_qc_category = runQuery("select distinct source, qc_category from toxval",toxval.db)
  old_qc_category$qc_category[old_qc_category$qc_category == "-"] <- NA
  tables_names <- unique(table_df$`Source Name`)
  valid_sources <- old_qc_category %>%
    filter(source %in% slist & source %in% tables_names)

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
    if(qc_status == "LV 1- In Review"){
      if(!grepl("Source overall passed QC, but this record was not manually checked", existing_source$qc_category)){
        if(qc_category_new == "-" | is.na(qc_category_new)){
          qc_category_new = "Source overall passed QC, but this record was not manually checked"
        } else{
          qc_category_new = paste0(qc_category_new, ", Source overall passed QC, but this record was not manually checked")
        }
      }
    }
    new_stuff <- c(src, qc_category_new)
    res <- rbind(res, new_stuff)
  }
  colnames(res) <- c("source", "qc_category_new")
  res_merge <- merge(old_qc_category, res, by="source")
  res_final <- res_merge %>%
    tidyr::unite(col="qc_category", qc_category, qc_category_new, sep = ", ", na.rm = TRUE) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(qc_category = toString(unique(unlist(strsplit(qc_category,",\\s+")))))

  # Update qc_category in toxval
  if(nrow(res_final)){
    updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                         "ON (a.source = b.source) SET a.qc_category = b.qc_category")
    # Run update query
    runUpdate(table="toxval", updateQuery=updateQuery, updated_df=res_final, db=toxval.db)
  }
}
