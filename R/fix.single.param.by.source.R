#-------------------------------------------------------------------------------------
#' Alter the contents of toxval according to an excel dictionary
#' @param toxval.db The version of toxval in which the data is altered.
#' @param param The parameter value to be fixed
#' @param source The source to be fixed
#' @param subsource The subsource to be fixed (NULL default)
#' @param ignore If TRUE allow missing values to be ignored
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @param units.data A dataframe containing current units data if units are to be reported (NULL default)
#' @return The database will be altered
#' @export
#-------------------------------------------------------------------------------------
fix.single.param.by.source <- function(toxval.db, param, source, subsource=NULL, ignore = FALSE,
                                       report.only=FALSE, units.data=NULL) {
  printCurrentFunction(paste(toxval.db,":",param, ":", source,subsource))

  # Handle addition of subsource for queries
  query_addition = " and qc_status NOT LIKE '%fail%'"
  if(!is.null(subsource)) {
    query_addition = paste0(query_addition, " and subsource='", subsource, "'")
  }

  # Read and prepare parameter dictionary
  file <- paste0(toxval.config()$datapath,"dictionary/2021_dictionaries/",param,"_5.xlsx")
  mat <- openxlsx::read.xlsx(file, na.strings = "NOTHING")
  #print(View(mat))
  mat_flag_change <- grep('\\[\\.\\.\\.\\]',mat[,2])
  mat[mat_flag_change,2] <- stringr::str_replace_all(mat[mat_flag_change,2],'\\[\\.\\.\\.\\]','XXX')
  print(dim(mat))

  db.values <- runQuery(paste0("select distinct ",param,"_original from toxval where source like '",source,"'",query_addition),
                        toxval.db)[,1]
  missing <- db.values[!is.element(db.values,c(mat[,2],""))]

  missing = missing[!is.na(missing)]
  # If only reporting, return without making changes
  if(report.only) return(missing)

  if(length(missing)>0 & !ignore) {
    cat("values missing from the dictionary\n")
    for(i in 1:length(missing)) cat(missing[i],"\n",sep="")
    #browser()
  }

  # file <- paste0("./dictionary/missing_values_",param,"_",source,"_",Sys.Date(),".xlsx")
  # write.xlsx(missing,file)

  cat("  original list: ",nrow(mat),"\n")

  if(is.null(units.data)) {
    query <- paste0("select distinct (",param,"_original) from toxval where source like '",source,"'",query_addition)
    param_original_values <- runQuery(query,toxval.db)[,1]
    #print(View(param_original_values))
    mat <- mat[is.element(mat[,2],param_original_values),]
  } else {
    param_original_values = units.data %>%
      dplyr::select(toxval_units_original) %>%
      dplyr::distinct() %>%
      dplyr::mutate(include = 1)
    mat = mat %>%
      dplyr::left_join(param_original_values, by=c("toxval_units_original")) %>%
      dplyr::filter(include == 1) %>%
      dplyr::select(-include)
  }

  #print(View(mat))
  cat("  final list: ",nrow(mat),"\n")

  # Use parameter dictionary to update entries in toxval
  for(i in seq_len(dim(mat)[1])) {
    v0 <- mat[i,2]
    v1 <- mat[i,1]

    cat(v0,":",v1,"\n"); utils::flush.console()
    if(is.null(units.data)) {
      query <- paste0("update toxval set ",param,"='",v1,"' where ",param,"_original='",v0,"' and source like '",source,"'",query_addition)
      runQuery(query, toxval.db)
    } else {
      # Track altered values
      change = "Transform variant unit names to standard ones"
      current_changes = units.data %>%
        dplyr::filter(toxval_units_original == !!v0) %>%
        dplyr::mutate(
          toxval_units = !!v1,
          change_made = dplyr::case_when(
            grepl(!!change, change_made) ~ change_made,
            is.na(change_made) ~ !!change,
            TRUE ~ paste0(change_made, "; ", !!change)
          )
        )
      changed_ids = current_changes %>%
        dplyr::pull(toxval_id) %>%
        unique()
      units.data = units.data %>%
        dplyr::filter(!(toxval_id %in% changed_ids)) %>%
        dplyr::bind_rows(current_changes)
    }
  }
  if(is.null(units.data)) {
    query <- paste0("update toxval set ",param,"='-' where ",param,"_original is NULL and source like '",source,"'",query_addition)
    runQuery(query, toxval.db)
  } else {
    # Track altered values
    change = "Set NULL toxval_units to '-'"
    current_changes = units.data %>%
      dplyr::filter(toxval_units_original %in% c(as.character(NA), "")) %>%
      dplyr::mutate(
        toxval_units = "-",
        change_made = dplyr::case_when(
          is.na(change_made) ~ !!change,
          TRUE ~ paste0(change_made, "; ", !!change)
        )
      )
    changed_ids = current_changes %>%
      dplyr::pull(toxval_id) %>%
      unique()
    units.data = units.data %>%
      dplyr::filter(!(toxval_id %in% changed_ids)) %>%
      dplyr::bind_rows(current_changes)

    # Return only changed data
    final_data = units.data %>%
      tidyr::drop_na(change_made)
    return(final_data)
  }
}
