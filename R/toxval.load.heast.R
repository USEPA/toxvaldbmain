#--------------------------------------------------------------------------------------
#' Load the HEAST data from toxval_source to toxval
#' @param toxval.db The database version to use
#' @param source.db The source database
#' @param log If TRUE, send output to a log file
#' @param remove_null_dtxsid If TRUE, delete source records without curated DTXSID value
#--------------------------------------------------------------------------------------
toxval.load.heast <- function(toxval.db, source.db, log=FALSE, remove_null_dtxsid=TRUE){
  source = "HEAST"
  source_table = "source_heast"
  verbose = log
  #####################################################################
  cat("start output log, log files for each source can be accessed from output_log folder\n")
  #####################################################################
  if(log) {
    con1 = file.path(toxval.config()$datapath,paste0(source,"_",Sys.Date(),".log"))
    con1 = logr::log_open(con1)
    con = file(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"))
    sink(con, append=TRUE)
    sink(con, append=TRUE, type="message")
  }
  #####################################################################
  cat("clean source_info by source\n")
  #####################################################################
  import.source.info.by.source(toxval.db, source)

  #####################################################################
  cat("clean by source\n")
  #####################################################################
  clean.toxval.by.source(toxval.db,source)

  #####################################################################
  cat("load data to res\n")
  #####################################################################
  # Whether to remove records with NULL DTXSID values
  if(!remove_null_dtxsid){
    query = paste0("select * from ",source_table)
  } else {
    query = paste0("select * from ",source_table, " ",
                   # Filter out records without curated chemical information
                   "WHERE chemical_id IN (SELECT chemical_id FROM source_chemical WHERE dtxsid is NOT NULL)")
  }
  res = runQuery(query,source.db,TRUE,FALSE)
  res = res[,!names(res) %in% toxval.config()$non_hash_cols[!toxval.config()$non_hash_cols %in%
                                                              c("chemical_id", "document_name", "source_hash", "qc_status")]]
  res$source = source
  res$details_text = paste(source,"Details")
  print(paste0("Dimensions of source data: ", toString(dim(res))))

  #####################################################################
  cat("Add code to deal with specific issues for this source\n")
  #####################################################################
  res = res %>%
    dplyr::mutate(
      # Set NA for units without values
      study_duration_units = dplyr::case_when(
        is.na(study_duration_value) ~ as.character(NA),
        TRUE ~ study_duration_units
      ),

      # Handle ranged study_duration values - maintain original range, set database values to NA
      study_duration_value_original = study_duration_value,
      study_duration_value = as.numeric(study_duration_value),
      study_duration_units = study_duration_units %>%
        gsub(", ?", "-", .),

      # Set redundant subsource_url values to "-"
      subsource_url = dplyr::case_when(
        subsource_url == source_url ~ "-",
        TRUE ~ subsource_url
      ),
      # Set key_finding
      key_finding = dplyr::case_when(
        !toxval_type %in% c("RfD", "RfC") ~ "key",
        TRUE ~ "no"
      )
    ) %>%
    # Map experimental species information to critical_effect for derived toxval_type entries
    fix.associated.pod.critical_effect(., c("chemical_id", "comment"))

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  nlist = names(res)
  nlist = nlist[!nlist %in% c("casrn","name", "toxval_relationship_id",
                              # Do not remove fields that would become "_original" fields
                              unique(gsub("_original", "", cols)))]
  nlist = nlist[!nlist %in% cols]

  # Remove columns that are not used in toxval
  res = res %>% dplyr::select(!dplyr::any_of(nlist))

  # Check if any non-toxval column still remaining in nlist
  nlist = names(res)
  nlist = nlist[!nlist %in% c("casrn","name", "toxval_relationship_id",
                              # Do not remove fields that would become "_original" fields
                              unique(gsub("_original", "", cols)))]
  nlist = nlist[!nlist %in% cols]

  if(length(nlist)>0) {
    cat("columns to be dealt with\n")
    print(nlist)
    browser()
  }
  print(dim(res))

  #####################################################################
  cat("Generic steps \n")
  #####################################################################
  res = dplyr::distinct(res)
  res = fill.toxval.defaults(toxval.db,res)
  res = generate.originals(toxval.db,res)
  res$toxval_numeric = as.numeric(res$toxval_numeric)
  print(paste0("Dimensions of source data after originals added: ", toString(dim(res))))
  res=fix.non_ascii.v2(res,source)
  # Remove excess whitespace
  res = res %>%
    dplyr::mutate(dplyr::across(tidyselect::where(is.character), stringr::str_squish))
  res = dplyr::distinct(res)
  res = res[, !names(res) %in% c("casrn","name")]
  print(paste0("Dimensions of source data after ascii fix and removing chemical info: ", toString(dim(res))))

  #####################################################################
  cat("add toxval_id to res\n")
  #####################################################################
  count = runQuery("select count(*) from toxval",toxval.db)[1,1]
  if(count==0) {
    tid0 = 1
  } else {
    tid0 = runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
  }
  tids = seq(from=tid0,to=tid0+nrow(res)-1)
  res$toxval_id = tids
  print(dim(res))

  # #####################################################################
  # cat("add record linkages to toxval_relationship\n")
  # #####################################################################
  # Add linkages connecting RfC/RfD to original test data
  linkage_res = res %>%
    # Expand collapsed toxval_relationship_id
    tidyr::separate_rows(toxval_relationship_id, sep = " \\|::\\| ") %>%
    dplyr::mutate(
      toxval_relationship_id = toxval_relationship_id %>%
        stringr::str_squish() %>%
        as.numeric(),
      # Add variable to ensure relationships are ordered as RfC/RfD - original data ("derived from")
      sort_var = dplyr::case_when(
        toxval_type %in% c("RfC", "RfD") ~ 0,
        TRUE ~ 1
      )
    ) %>%
    dplyr::select(toxval_id, toxval_relationship_id, toxval_type, sort_var) %>%
    dplyr::group_by(toxval_relationship_id) %>%
    # Ensure "derived from" ordering
    dplyr::arrange(sort_var, .by_group=TRUE) %>%
    # Build relationship
    dplyr::mutate(toxval_relationship = paste0(toxval_id, collapse = ", "),
                  relationship = paste("derived from") %>%
                    stringr::str_squish()) %>%
    dplyr::ungroup() %>%
    # Remove entries where no linkage was fond
    dplyr::filter(grepl(",", toxval_relationship)) %>%
    # Convert to final linkage table format
    dplyr::select(toxval_relationship, relationship) %>%
    dplyr::distinct() %>%
    tidyr::separate(col="toxval_relationship", into=c("toxval_id_1", "toxval_id_2"), sep = ", ") %>%
    dplyr::mutate(dplyr::across(c("toxval_id_1", "toxval_id_2"), ~as.numeric(.)))

  # Send linkage data to ToxVal
  if(nrow(linkage_res)) {
    runInsertTable(linkage_res, "toxval_relationship", toxval.db)
  }

  # Remove toxval_relationship_id column from res
  res = res %>% dplyr::select(-toxval_relationship_id)

  #####################################################################
  cat("pull out record source to refs\n")
  #####################################################################
  cols = runQuery("desc record_source",toxval.db)[,1]
  nlist = names(res)
  keep = nlist[is.element(nlist,cols)]
  refs = res[,keep]
  cols = runQuery("desc toxval",toxval.db)[,1]
  nlist = names(res)
  remove = nlist[!is.element(nlist,cols)]
  res = res[ , !(names(res) %in% c(remove))]
  print(dim(res))

  #####################################################################
  cat("add extra columns to refs\n")
  #####################################################################
  refs$record_source_type = "-"
  refs$record_source_note = "-"
  refs$record_source_level = "-"
  print(paste0("Dimensions of references after adding ref columns: ", toString(dim(refs))))

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = dplyr::distinct(res)
  refs = dplyr::distinct(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$details_text = paste(source,"Details")
  runInsertTable(res, "toxval", toxval.db, verbose)
  print(paste0("Dimensions of source data pushed to toxval: ", toString(dim(res))))
  runInsertTable(refs, "record_source", toxval.db, verbose)
  print(paste0("Dimensions of references pushed to record_source: ", toString(dim(refs))))

  #####################################################################
  cat("do the post processing\n")
  #####################################################################
  toxval.load.postprocess(toxval.db,source.db,source,do.convert.units=FALSE, remove_null_dtxsid=remove_null_dtxsid)

  if(log) {
    #####################################################################
    cat("stop output log \n")
    #####################################################################
    closeAllConnections()
    logr::log_close()
    output_message = read.delim(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"), stringsAsFactors = FALSE, header = FALSE)
    names(output_message) = "message"
    output_log = read.delim(paste0(toxval.config()$datapath,"log/",source,"_",Sys.Date(),".log"), stringsAsFactors = FALSE, header = FALSE)
    names(output_log) = "log"
    new_log = log_message(output_log, output_message[,1])
    writeLines(new_log, paste0(toxval.config()$datapath,"output_log/",source,"_",Sys.Date(),".txt"))
  }
  #####################################################################
  cat("finish\n")
  #####################################################################
  return(0)

  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
}
