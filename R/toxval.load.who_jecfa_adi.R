#--------------------------------------------------------------------------------------
#
#' Load WHO JECFA ADI from toxval_source to toxval
#' @param toxval.db The database version to use
#' @param source.db The source database
#' @param log If TRUE, send output to a log file
#' @param remove_null_dtxsid If TRUE, delete source records without curated DTXSID value
#--------------------------------------------------------------------------------------
toxval.load.who_jecfa_adi <- function(toxvaldb,source.db, log=FALSE, remove_null_dtxsid=TRUE){
  source = 'WHO JECFA ADI'
  source_table = "source_who_jecfa_adi"
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
  cremove = c("who_jecfa_chemical_id","synonyms","previous_years","tox_monograph_url", "jecfa_number", "tox_monograph", "cas_number", "adi",
              "meeting", "flavis_number", "mrl_code", "mrl_comment", "fema_number", "specs_code", "addendum_url", "chemical_url", "coe_number",
              "residues", "chemical_names", "ins", "intake", "report", "report_url", "addendum", "webpage_name", "evaluation_year",
              "specification_url", "ins_matches", "functional_class", "allergenicity", "residues_url", "treatment_level", "tolerable_intake", "comments",
              "toxval_units_comments", "study_duration_qualifier", "specification", "source_version_date")
  res = res[ , !(names(res) %in% cremove)]

  # Get DTXSID and temporarily add to res
  query = paste0("SELECT DISTINCT chemical_id, dtxsid FROM source_chemical WHERE source='", source, "'")
  dtxsid_map = runQuery(query, toxval.db)
  res = res %>%
    dplyr::left_join(dtxsid_map, by=c("chemical_id"))

  # Dedup based on DTXSID
  dedup_fields = c("name", "casrn", "chemical_id", "source_hash",
                   "range_relationship_id", "relationship", "subsource_url")
  dtxsid_hash_cols = names(res)[!names(res) %in% dedup_fields]

  # Add source_hash_temp column
  res.temp = source_hash_vectorized(res, dtxsid_hash_cols)
  res$source_hash_temp = res.temp$source_hash

  # Perform deduping
  res = res %>%
    dplyr::group_by(source_hash_temp) %>%
    dplyr::mutate(dplyr::across(dplyr::any_of(!!dedup_fields),
                                ~paste0(.[!is.na(.)], collapse=" |::| ") %>%
                                  dplyr::na_if("NA") %>%
                                  dplyr::na_if("") %>%
                                  dplyr::na_if("-")
    )) %>%
    dplyr::ungroup() %>%
    dplyr::distinct()

  # Get mapping of correctly formatted toxval_subtype as "name, (source_hash)" to source_hash_temp
  toxval_subtype_res = res %>%
    dplyr::group_by(source_hash_temp) %>%
    tidyr::separate_rows(source_hash, name, sep=" \\|::\\| ") %>%
    dplyr::mutate(source_hash = stringr::str_c("(", source_hash, ")")) %>%
    tidyr::unite("combined_info", name, source_hash, sep=" ") %>%
    dplyr::select(combined_info, source_hash_temp) %>%
    dplyr::distinct() %>%
    dplyr::summarise(updated_subtype = paste(combined_info, collapse=", ")) %>%
    dplyr::select(source_hash_temp, updated_subtype) %>%
    dplyr::distinct()

  res = res %>%
    dplyr::left_join(toxval_subtype_res, by=c("source_hash_temp")) %>%
    dplyr::mutate(
      # Choose when to select new toxval_subtype vs. keep original "-"
      toxval_subtype = dplyr::case_when(
        grepl("\\|::\\|", name) ~ stringr::str_c("chemical group: ", updated_subtype),
        TRUE ~ toxval_subtype
      ),

      # Clean up name, source_hash, etc. to leave only parent info
      name = name %>%
        gsub(" \\|::\\|.+", "", .),
      source_hash = source_hash %>%
        gsub(" \\|::\\|.+", "", .),
      casrn = casrn %>%
        gsub(" \\|::\\|.+", "", .),
      subsource_url = subsource_url %>%
        gsub(" \\|::\\|.+", "", .),
      chemical_id = chemical_id %>%
        gsub(" \\|::\\|.+", "", .),

      # Remove parent info from toxval_subtype
      toxval_subtype = toxval_subtype %>%
        stringr::str_replace(stringr::fixed(stringr::str_c(name, " (", source_hash, "), ")), "") %>%
        stringr::str_squish(),

      # Select single relationship (all collapsed values are the same)
      relationship = dplyr::case_when(
        grepl("Lower Range", relationship) ~ "Lower Range",
        grepl("Upper Range", relationship) ~ "Upper Range",
        grepl("\\-", relationship) ~ "-",
        TRUE ~ relationship
      )
    ) %>%
    dplyr::select(-c("dtxsid", "source_hash_temp", "updated_subtype"))


  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  nlist = names(res)
  nlist = nlist[!is.element(nlist,c("casrn","name","range_relationship_id","relationship"))]
  nlist = nlist[!is.element(nlist,cols)]
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

  if("species_original" %in% names(res)) res$species_original = tolower(res$species_original)
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

  #####################################################################
  cat("Set the toxval_relationship for separated toxval_numeric range records")
  #####################################################################
  relationship_res = res %>%
    dplyr::mutate(range_relationship_id = as.character(range_relationship_id)) %>%
    # Handle collapsed rows
    tidyr::separate_rows(range_relationship_id, sep=" \\|::\\| ") %>%
    dplyr::mutate(relationship = gsub(" \\|.+", "", relationship)) %>%
    dplyr::filter(grepl("Range", relationship)) %>%
    dplyr::select(toxval_id, range_relationship_id, relationship) %>%
    tidyr::pivot_wider(id_cols = "range_relationship_id", names_from=relationship, values_from = toxval_id) %>%
    dplyr::rename(toxval_id_1 = `Lower Range`,
                  toxval_id_2 = `Upper Range`) %>%
    dplyr::mutate(relationship = "toxval_numeric range") %>%
    dplyr::select(-range_relationship_id) %>%
    dplyr::filter(!is.na(toxval_id_1), !is.na(toxval_id_2))
  # Insert range relationships into toxval_relationship table
  if(nrow(relationship_res)){
    runInsertTable(mat=relationship_res, table='toxval_relationship', db=toxval.db)
  }
  # Remove range_relationship_id
  res <- res %>%
    dplyr::select(-tidyselect::any_of(c("range_relationship_id", "relationship")))

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
  refs$url = "https://apps.who.int/food-additives-contaminants-jecfa-database/"
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
