#--------------------------------------------------------------------------------------
#' Load IRIS source from toxval_source to toxval
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param log If TRUE, send output to a log file
#' @param remove_null_dtxsid If TRUE, delete source records without curated DTXSID value
#' @export
#--------------------------------------------------------------------------------------
toxval.load.iris <- function(toxval.db,source.db, log=FALSE, remove_null_dtxsid=TRUE){
  printCurrentFunction(toxval.db)
  source <- "IRIS"
  source_table = "source_iris"
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

  non_toxval_cols <- c('iris_chemical_id',
                       'woe_characterization',
                       'iris_revision_history',
                       'archived',
                       # 'principal_study',
                       'uncertainty_factor',
                       'modifying_factor',
                       'study_confidence',
                       'data_confidence',
                       # 'overall_confidence',
                       'dose_type',
                       "endpoint", "principal_study", "study_duration_qualifier",
                       "full_reference")
  # Rename non-toxval columns
  res <- res %>%
    dplyr::rename(# risk_assessment_class = risk_assessment_duration,
                  quality = overall_confidence) %>%
    select(-dplyr::any_of(non_toxval_cols)) %>%
    dplyr::filter(toxval_numeric != "-")

  # Fill in long_ref where missing
  res$long_ref[res$long_ref == "-"] = res$study_reference[res$long_ref == "-"]

  res$source = source
  res$details_text = paste(source,"Details")
  print(paste0("Dimensions of source data: ", toString(dim(res))))

  #####################################################################
  cat("Add the code from the original version from Aswani\n")
  #####################################################################
  cremove = c("uf_composite", "confidence", "extrapolation_method", "class",
              "key_finding", "age",
              "assessment_type", "curator_notes", "risk_assessment_duration")

  res = res[ , !(names(res) %in% cremove)]

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  nlist = names(res)
  # Custom ignore "document_type" for later relationship processing
  nlist = nlist[!is.element(nlist,c("casrn","name", "document_type", "study_reference",
                                    "numeric_relationship_description","numeric_relationship_id"))]
  nlist = nlist[!is.element(nlist,cols)]
  if(length(nlist)>0) {
    cat("columns to be dealt with\n")
    print(nlist)
    browser()
  }
  print(paste0("Dimensions of source data: ", toString(dim(res))))
  # examples ...
  # names(res)[names(res) == "source_url"] = "url"
  # colnames(res)[which(names(res) == "phenotype")] = "critical_effect"

  #####################################################################
  cat("Generic steps \n")
  #####################################################################
  res = distinct(res)
  res = fill.toxval.defaults(toxval.db,res)
  res = generate.originals(toxval.db,res)

  if(is.element("species_original",names(res))) res[,"species_original"] = tolower(res[,"species_original"])
  res$toxval_numeric = as.numeric(res$toxval_numeric)
  print(paste0("Dimensions of source data: ", toString(dim(res))))
  res=fix.non_ascii.v2(res,source)
  # Remove excess whitespace
  res = res %>%
    dplyr::mutate(dplyr::across(where(is.character), stringr::str_squish))
  res = distinct(res)
  res = res[,!is.element(names(res),c("casrn","name"))]
  print(paste0("Dimensions of source data: ", toString(dim(res))))

  #####################################################################
  cat("add toxval_id to res\n")
  #####################################################################
  count = runQuery("select count(*) from toxval",toxval.db)[1,1]
  if(count==0){
    tid0 = 1
  } else {
    tid0 = runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
  }
  tids = seq(from=tid0,to=tid0+nrow(res)-1)
  res$toxval_id = tids
  print(paste0("Dimensions of source data: ", toString(dim(res))))

  #####################################################################
  cat("Set the toxval_relationship for separated toxval_numeric range records\n")
  #####################################################################
  relationship = res %>%
    dplyr::filter(!numeric_relationship_id  %in% c("-", NA)) %>%
    dplyr::select(toxval_id, numeric_relationship_id, numeric_relationship_description) %>%
    tidyr::pivot_wider(id_cols = "numeric_relationship_id",
                       names_from = numeric_relationship_description,
                       values_from = toxval_id) %>%
    dplyr::rename(toxval_id_1 = `Lower Range`,
                  toxval_id_2 = `Upper Range`) %>%
    dplyr::mutate(relationship = "toxval_numeric range") %>%
    dplyr::select(-numeric_relationship_id)
  # Insert range relationships into toxval_relationship table
  if(nrow(relationship)){
    runInsertTable(mat=relationship, table='toxval_relationship', db=toxval.db)
  }
  # Remove range_relationship_id
  res <- res %>%
    dplyr::select(-tidyselect::any_of(c("numeric_relationship_id", "numeric_relationship_description")))

  #####################################################################
  cat("pull out record source to refs\n")
  #####################################################################
  cols = runQuery("desc record_source",toxval.db)[,1]
  nlist = names(res)
  keep = nlist[is.element(nlist,cols)]
  refs = res[,keep]
  cols = runQuery("desc toxval",toxval.db)[,1]
  nlist = names(res)[!names(res) %in% c("document_type", "study_reference")]
  remove = nlist[!is.element(nlist, cols)]
  res = res[ , !(names(res) %in% c(remove))]
  print(paste0("Dimensions of source data: ", toString(dim(res))))

  #####################################################################
  cat("add extra columns to refs\n")
  #####################################################################
  refs$record_source_type = "-"
  refs$record_source_note = "-"
  refs$record_source_level = "-"
  print(dim(res))

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = distinct(res)
  refs = distinct(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "https://www.epa.gov/iris"
  res$subsource_url = "-"
  res$details_text = paste(source,"Details")
  #for(i in 1:nrow(res)) res[i,"toxval_uuid"] = UUIDgenerate()
  #for(i in 1:nrow(refs)) refs[i,"record_source_uuid"] = UUIDgenerate()
  # Push res except for set_toxval_relationship_by_toxval_type needed columns
  runInsertTable(res %>%
                   dplyr::select(-document_type, -study_reference),
                 "toxval", toxval.db, verbose)
  runInsertTable(refs, "record_source", toxval.db, verbose)
  print(dim(res))

  #####################################################################
  cat("Set Summary record relationship/hierarchy\n")
  #####################################################################
  # Set Summary record relationship/hierarchy
  set_toxval_relationship_by_toxval_type(res=res,
                                         toxval.db=toxval.db)

  #####################################################################
  cat("do the post processing\n")
  #####################################################################
  toxval.load.postprocess(toxval.db,source.db,source, remove_null_dtxsid)

  if(log) {
    #####################################################################
    cat("stop output log \n")
    #####################################################################
    closeAllConnections()
    logr::log_close()
    output_message = read.delim(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"), stringsAsFactors = F, header = F)
    names(output_message) = "message"
    output_log = read.delim(paste0(toxval.config()$datapath,"log/",source,"_",Sys.Date(),".log"), stringsAsFactors = F, header = F)
    names(output_log) = "log"
    new_log = log_message(output_log, output_message[,1])
    writeLines(new_log, paste0(toxval.config()$datapath,"output_log/",source,"_",Sys.Date(),".txt"))
  }
  #####################################################################
  cat("finish\n")
  #####################################################################
}
