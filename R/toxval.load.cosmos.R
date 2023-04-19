#-------------------------------------------------------------------------------------
#' Load teh COSMOS data from source to toxval
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param log If TRUE, send output to a log file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.cosmos <- function(toxval.db,source.db,log=F){
  printCurrentFunction(toxval.db)

  #####################################################################
  cat("start output log, log files for each source can be accessed from output_log folder\n")
  #####################################################################
  source <- "COSMOS"
  source_table = "source_cosmos"
  verbose = log
  #####################################################################
  cat("start output log, log files for each source can be accessed from output_log folder\n")
  #####################################################################
  if(log) {
    con1 = file.path(toxval.config()$datapath,paste0(source,"_",Sys.Date(),".log"))
    con1 = log_open(con1)
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
  query = paste0("select * from ",source_table)
  res = runQuery(query,source.db,T,F)
  res = res[ , !(names(res) %in% c("source_id","clowder_id","parent_hash","create_time","modify_time","created_by"))]
  res = res[ , !(names(res) %in% c("qc_flags","qc_notes","version","parent_chemical_id"))]
  res$source = source
  res$details_text = paste(source,"Details")
  print(dim(res))

  #####################################################################
  cat("Trim leading and trailing spaces\n")
  #####################################################################
  res = fix.trim_spaces(res)

  #####################################################################
  cat("Add the code from the original version from Aswani\n")
  #####################################################################
  res$exposure_route <- res$route_of_exposure
  res$exposure_method <-  res$route_of_exposure
  res$study_duration_value <- res$duration
  res$study_duration_units <- res$duration
  res$toxval_numeric <- res$study_results
  res$toxval_type <- res$study_results
  res$toxval_units <- res$study_results
  names(res) <- gsub("year_report/citation", "year", names(res))
  res$year <-  gsub("-","", res$year)
  res$year <-  as.integer(res$year)
  names(res) <- gsub("test_substance_name", "name", names(res))
  names(res) <- gsub("registry_number", "casrn", names(res))
  names(res) <- gsub("study_no", "source_study_id", names(res))
  names(res) <- gsub("document_source", "subsource", names(res))
  names(res) <- gsub("data_quality", "quality", names(res))
  names(res) <- gsub("study_reference", "long_ref", names(res))
  names(res) <- gsub("study_title", "title", names(res))

  #####################################################################
  cat("checks, finds and replaces non ascii characters in res with XXX\n")
  #####################################################################
  #res <- fix.non_ascii(res)
  #res <- generate.originals(toxval.db,res)

  ### seperate study information, route of exposure into exposure method and exposure route
  res$exposure_route <- gsub("\\-.*","", res$route_of_exposure)
  exp_method <- grep("-", res$route_of_exposure, value = T)
  new_exp_method <- gsub(".*\\-","", exp_method)
  res$exposure_method <- "-"
  res$exposure_method[res$route_of_exposure %in% exp_method] <- new_exp_method

  ### duplicated study no in study information table converted to 6618001
  which(res$source_study_id == 6618)
  res$source_study_id[which(duplicated(res$source_study_id))] <- as.integer(paste(res$source_study_id[which(duplicated(res$source_study_id))], "001", sep = ""))
  which(res$source_study_id == 6618)
  ### seperate duration to study_duration_value and study_duration_units
  res[is.na(res)] <- "-"
  res$duration <- gsub("Gestation day","GD",res$duration, ignore.case = TRUE)
  res$duration <- gsub(" NA","-1 -",res$duration, ignore.case = TRUE)
  res <- separate(res,duration,c("study_duration_value", "study_duration_units")," ")
  res[grep("[0-9]-",res$study_duration_value),"study_duration_value"] <- as.numeric(sapply(strsplit(res[grep("[0-9]-",res$study_duration_value),"study_duration_value"],"-"), '[[',2)) - as.numeric(sapply(strsplit(res[grep("[0-9]-",res$study_duration_value),"study_duration_value"],"-"), '[[',1))
  res$study_duration_value <- as.numeric(res$study_duration_value)

  ### create toxval_numeric, toxval_type and toxval_units from study_results
  res$study_results <- gsub("=", ": ",res$study_results)
  res$study_results <- gsub("L;", "L:",res$study_results)
  #######res$study_results <- enc2utf8(res$study_results)
  res <-mutate(res, study_results = strsplit(study_results,";")) %>% unnest(study_results)
  res <- res[!is.na(res$study_results),]
  res <- res[res$study_results != "",]
  res <- res[grep("established",res$study_results, ignore.case = TRUE, invert = TRUE),]
  res <- res[grep("CFSAN",res$study_results, ignore.case = TRUE, invert = TRUE),]
  res$toxval_type <- str_trim(substr(res$study_results,1,regexpr(":",res$study_results)-1))
  res$toxval_numeric <- str_trim(substr(res$study_results,regexpr(":",res$study_results)+1,regexpr("[0-9] ",res$study_results)))
  res$toxval_numeric <- as.numeric(res$toxval_numeric)
  res$study_results <- gsub("m2","m-squared",res$study_results)
  res$toxval_units <- str_trim(gsub(".*[0-9]","",res$study_results))
  res[res$toxval_units=="", which(colnames(res)=="toxval_type")] <- "-"
  res[res$toxval_units=="", which(colnames(res)=="toxval_units")] <- "-"
  res <- unique(res)

  ### fix name
  res[grep("Preferred",res$name, invert = TRUE),"name"] = paste0(grep("Preferred",res$name, invert = TRUE, value = TRUE)," (Preferred Term)")
  res$name = unlist(sapply(strsplit(res$name, "; "), grep, pattern="Preferred", value = TRUE))
  res$name = gsub(" (Preferred Term)","",res$name, fixed = TRUE)
  res$name = gsub(" (INCI)","",res$name, fixed = TRUE)

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  res = as.data.frame(res)
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  res = res[ , !(names(res) %in% c("study_id","cms_id","per_purity","per_active","test_substance_comments",
                                   "route_of_exposure","dose_or_conc_levels","dose_comments","study_design_comments",
                                   "study_results","study_result_comments","document_number"))]
  nlist = names(res)
  nlist = nlist[!is.element(nlist,c("casrn","name"))]
  nlist = nlist[!is.element(nlist,cols)]
  if(length(nlist)>0) {
    cat("columns to be dealt with\n")
    print(nlist)
    browser()
  }
  print(dim(res))

  ##########################################################
  cat("remove the redundancy from the source_hash\n")
  ##########################################################
  x=seq(from=1,to=nrow(res))
  y = paste0(res$source_hash,"_",x)
  res$source_hash = y

  #####################################################################
  cat("Generic steps \n")
  #####################################################################
  res = unique(res)
  res = res[!is.na(res$toxval_numeric),]
  res = res[res$toxval_numeric>0,]
  res = fill.toxval.defaults(toxval.db,res)
  res = generate.originals(toxval.db,res)
  if(is.element("species_original",names(res))) res[,"species_original"] = tolower(res[,"species_original"])
  res$toxval_numeric = as.numeric(res$toxval_numeric)
  print(dim(res))
  res=fix.non_ascii.v2(res,source)
  res = data.frame(lapply(res, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F, check.names=F)
  res = unique(res)
  res = res[,!is.element(names(res),c("casrn","name"))]
  print(dim(res))

  #####################################################################
  cat("add toxval_id to res\n")
  #####################################################################
  count = runQuery("select count(*) from toxval",toxval.db)[1,1]
  if(count==0) tid0 = 1
  else tid0 = runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
  tids = seq(from=tid0,to=tid0+nrow(res)-1)
  res$toxval_id = tids
  print(dim(res))

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
  refs$record_source_type = "website"
  refs$record_source_note = "to be cleaned up"
  refs$record_source_level = "primary (risk assessment values)"
  print(dim(res))

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = unique(res)
  refs = unique(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "https://www.ng.cosmosdb.eu/"
  res$subsource_url = "-"
  res$details_text = paste(source,"Details")
  for(i in 1:nrow(res)) res[i,"toxval_uuid"] = UUIDgenerate()
  for(i in 1:nrow(refs)) refs[i,"record_source_uuid"] = UUIDgenerate()
  runInsertTable(res, "toxval", toxval.db, verbose)
  runInsertTable(refs, "record_source", toxval.db, verbose)
  print(dim(res))

  #####################################################################
  cat("do the post processing\n")
  #####################################################################
  toxval.load.postprocess(toxval.db,source.db,source)

  if(log) {
    #####################################################################
    cat("stop output log \n")
    #####################################################################
    closeAllConnections()
    log_close()
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

