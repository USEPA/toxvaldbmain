#--------------------------------------------------------------------------------------
#' Load HAWC PFAS 430 from toxval_source to toxval
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The version of toxval_source from which the tables are loaded.
#' @param log If TRUE, send output to a log file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.hawc_pfas_430 <- function(toxval.db, source.db,log=F){
  printCurrentFunction(toxval.db)
  source <- "HAWC PFAS 430"
  source_table = "source_hawc_pfas_430"
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
  cat("Add the code from the original version from Aswani\n")
  #####################################################################
  res$casrn <-  gsub("([a-zA-Z]+\\s+[a-zA-Z]*\\:*\\s*)(.*)","\\2",res$casrn)
  res$exposure_route <- gsub("(^[a-zA-Z]+)(\\s*.*)","\\1", res$route_of_exposure)
  res$exposure_method <- gsub("(^[a-zA-Z]+\\s*)(.*)","\\2", res$route_of_exposure)
  res$exposure_method <- gsub("^\\-\\s+","", res$exposure_method)
  res$study_duration_value <- gsub("(^\\d+)(\\s+.*)","\\1",res$exposure_duration_text)
  res$study_duration_value <- gsub("(^\\d+)(.*)","\\1",res$study_duration_value)
  range_vals <- grep("\\-", res$study_duration_value)
  res[range_vals,"study_duration_value"] <- res[range_vals,"exposure_duration_value_original"]
  res$study_duration_units <- gsub("(^GD)(\\s+.*)","\\1",res$exposure_duration_text)
  res$study_duration_units <- gsub("(^\\d+\\s+)(\\w+)(\\s*.*)","\\2",res$study_duration_units)
  res$study_duration_units <- gsub("(.*)(\\d+\\s+)(\\w+)(\\s*.*)","\\3",res$study_duration_units)
  res$study_duration_units <- gsub("(\\d+\\s*)(\\w+)(\\s*.*)","\\2",res$study_duration_units)
  res[is.element(res$study_duration_units,"d"),"study_duration_units"] <- "day"
  res[is.element(res$study_duration_units,"GD"),"study_duration_units"] <- "day"
  res[is.element(res$study_duration_units,"wk"),"study_duration_units"] <- "week"
  res[is.element(res$study_duration_units,"yr"),"study_duration_units"] <- "year"

  res$study_type <- gsub("(^\\w+\\-*\\w*)(\\s*.*)","\\1",res$study_type_original)
  res$study_duration_value <- as.numeric(res$study_duration_value)

  cremove=c("target","noel_original","loel_original",
            "fel_original","endpoint_url_original","bmd",
            "study_id","authors_short","full_text_url",
            "study_url_original","experiment_name","chemical_source",
            "guideline_compliance","dosing_regime_id","route_of_exposure",
            "exposure_duration_value_original","exposure_duration_text","doses")
  res = res[ , !(names(res) %in% cremove)]

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  res = res[ , !(names(res) %in% c("data_location","doses_units"))]
  nlist = names(res)
  nlist = nlist[!is.element(nlist,c("casrn","name"))]
  nlist = nlist[!is.element(nlist,cols)]
  if(length(nlist)>0) {
    cat("columns to be dealt with\n")
    print(nlist)
    browser()
  }
  print(dim(res))

  # examples ...
  # names(res)[names(res) == "source_url"] = "url"
  # colnames(res)[which(names(res) == "phenotype")] = "critical_effect"

  #####################################################################
  cat("Generic steps \n")
  #####################################################################
  res = unique(res)
  for(i in 1:nrow(res)) {
    x = res[i,"toxval_numeric"]
    if(length(grep(";",x))>0) {
      y = str_split(x,";")[[1]]
      z = as.numeric(y)
      res[i,"toxval_numeric"] = min(z)
    }
    else res[i,"toxval_numeric"] = as.numeric(x)
  }
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
  refs$record_source_type = ""
  refs$record_source_note = ""
  refs$record_source_level = ""
  print(dim(res))

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = unique(res)
  refs = unique(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "-"
  res$subsource_url = "-"
  res$details_text = paste(source,"Details")
  #for(i in 1:nrow(res)) res[i,"toxval_uuid"] = UUIDgenerate()
  #for(i in 1:nrow(refs)) refs[i,"record_source_uuid"] = UUIDgenerate()
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
